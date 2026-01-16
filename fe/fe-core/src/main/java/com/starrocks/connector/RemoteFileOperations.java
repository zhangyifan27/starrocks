// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector;

import StorageEngineClient.CombineFileSplit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveWriteUtils;
import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utils.TdwUtil;
import com.tencent.tdw.security.exceptions.SecureException;
import jline.internal.Log;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.connector.hive.HiveWriteUtils.checkedDelete;
import static com.starrocks.connector.hive.HiveWriteUtils.createDirectory;
import static com.starrocks.connector.hive.HiveWriteUtils.fileCreatedByQuery;
import static com.starrocks.fs.hdfs.HdfsFsManager.USER_NAME_KEY;

public class RemoteFileOperations {
    private static final Logger LOG = LogManager.getLogger(RemoteFileOperations.class);

    public static final String HMS_PARTITIONS_REMOTE_FILES = "HMS.PARTITIONS.LIST_FS_PARTITIONS";
    protected CachingRemoteFileIO remoteFileIO;
    private final List<ExecutorService> pullRemoteFileExecutors;
    private final AtomicLong executorRoundRobinIndex = new AtomicLong(0);
    private final Executor updateRemoteFilesExecutor;
    private final boolean isRecursive;
    private final boolean enableCatalogLevelCache;
    private final Configuration conf;

    /**
     * Get remote file pull timeout.
     * Returns the minimum of remoteFilePullTimeout and queryTimeout.
     * @return timeout in milliseconds, returns Long.MAX_VALUE if unavailable
     */
    private static long getRemoteFilePullTimeout() {
        long remoteFilePullTimeout = Long.MAX_VALUE;
        if (ConnectContext.get() != null && (ConnectContext.get().getSessionVariable() != null)) {
            remoteFilePullTimeout = ConnectContext.get().getSessionVariable().getRemoteFilePullTimeout();
            int queryTimeoutS = ConnectContext.get().getSessionVariable().getQueryTimeoutS();
            remoteFilePullTimeout = Math.min(remoteFilePullTimeout, queryTimeoutS * 1000L);
        }
        return remoteFilePullTimeout;
    }

    public RemoteFileOperations(CachingRemoteFileIO remoteFileIO,
                                ExecutorService pullRemoteFileExecutors,
                                Executor updateRemoteFilesExecutor,
                                boolean isRecursive,
                                boolean enableCatalogLevelCache,
                                Configuration conf) {
        this.remoteFileIO = remoteFileIO;
        this.pullRemoteFileExecutors = new ArrayList<>(1);
        this.pullRemoteFileExecutors.add(pullRemoteFileExecutors);
        this.updateRemoteFilesExecutor = updateRemoteFilesExecutor;
        this.isRecursive = isRecursive;
        this.enableCatalogLevelCache = enableCatalogLevelCache;
        this.conf = conf;
    }

    public RemoteFileOperations(CachingRemoteFileIO remoteFileIO,
                                List<ExecutorService> pullRemoteFileExecutors,
                                Executor updateRemoteFilesExecutor,
                                boolean isRecursive,
                                boolean enableCatalogLevelCache,
                                Configuration conf) {
        this.remoteFileIO = remoteFileIO;
        this.pullRemoteFileExecutors = pullRemoteFileExecutors;
        this.updateRemoteFilesExecutor = updateRemoteFilesExecutor;
        this.isRecursive = isRecursive;
        this.enableCatalogLevelCache = enableCatalogLevelCache;
        this.conf = conf;
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions) {
        return getRemoteFiles(partitions, Optional.empty(), true);
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions, boolean useCache) {
        return getRemoteFiles(partitions, Optional.empty(), useCache);
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions, Optional<String> hudiTableLocation) {
        return getRemoteFiles(partitions, hudiTableLocation, true);
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions, Optional<String> hudiTableLocation, boolean useCache) {
        // Record start time for latency metrics
        long startTime = System.currentTimeMillis();

        // Record total getRemoteFiles calls
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_REMOTE_FILE_GET_ALL.increase(1L);
        }

        Map<RemotePathKey, Partition> pathKeyToPartition = Maps.newHashMap();
        for (Partition partition : partitions) {
            RemotePathKey key = RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation);
            pathKeyToPartition.put(key, partition);
        }

        int cacheMissSize = partitions.size();
        if (enableCatalogLevelCache && useCache) {
            cacheMissSize = cacheMissSize - remoteFileIO.getPresentRemoteFiles(
                    Lists.newArrayList(pathKeyToPartition.keySet())).size();
        }

        List<RemoteFileInfo> resultRemoteFiles = Lists.newArrayList();
        RemotePathKey.HudiContext hudiContext = new RemotePathKey.HudiContext();

        long remoteFilePullTimeout = getRemoteFilePullTimeout();
        Tracers.count(Tracers.Module.EXTERNAL, HMS_PARTITIONS_REMOTE_FILES, cacheMissSize);
        try (Timer ignored = Tracers.watchScope(Tracers.Module.EXTERNAL, HMS_PARTITIONS_REMOTE_FILES)) {
            // Check if task queue mode is enabled via FE config
            boolean useTaskQueueMode = Config.enable_remote_file_task_queue_mode;

            // Get worker count from session variable
            int configuredWorkerCount = 0;
            if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
                configuredWorkerCount = ConnectContext.get().getSessionVariable().getRemoteFilePullWorkerCount();
            }

            // Get properties once at the outer layer to pass user information into the methods
            Map<String, String> properties = getProperties();

            if (useTaskQueueMode) {
                // Task queue mode: put all partitions into a queue, fixed number of workers fetch tasks from the queue
                executeWithTaskQueueMode(partitions, pathKeyToPartition, resultRemoteFiles, remoteFilePullTimeout,
                        hudiContext, isRecursive, hudiTableLocation, useCache, configuredWorkerCount, properties);
            } else {
                // Original mode: submit one task per partition
                executeWithOriginalMode(partitions, pathKeyToPartition, resultRemoteFiles, remoteFilePullTimeout,
                        hudiContext, isRecursive, hudiTableLocation, useCache, properties);
            }
        } catch (Throwable e) {
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_REMOTE_FILE_GET_ERR.increase(1L);
            }
            throw e;
        }

        // Calculate elapsed time once for both metrics and logging
        long elapseMs = System.currentTimeMillis() - startTime;
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_REMOTE_FILE_GET_SUCCESS.increase(1L);
            MetricRepo.HISTO_REMOTE_FILE_OPERATIONS_LATENCY.update(elapseMs);
        }

        // Log slow operations (> 1 minute)
        if (elapseMs > Config.remote_file_warn_response_time) {
            String queryId = "N/A";
            if (ConnectContext.get() != null && ConnectContext.get().getQueryId() != null) {
                queryId = ConnectContext.get().getQueryId().toString();
            }
            LOG.warn("Slow remote file operation detected: elapsed time {} ms ({} s), " +
                            "query id: {}, partition count: {}, result file count: {}, use cache: {}, is recursive: {}",
                    elapseMs, elapseMs / 1000, queryId, partitions.size(), resultRemoteFiles.size(), useCache, isRecursive);
        }

        return resultRemoteFiles;
    }

    /**
     * Original mode: submit one task per partition.
     */
    private void executeWithOriginalMode(
            List<Partition> partitions,
            Map<RemotePathKey, Partition> pathKeyToPartition,
            List<RemoteFileInfo> resultRemoteFiles,
            long remoteFilePullTimeout,
            RemotePathKey.HudiContext hudiContext,
            boolean isRecursive,
            Optional<String> hudiTableLocation,
            boolean useCache,
            Map<String, String> properties) {
        List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures = Lists.newArrayList();
        List<Map<RemotePathKey, List<RemoteFileDesc>>> results = Lists.newArrayList();

        // For tracking queue time statistics
        Queue<Long> startExecutionTimes = new LinkedBlockingQueue<>();

        try {
            long startTime = System.currentTimeMillis();
            // Submit tasks for all partitions
            for (int i = 0; i < partitions.size(); i++) {
                Partition partition = partitions.get(i);
                int executorIndex = getExecutorIndex(partition);
                RemotePathKey pathKey = buildRemotePathKey(partition, isRecursive, hudiTableLocation, properties, hudiContext);

                Future<Map<RemotePathKey, List<RemoteFileDesc>>> future =
                        pullRemoteFileExecutors.get(executorIndex).submit(() -> {
                            // Record task execution start time
                            startExecutionTimes.add(System.currentTimeMillis());
                            return remoteFileIO.getRemoteFiles(pathKey, useCache);
                        });
                futures.add(future);
            }

            // Collect results from futures
            collectFutureResults(futures, results, startTime, remoteFilePullTimeout);

            // Calculate and log queue time statistics
            logQueueTimeStats(startTime, startExecutionTimes, "OriginalMode", partitions.size(), partitions.size());

            // Build remote file info
            collectRemoteFileInfos(results, pathKeyToPartition, resultRemoteFiles);
        } catch (Throwable e) {
            cancelAllFutures(futures);
            throw e;
        }
    }

    /**
     * Get executor index based on partition's authority
     */
    private int getExecutorIndex(Partition partition) {
        String authority = new Path(partition.getFullPath()).toUri().getAuthority();
        if (StringUtils.isNotEmpty(authority)) {
            return Math.abs(authority.hashCode()) % pullRemoteFileExecutors.size();
        }
        return 0;
    }

    /**
     * Build RemotePathKey for the partition
     */
    private RemotePathKey buildRemotePathKey(Partition partition, boolean isRecursive,
            Optional<String> hudiTableLocation, Map<String, String> properties,
            RemotePathKey.HudiContext hudiContext) {
        RemotePathKey pathKey = RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation, properties);
        pathKey.setHudiContext(hudiContext);
        if (isFormatFileWithSplitEnabled(partition)) {
            pathKey.setSplitStorageFormat(true);
        }
        return pathKey;
    }

    /**
     * Check if the partition is FORMATFILE format with split enabled
     */
    private boolean isFormatFileWithSplitEnabled(Partition partition) {
        return partition.getInputFormat().equals(RemoteFileInputFormat.FORMATFILE) && Config.enable_native_split_storage_format;
    }

    /**
     * Collect results from futures
     */
    private void collectFutureResults(List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures,
            List<Map<RemotePathKey, List<RemoteFileDesc>>> results, long startTime, long timeout) {
        for (int i = 0; i < futures.size(); i++) {
            Future<Map<RemotePathKey, List<RemoteFileDesc>>> future = futures.get(i);
            try {
                long remainingTimeout = timeout - (System.currentTimeMillis() - startTime);
                if (remainingTimeout <= 0) {
                    throw new StarRocksConnectorException(
                            "Timeout after processing %d/%d partitions, total timeout: %d ms",
                            i, futures.size(), timeout);
                }
                results.add(future.get(remainingTimeout, TimeUnit.MILLISECONDS));
            } catch (TimeoutException e) {
                throw new StarRocksConnectorException(
                        "Timeout while getting remote files for partition %d/%d, timeout: %d ms",
                        i + 1, futures.size(), timeout);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StarRocksConnectorException(
                        "Interrupted while getting remote files for partition %d/%d",
                        i + 1, futures.size());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new StarRocksConnectorException(
                        "Failed to get remote files for partition %d/%d, msg: %s",
                        i + 1, futures.size(), cause.getMessage());
            }
        }
    }

    /**
     * Cancel all futures in the list (generic version)
     */
    private <T> void cancelAllFutures(List<Future<T>> futures) {
        for (Future<T> future : futures) {
            try {
                future.cancel(true);
            } catch (Exception e) {
                LOG.warn("Failed to cancel future: " + e.getMessage());
            }
        }
    }

    /**
     * Task queue mode: fixed number of workers fetch tasks from a shared queue
     */
    private void executeWithTaskQueueMode(
            List<Partition> partitions,
            Map<RemotePathKey, Partition> pathKeyToPartition,
            List<RemoteFileInfo> resultRemoteFiles,
            long remoteFilePullTimeout,
            RemotePathKey.HudiContext hudiContext,
            boolean isRecursive,
            Optional<String> hudiTableLocation,
            boolean useCache,
            int configuredWorkerCount,
            Map<String, String> properties) {
        AtomicBoolean cancelled = new AtomicBoolean(false);
        List<Future<Void>> workerFutures = Lists.newArrayList();
        try {
            long startTime = System.currentTimeMillis();
            int workerCount = Math.max(1, Math.min(configuredWorkerCount, partitions.size()));

            BlockingQueue<Partition> taskQueue = new LinkedBlockingQueue<>(partitions);
            Map<RemotePathKey, List<RemoteFileDesc>> concurrentResult = new ConcurrentHashMap<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(workerCount);

            ExecutorService executor = pullRemoteFileExecutors.get(0);
            if (pullRemoteFileExecutors.size() > 1) {
                long counter = executorRoundRobinIndex.getAndIncrement();
                int index = (int) (Math.abs(counter) % pullRemoteFileExecutors.size());
                executor = pullRemoteFileExecutors.get(index);
            }

            // For tracking queue time statistics (TaskQueue mode)
            Queue<Long> partitionStartExecutionTimes = new LinkedBlockingQueue<>();

            for (int i = 0; i < workerCount; i++) {
                final int workerIndex = i;

                Callable<Void> worker = () -> {
                    try {
                        while (!cancelled.get()) {
                            Partition partition = taskQueue.poll();
                            if (partition == null) {
                                break;
                            }
                            try {
                                // Record execution start time for each partition
                                partitionStartExecutionTimes.add(System.currentTimeMillis());

                                RemotePathKey pathKey = buildRemotePathKey(partition, isRecursive, hudiTableLocation,
                                        properties, hudiContext);
                                Map<RemotePathKey, List<RemoteFileDesc>> files = remoteFileIO.getRemoteFiles(pathKey, useCache);
                                concurrentResult.putAll(files);
                            } catch (Throwable e) {
                                LOG.error("Worker {} failed to process partition {} with error: {}",
                                        workerIndex, partition.getFullPath(), e.getMessage());
                                errorRef.compareAndSet(null, e);
                                cancelled.set(true);
                                taskQueue.clear();
                                break;
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                    return null;
                };

                try {
                    workerFutures.add(executor.submit(worker));
                } catch (Exception e) {
                    LOG.error("Failed to submit worker {}: {}", i, e.getMessage());
                    throw new StarRocksConnectorException(
                            "Failed to submit worker task %d/%d, msg: %s", i + 1, workerCount, e.getMessage());
                }
            }

            // Wait for all workers to complete
            long timeout = remoteFilePullTimeout - (System.currentTimeMillis() - startTime);
            if (timeout <= 0) {
                throw new StarRocksConnectorException(
                        "Failed to get remote files, msg: timeout before workers start, total timeout: %d ms.",
                        remoteFilePullTimeout);
            }

            try {
                boolean completed = latch.await(timeout, TimeUnit.MILLISECONDS);
                if (!completed) {
                    throw new StarRocksConnectorException(
                            "Failed to get remote files, msg: timeout after %d ms (limit: %d ms).",
                            System.currentTimeMillis() - startTime, remoteFilePullTimeout);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StarRocksConnectorException("Failed to get remote files, msg: interrupted");
            }

            Throwable error = errorRef.get();
            if (error != null) {
                throw new StarRocksConnectorException("Failed to get remote files", error);
            }

            // Calculate and log queue time statistics (TaskQueue mode)
            logQueueTimeStats(startTime, partitionStartExecutionTimes, "TaskQueueMode", partitions.size(), workerCount);

            // Collect results in the original partition order to ensure consistent ordering
            for (Partition partition : partitions) {
                RemotePathKey pathKey = buildRemotePathKey(partition, isRecursive, hudiTableLocation,
                        properties, hudiContext);
                List<RemoteFileDesc> fileDescs = concurrentResult.get(pathKey);
                if (fileDescs != null) {
                    resultRemoteFiles.add(buildRemoteFileInfo(partition, fileDescs));
                }
            }
        } catch (Throwable e) {
            cancelled.set(true);
            cancelAllFutures(workerFutures);
            throw e;
        }
    }

    public List<RemoteFileInfo> getPresentFilesInCache(Collection<Partition> partitions) {
        return getPresentFilesInCache(partitions, Optional.empty());
    }

    public List<RemoteFileInfo> getPresentFilesInCache(Collection<Partition> partitions, Optional<String> hudiTableLocation) {
        Map<RemotePathKey, Partition> pathKeyToPartition = partitions.stream()
                .collect(Collectors.toMap(partition -> RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation),
                        Function.identity()));

        List<RemotePathKey> paths = partitions.stream()
                .map(partition -> RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation))
                .collect(Collectors.toList());

        Map<RemotePathKey, List<RemoteFileDesc>> presentFiles = remoteFileIO.getPresentRemoteFiles(paths);
        return fillFileInfo(presentFiles, pathKeyToPartition);
    }

    public List<RemoteFileInfo> getRemoteFileInfoForStats(List<Partition> partitions, Optional<String> hudiTableLocation) {
        if (enableCatalogLevelCache) {
            return getPresentFilesInCache(partitions, hudiTableLocation);
        } else {
            return getRemoteFiles(partitions, hudiTableLocation);
        }
    }

    public void refreshPartitionFilesCache(Path path) {
        RemotePathKey remotePathKey = RemotePathKey.of(path.toString(), isRecursive, Optional.empty(), getProperties());
        remoteFileIO.updateRemoteFiles(remotePathKey);
    }

    /**
     * Fill file information for a single path-to-descriptor mapping.
     */
    private List<RemoteFileInfo> fillFileInfo(
            Map<RemotePathKey, List<RemoteFileDesc>> files,
            Map<RemotePathKey, Partition> partitions) {
        List<RemoteFileInfo> result = Lists.newArrayList();

        for (Map.Entry<RemotePathKey, List<RemoteFileDesc>> entry : files.entrySet()) {
            RemotePathKey key = entry.getKey();
            List<RemoteFileDesc> remoteFileDescs = entry.getValue();
            Partition partition = partitions.get(key);
            result.add(buildRemoteFileInfo(partition, remoteFileDescs));
        }

        return result;
    }

    private RemoteFileInfo buildRemoteFileInfo(Partition partition, List<RemoteFileDesc> fileDescs) {
        List<RemoteFileDesc> processedDescs = fileDescs;

        if (partition.getInputFormat().equals(RemoteFileInputFormat.FORMATFILE)) {
            processedDescs = new ArrayList<>(fileDescs.size());
            for (RemoteFileDesc desc : fileDescs) {
                if (desc instanceof StorageFormatRemoteFileDesc) {
                    processedDescs.add(desc);
                } else {
                    if (Config.enable_split_storage_format) {
                        processedDescs.add(toStorageFormatDesc(partition, desc));
                    } else {
                        processedDescs.add(convertToWholeFileDesc(desc));
                    }
                }
            }
        }

        return RemoteFileInfo.builder()
                .setFormat(partition.getInputFormat())
                .setFullPath(partition.getFullPath())
                .setFiles(processedDescs.stream()
                        .map(desc -> desc.setTextFileFormatDesc(partition.getTextFileFormatDesc()))
                        .map(desc -> desc.setSplittable(partition.isSplittable()))
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * Convert a RemoteFileDesc to a whole-file descriptor for FORMATFILE format.
     */
    private RemoteFileDesc convertToWholeFileDesc(RemoteFileDesc desc) {
        RemoteFileBlockDesc blockDesc = desc.getBlockDescs().get(0);
        RemoteFileBlockDesc wholeFileBlock = new RemoteFileBlockDesc(0, desc.getLength(),
                blockDesc.getReplicaHostIds(), new long[] {-1}, blockDesc.getHiveRemoteFileIO());
        return new RemoteFileDesc(desc.getFileName(), "", desc.getLength(),
                desc.getModificationTime(), ImmutableList.of(wholeFileBlock));
    }

    /** Convert RemoteFileDesc to StorageFormat format */
    private RemoteFileDesc toStorageFormatDesc(Partition partition, RemoteFileDesc desc) {
        List<CombineFileSplit> splitList = new ArrayList<>(desc.getBlockDescs().size());
        Path[] paths = {new Path(partition.getFullPath(), desc.getFileName())};

        for (RemoteFileBlockDesc blockDesc : desc.getBlockDescs()) {
            splitList.add(new CombineFileSplit(null, paths,
                    new long[] {blockDesc.getOffset()},
                    new long[] {blockDesc.getLength()},
                    new String[] {""}));
        }
        return StorageFormatRemoteFileDesc.createStorageFormatRemoteFileDesc(desc.getLength(), splitList);
    }

    /**
     * Collect remote file information from multiple path-to-descriptor mappings.
     */
    private void collectRemoteFileInfos(
            List<Map<RemotePathKey, List<RemoteFileDesc>>> result,
            Map<RemotePathKey, Partition> pathKeyToPartition,
            List<RemoteFileInfo> resultRemoteFiles) {
        for (Map<RemotePathKey, List<RemoteFileDesc>> pathToDesc : result) {
            resultRemoteFiles.addAll(fillFileInfo(pathToDesc, pathKeyToPartition));
        }
    }

    public void invalidateAll() {
        remoteFileIO.invalidateAll();
    }

    public Executor getUpdateFsExecutor() {
        return updateRemoteFilesExecutor;
    }

    public void asyncRenameFiles(
            List<CompletableFuture<?>> renameFileFutures,
            AtomicBoolean cancelled,
            Path writePath,
            Path targetPath,
            List<String> fileNames) {
        FileSystem fileSystem;
        try {
            fileSystem = HiveWriteUtils.getTAuthFileSystem(writePath, conf);
        } catch (Exception e) {
            Log.error("Failed to get fileSystem", e);
            throw new StarRocksConnectorException("Failed to move data files to target location. " +
                    "Failed to get file system on path %s. msg: %s", writePath, e.getMessage());
        }

        for (String fileName : fileNames) {
            Path source = new Path(writePath, fileName);
            Path target = new Path(targetPath, fileName);
            renameFileFutures.add(CompletableFuture.runAsync(() -> {
                if (cancelled.get()) {
                    return;
                }
                try {
                    if (fileSystem.exists(target)) {
                        throw new StarRocksConnectorException("Failed to move data files from %s to target location %s. msg:" +
                                " target location already exists", source, target);
                    }

                    if (source.toUri().toString().equals(target.toUri().toString())) {
                        LOG.info("source {}} = target {}, skip rename", source.toUri(), target.toUri());
                        return;
                    }

                    if (!fileSystem.rename(source, target)) {
                        throw new StarRocksConnectorException("Failed to move data files from %s to target location %s. msg:" +
                                " rename operation failed", source, target);
                    }
                } catch (IOException e) {
                    LOG.error("Failed to rename data files", e);
                    throw new StarRocksConnectorException("Failed to move data files from %s to final location %s. msg: %s",
                            source, target, e.getMessage());
                }
            }, updateRemoteFilesExecutor));
        }
    }

    public void renameDirectory(Path source, Path target, Runnable runWhenPathNotExist) {
        if (pathExists(target)) {
            throw new StarRocksConnectorException("Unable to rename from %s to %s. msg: target directory already exists",
                    source, target);
        }

        if (!pathExists(target.getParent())) {
            createDirectory(target.getParent(), conf);
        }

        runWhenPathNotExist.run();

        try {
            if (source.toUri().toString().equals(target.toUri().toString())) {
                LOG.info("source {} = target {}, skip rename", source.toUri(), target.toUri());
                return;
            }
            FileSystem fileSystem = HiveWriteUtils.getTAuthFileSystem(source, conf);
            if (!fileSystem.rename(source, target)) {
                throw new StarRocksConnectorException("Failed to rename %s to %s: rename returned false", source, target);
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException("Failed to rename %s to %s, msg: %s", source, target, e.getMessage());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (SecureException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeNotCurrentQueryFiles(Path partitionPath, String queryId) {
        try {
            FileSystem fileSystem = FileSystem.get(partitionPath.toUri(), conf);
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(partitionPath, false);
            while (iterator.hasNext()) {
                Path file = iterator.next().getPath();
                if (!fileCreatedByQuery(file.getName(), queryId)) {
                    checkedDelete(fileSystem, file, false);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to delete partition {} files when overwriting on s3", partitionPath, e);
            throw new StarRocksConnectorException("Failed to delete partition %s files during overwrite. msg: %s",
                    partitionPath, e.getMessage());
        }
    }

    public boolean pathExists(Path path) {
        return HiveWriteUtils.pathExists(path, conf);
    }

    public boolean deleteIfExists(Path path, boolean recursive) {
        return HiveWriteUtils.deleteIfExists(path, recursive, conf);
    }

    public FileStatus[] listStatus(Path path) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return fileSystem.listStatus(path);
        } catch (Exception e) {
            LOG.error("Failed to list path {}", path, e);
            throw new StarRocksConnectorException("Failed to list path %s. msg: %s", path.toString(), e.getMessage());
        }
    }

    private Map<String, String> getProperties() {
        String username = TdwUtil.getTdwUserName();
        Map<String, String> properties = new ConcurrentHashMap<>();
        if (StringUtils.isNotEmpty(username)) {
            properties.put(USER_NAME_KEY, username);
        }
        if (ConnectContext.get() != null && (ConnectContext.get().getSessionVariable() != null)) {
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            properties.put("forceScheduleLocal", String.valueOf(sessionVariable.getForceScheduleLocal()));
        }
        return properties;
    }

    /**
     * Calculate and log the queue time statistics for tasks in the thread pool.
     * @param submitTime task submit time
     * @param startExecutionTimes queue of task execution start times
     * @param mode execution mode name
     * @param taskCount number of tasks
     * @param parallelWorkerCount number of parallel workers
     */
    private void logQueueTimeStats(long submitTime, Queue<Long> startExecutionTimes,
                                   String mode, int taskCount, int parallelWorkerCount) {
        if (startExecutionTimes.isEmpty()) {
            return;
        }

        long totalQueueTime = 0;
        long maxQueueTime = 0;
        long minQueueTime = Long.MAX_VALUE;
        int validCount = 0;

        Long executionStartTime;
        while ((executionStartTime = startExecutionTimes.poll()) != null) {
            long queueTime = executionStartTime - submitTime;
            totalQueueTime += queueTime;
            maxQueueTime = Math.max(maxQueueTime, queueTime);
            minQueueTime = Math.min(minQueueTime, queueTime);
            validCount++;
            // Update metrics with queue time statistics
            if (MetricRepo.hasInit) {
                MetricRepo.HISTO_REMOTE_FILE_QUEUE_TIME.update(queueTime);
            }
        }

        if (minQueueTime == Long.MAX_VALUE) {
            minQueueTime = 0;
        }

        long avgQueueTime = totalQueueTime / validCount;

        // Get query ID for log correlation
        String queryId = "N/A";
        if (ConnectContext.get() != null && ConnectContext.get().getQueryId() != null) {
            queryId = ConnectContext.get().getQueryId().toString();
        }

        // Log queue time statistics
        LOG.debug("[{}] Thread pool queue time stats - queryId: {}, taskCount: {}, executedCount: {}, " +
                        "parallelWorkerCount: {}, avgQueueTime: {} ms, maxQueueTime: {} ms, minQueueTime: {} ms, " +
                        "totalQueueTime: {} ms", mode, queryId, taskCount, validCount, parallelWorkerCount, avgQueueTime,
                maxQueueTime, minQueueTime, totalQueueTime);

        Map<String, Object> queueTimeStatsMap = Maps.newHashMap();
        queueTimeStatsMap.put("avgQueueTime", avgQueueTime + "ms");
        queueTimeStatsMap.put("maxQueueTime", maxQueueTime + "ms");
        queueTimeStatsMap.put("minQueueTime", minQueueTime + "ms");
        queueTimeStatsMap.put("parallelWorkerCount", parallelWorkerCount);
        queueTimeStatsMap.put("partitionsNum", taskCount);
        Tracers.record(Tracers.Module.EXTERNAL, HMS_PARTITIONS_REMOTE_FILES + ".QUEUE_WAIT_TIME",
                GsonUtils.GSON.toJson(queueTimeStatsMap));
    }
}
