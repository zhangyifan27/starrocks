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

        // Check if async pull is enabled (read from SessionVariable)
        boolean enableAsyncPull = true;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            enableAsyncPull = ConnectContext.get().getSessionVariable().isEnableAsyncPullRemoteFile();
        }

        // If async pull is disabled, use synchronous mode
        if (!enableAsyncPull) {
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_REMOTE_FILE_GET_SYNC.increase(1L);
            }
            List<RemoteFileInfo> result = getRemoteFilesSynchronously(partitions, hudiTableLocation, useCache,
                    pathKeyToPartition, cacheMissSize);
            if (MetricRepo.hasInit) {
                // Record latency for synchronous mode
                long elapseMs = System.currentTimeMillis() - startTime;
                MetricRepo.HISTO_REMOTE_FILE_OPERATIONS_LATENCY.update(elapseMs);
            }
            return result;
        }

        // Record async mode calls
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_REMOTE_FILE_GET_ASYNC.increase(1L);
        }

        List<RemoteFileInfo> resultRemoteFiles = Lists.newArrayList();
        List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures = Lists.newArrayList();
        List<Map<RemotePathKey, List<RemoteFileDesc>>> result = Lists.newArrayList();

        RemotePathKey.HudiContext hudiContext = new RemotePathKey.HudiContext();

        long remoteFilePullTimeout = Long.MAX_VALUE;
        if (ConnectContext.get() != null && (ConnectContext.get().getSessionVariable() != null)) {
            remoteFilePullTimeout = ConnectContext.get().getSessionVariable().getRemoteFilePullTimeout();
            int queryTimeoutS = ConnectContext.get().getSessionVariable().getQueryTimeoutS();
            remoteFilePullTimeout = Math.min(remoteFilePullTimeout, queryTimeoutS * 1000L);
        }
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
                executeWithTaskQueueMode(partitions, result, remoteFilePullTimeout,
                        hudiContext, isRecursive, hudiTableLocation, useCache, configuredWorkerCount, properties);
            } else {
                // Original mode: submit one task per partition
                executeWithOriginalMode(partitions, futures, result, remoteFilePullTimeout,
                        hudiContext, isRecursive, hudiTableLocation, useCache, properties);
            }
        } catch (Throwable e) {
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_REMOTE_FILE_GET_ERR.increase(1L);
            }
            throw e;
        }

        for (Map<RemotePathKey, List<RemoteFileDesc>> pathToDesc : result) {
            resultRemoteFiles.addAll(fillFileInfo(pathToDesc, pathKeyToPartition));
        }

        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_REMOTE_FILE_GET_SUCCESS.increase(1L);
            // Record latency for asynchronous mode
            long elapseMs = System.currentTimeMillis() - startTime;
            MetricRepo.HISTO_REMOTE_FILE_OPERATIONS_LATENCY.update(elapseMs);
        }

        // Log slow operations (> 1 minute)
        long elapseMs = System.currentTimeMillis() - startTime;
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
     * Original mode: submit one task per partition
     */
    private void executeWithOriginalMode(
            List<Partition> partitions,
            List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures,
            List<Map<RemotePathKey, List<RemoteFileDesc>>> result,
            long remoteFilePullTimeout,
            RemotePathKey.HudiContext hudiContext,
            boolean isRecursive,
            Optional<String> hudiTableLocation,
            boolean useCache,
            Map<String, String> properties) {
        try {
            long startTime = System.currentTimeMillis();
            // Submit tasks for all partitions
            for (int i = 0; i < partitions.size(); i++) {
                Partition partition = partitions.get(i);
                try {
                    String authority = new Path(partition.getFullPath()).toUri().getAuthority();
                    int index = 0;
                    if (StringUtils.isNotEmpty(authority)) {
                        index = Math.abs(authority.hashCode()) % pullRemoteFileExecutors.size();
                    }
                    RemotePathKey pathKey =
                            RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation, properties);
                    pathKey.setHudiContext(hudiContext);
                    Future<Map<RemotePathKey, List<RemoteFileDesc>>> future = pullRemoteFileExecutors.get(index).submit(() ->
                            remoteFileIO.getRemoteFiles(pathKey, useCache));
                    futures.add(future);
                } catch (Exception e) {
                    cancelAllFutures(futures);
                    throw new StarRocksConnectorException(
                            "Failed to submit task for partition %s (index: %d/%d), msg: %s",
                            partition.getFullPath(), i + 1, partitions.size(), e.getMessage());
                }
            }

            // Collect results from all futures
            for (int i = 0; i < futures.size(); i++) {
                Future<Map<RemotePathKey, List<RemoteFileDesc>>> future = futures.get(i);
                try {
                    long timeout = remoteFilePullTimeout - (System.currentTimeMillis() - startTime);
                    if (timeout <= 0) {
                        cancelAllFutures(futures);
                        throw new StarRocksConnectorException(
                                "Timeout after processing %d/%d partitions, total timeout: %d ms",
                                i, futures.size(), remoteFilePullTimeout);
                    }
                    result.add(future.get(timeout, TimeUnit.MILLISECONDS));
                } catch (TimeoutException e) {
                    cancelAllFutures(futures);
                    throw new StarRocksConnectorException(
                            "Timeout while getting remote files for partition %d/%d, timeout: %d ms",
                            i + 1, futures.size(), remoteFilePullTimeout);
                } catch (InterruptedException e) {
                    cancelAllFutures(futures);
                    Thread.currentThread().interrupt();
                    throw new StarRocksConnectorException(
                            "Interrupted while getting remote files for partition %d/%d",
                            i + 1, futures.size());
                } catch (ExecutionException e) {
                    cancelAllFutures(futures);
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    throw new StarRocksConnectorException(
                            "Failed to get remote files for partition %d/%d, msg: %s",
                            i + 1, futures.size(), cause.getMessage());
                }
            }
        } catch (Throwable e) {
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_REMOTE_FILE_GET_ERR.increase(1L);
            }
            throw e;
        }
    }

    /**
     * Cancel all futures in the list
     */
    private void cancelAllFutures(List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures) {
        for (Future<Map<RemotePathKey, List<RemoteFileDesc>>> future : futures) {
            try {
                future.cancel(true);
            } catch (Exception e) {
                LOG.warn("Failed to cancel future: " + e.getMessage());
            }
        }
    }

    /**
     * Task queue mode: put all partitions into a queue, fixed number of workers fetch tasks from the queue
     */
    private void executeWithTaskQueueMode(
            List<Partition> partitions,
            List<Map<RemotePathKey, List<RemoteFileDesc>>> result,
            long remoteFilePullTimeout,
            RemotePathKey.HudiContext hudiContext,
            boolean isRecursive,
            Optional<String> hudiTableLocation,
            boolean useCache,
            int configuredWorkerCount,
            Map<String, String> properties) {
        // Declare these outside try block so they can be accessed in catch block
        AtomicBoolean cancelled = new AtomicBoolean(false);
        List<Future<Void>> workerFutures = Lists.newArrayList();
        try {
            long startTime = System.currentTimeMillis();
            // Determine worker count: configurable via session variable, default to min(executor pool size, partition count)
            int workerCount = Math.min(configuredWorkerCount, partitions.size());
            // Ensure at least one worker
            workerCount = Math.max(1, workerCount);

            // Use task queue pattern: put all partitions into a queue, fixed number of workers fetch tasks from the queue
            BlockingQueue<Partition> taskQueue = new LinkedBlockingQueue<>(partitions);
            // Use ConcurrentHashMap to store results, thread-safe
            Map<RemotePathKey, List<RemoteFileDesc>> concurrentResult = new ConcurrentHashMap<>();
            // Used to capture exceptions
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(workerCount);

            ExecutorService executor = pullRemoteFileExecutors.get(0);
            if (pullRemoteFileExecutors.size() > 1) {
                // Select executor using round-robin to achieve load balancing
                long counter = executorRoundRobinIndex.getAndIncrement();
                int index = (int) (Math.abs(counter) % pullRemoteFileExecutors.size());
                executor = pullRemoteFileExecutors.get(index);
            }

            // Submit fixed number of worker tasks
            for (int i = 0; i < workerCount; i++) {
                final int workerIndex = i;

                Callable<Void> worker = () -> {
                    try {
                        while (!cancelled.get()) {
                            // Fetch task from queue, use poll to avoid blocking
                            Partition partition = taskQueue.poll();
                            if (partition == null) {
                                // Queue is empty, task completed
                                break;
                            }

                            try {
                                RemotePathKey pathKey = RemotePathKey.of(
                                        partition.getFullPath(),
                                        isRecursive,
                                        hudiTableLocation,
                                        properties);
                                pathKey.setHudiContext(hudiContext);

                                // Execute actual file retrieval operation
                                Map<RemotePathKey, List<RemoteFileDesc>> files =
                                        remoteFileIO.getRemoteFiles(pathKey, useCache);

                                // Put results into concurrent-safe Map
                                concurrentResult.putAll(files);

                            } catch (Throwable e) {
                                LOG.error("Worker {} failed to process partition {} with error: {}",
                                        workerIndex, partition.getFullPath(), e.getMessage());
                                // Record the first error
                                errorRef.compareAndSet(null, e);
                                // Set cancellation flag to notify other workers to stop
                                cancelled.set(true);
                                // Clear queue to prevent other workers from continuing
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
                    // If submission fails, fail fast and let outer catch handle cleanup
                    LOG.error("Failed to submit worker {}: {}", i, e.getMessage());
                    throw new StarRocksConnectorException(
                            "Failed to submit worker task %d/%d, msg: %s", i + 1, workerCount, e.getMessage());
                }
            }

            // Wait for all workers to complete with timeout control
            try {
                long timeout = remoteFilePullTimeout - (System.currentTimeMillis() - startTime);
                if (timeout <= 0) {
                    // Already timeout, let outer catch handle cleanup
                    throw new StarRocksConnectorException(
                            "Failed to get remote files, msg: timeout before workers start, total timeout: %d ms.",
                            remoteFilePullTimeout);
                }

                boolean completed = latch.await(timeout, TimeUnit.MILLISECONDS);

                if (!completed) {
                    // Timeout, let outer catch handle cleanup
                    throw new StarRocksConnectorException(
                            "Failed to get remote files, msg: timeout after %d ms (limit: %d ms).",
                            System.currentTimeMillis() - startTime, remoteFilePullTimeout);
                }

                // Check if any error occurred
                Throwable error = errorRef.get();
                if (error != null) {
                    throw new StarRocksConnectorException("Failed to get remote files", error);
                }

                // Add results to result list
                result.add(concurrentResult);

            } catch (InterruptedException e) {
                // Interrupted, restore interrupt status and let outer catch handle cleanup
                Thread.currentThread().interrupt();
                throw new StarRocksConnectorException(
                        "Failed to get remote files, msg: interrupted");
            }
        } catch (Throwable e) {
            // Cancel all workers on any error
            cancelAllWorkers(cancelled, workerFutures);
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_REMOTE_FILE_GET_ERR.increase(1L);
            }
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
        if (partition.getInputFormat().equals(RemoteFileInputFormat.FORMATFILE)) {
            return buildRemoteFileInfoForStorageFormat(partition, fileDescs);
        } else {
            RemoteFileInfo.Builder builder = RemoteFileInfo.builder()
                    .setFormat(partition.getInputFormat())
                    .setFullPath(partition.getFullPath())
                    .setFiles(fileDescs.stream()
                            .map(desc -> desc.setTextFileFormatDesc(partition.getTextFileFormatDesc()))
                            .map(desc -> desc.setSplittable(partition.isSplittable()))
                            .collect(Collectors.toList()));

            return builder.build();
        }
    }

    private RemoteFileInfo buildRemoteFileInfoForStorageFormat(Partition partition, List<RemoteFileDesc> fileDescs) {
        if (Config.enable_split_storage_format) {
            return StorageFormatUtils.buildRemoteFileInfoForStorageFormat(partition, fileDescs);
        }
        List<RemoteFileDesc> sfFileDescs = new ArrayList<>(fileDescs.size());
        for (RemoteFileDesc desc : fileDescs) {
            List<RemoteFileBlockDesc> fileBlockDescs = new ArrayList<>(1);
            // file as a whole
            RemoteFileBlockDesc wholeFileBlockDesc = new RemoteFileBlockDesc(0,
                    desc.getLength(),
                    desc.getBlockDescs().get(0).getReplicaHostIds(),
                    new long[] {-1},
                    desc.getBlockDescs().get(0).getHiveRemoteFileIO());
            fileBlockDescs.add(wholeFileBlockDesc);
            RemoteFileDesc sfFileDesc = new RemoteFileDesc(desc.getFileName(), "", desc.getLength(),
                    desc.getModificationTime(), ImmutableList.copyOf(fileBlockDescs));
            sfFileDescs.add(sfFileDesc);
        }
        RemoteFileInfo.Builder builder = RemoteFileInfo.builder()
                .setFormat(partition.getInputFormat())
                .setFullPath(partition.getFullPath())
                .setFiles(sfFileDescs.stream()
                        .map(desc -> desc.setTextFileFormatDesc(partition.getTextFileFormatDesc()))
                        .map(desc -> desc.setSplittable(partition.isSplittable()))
                        .collect(Collectors.toList()));

        return builder.build();
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
     * Synchronously get remote files without using thread pool.
     * This method is used when enable_async_pull_remote_file is set to false.
     */
    private List<RemoteFileInfo> getRemoteFilesSynchronously(
            List<Partition> partitions,
            Optional<String> hudiTableLocation,
            boolean useCache,
            Map<RemotePathKey, Partition> pathKeyToPartition,
            int cacheMissSize) {
        List<RemoteFileInfo> resultRemoteFiles = Lists.newArrayList();
        List<Map<RemotePathKey, List<RemoteFileDesc>>> result = Lists.newArrayList();

        RemotePathKey.HudiContext hudiContext = new RemotePathKey.HudiContext();

        Tracers.count(Tracers.Module.EXTERNAL, HMS_PARTITIONS_REMOTE_FILES, cacheMissSize);
        try (Timer ignored = Tracers.watchScope(Tracers.Module.EXTERNAL, HMS_PARTITIONS_REMOTE_FILES)) {
            for (Partition partition : partitions) {
                RemotePathKey pathKey =
                        RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation, getProperties());
                pathKey.setHudiContext(hudiContext);
                // Directly call getRemoteFiles without thread pool
                Map<RemotePathKey, List<RemoteFileDesc>> files = remoteFileIO.getRemoteFiles(pathKey, useCache);
                result.add(files);
            }
        } catch (Throwable e) {
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_REMOTE_FILE_GET_ERR.increase(1L);
            }
            throw e;
        }

        for (Map<RemotePathKey, List<RemoteFileDesc>> pathToDesc : result) {
            resultRemoteFiles.addAll(fillFileInfo(pathToDesc, pathKeyToPartition));
        }

        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_REMOTE_FILE_GET_SUCCESS.increase(1L);
        }
        return resultRemoteFiles;
    }

    /**
     * Cancel all worker tasks and set the cancelled flag.
     * This method is used to stop all running workers when an error occurs or timeout happens.
     *
     * @param cancelled the atomic boolean flag to indicate cancellation
     * @param workerFutures the list of worker futures to cancel
     */
    private void cancelAllWorkers(AtomicBoolean cancelled, List<Future<Void>> workerFutures) {
        cancelled.set(true);
        for (Future<Void> future : workerFutures) {
            future.cancel(true);
        }
    }
}
