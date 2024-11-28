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
import com.starrocks.qe.ConnectContext;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
        List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures = Lists.newArrayList();
        List<Map<RemotePathKey, List<RemoteFileDesc>>> result = Lists.newArrayList();

        RemotePathKey.HudiContext hudiContext = new RemotePathKey.HudiContext();

        long remoteFilePullTimeout = Long.MAX_VALUE;
        if (ConnectContext.get() != null && (ConnectContext.get().getSessionVariable() != null)) {
            remoteFilePullTimeout = ConnectContext.get().getSessionVariable().getRemoteFilePullTimeout();
        }
        Tracers.count(Tracers.Module.EXTERNAL, HMS_PARTITIONS_REMOTE_FILES, cacheMissSize);
        try (Timer ignored = Tracers.watchScope(Tracers.Module.EXTERNAL, HMS_PARTITIONS_REMOTE_FILES)) {
            for (Partition partition : partitions) {
                try {
                    String authority = new Path(partition.getFullPath()).toUri().getAuthority();
                    int index = 0;
                    if (StringUtils.isNotEmpty(authority)) {
                        index = Math.abs(authority.hashCode()) % pullRemoteFileExecutors.size();
                    }
                    RemotePathKey pathKey =
                            RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation, getProperties());
                    pathKey.setHudiContext(hudiContext);
                    Future<Map<RemotePathKey, List<RemoteFileDesc>>> future = pullRemoteFileExecutors.get(index).submit(() ->
                            remoteFileIO.getRemoteFiles(pathKey, useCache));
                    futures.add(future);
                } catch (Throwable e) {
                    try {
                        for (Future<Map<RemotePathKey, List<RemoteFileDesc>>> entry : futures) {
                            entry.cancel(true);
                        }
                    } catch (Throwable throwable) {
                        LOG.warn("Failed to cancel future", throwable);
                    }
                    throw new StarRocksConnectorException("Add task to remoteFileExecutor error, msg: %s",
                            e.getMessage());
                }
            }

            long startTime = System.currentTimeMillis();
            for (Future<Map<RemotePathKey, List<RemoteFileDesc>>> future : futures) {
                try {
                    long timeout = remoteFilePullTimeout - (System.currentTimeMillis() - startTime);
                    if (timeout > 0) {
                        result.add(future.get(timeout, TimeUnit.MILLISECONDS));
                    } else {
                        result.add(future.get(1, TimeUnit.MILLISECONDS));
                    }
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    try {
                        for (Future<Map<RemotePathKey, List<RemoteFileDesc>>> entry : futures) {
                            entry.cancel(true);
                        }
                    } catch (Throwable throwable) {
                        LOG.warn("Failed to cancel future", throwable);
                    }
                    String errMsg = e.getMessage();
                    if (errMsg == null) {
                        errMsg = "timeout with " + remoteFilePullTimeout + " ms.";
                    }
                    throw new StarRocksConnectorException("Failed to get remote files, msg: %s", errMsg);
                }
            }
        }

        for (Map<RemotePathKey, List<RemoteFileDesc>> pathToDesc : result) {
            resultRemoteFiles.addAll(fillFileInfo(pathToDesc, pathKeyToPartition));
        }

        return resultRemoteFiles;
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
        RemotePathKey remotePathKey = RemotePathKey.of(path.toString(), isRecursive);
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
        Map<String, String> properties = new HashMap<>();
        if (StringUtils.isNotEmpty(username)) {
            properties.put(USER_NAME_KEY, username);
        }
        return properties;
    }
}
