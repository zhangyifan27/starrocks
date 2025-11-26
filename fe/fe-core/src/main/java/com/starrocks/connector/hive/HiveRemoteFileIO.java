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


package com.starrocks.connector.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.fs.hdfs.HdfsFs;
import com.starrocks.metric.MetricRepo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

public class HiveRemoteFileIO implements RemoteFileIO {
    private static final Logger LOG = LogManager.getLogger(HiveRemoteFileIO.class);

    private final Configuration configuration;

    // only used for ut.
    private FileSystem fileSystem;

    // blockHost is ip:port
    private final Map<String, Long> blockHostToId = new ConcurrentHashMap<>();
    private final Map<Long, String> idToBlockHost = new ConcurrentHashMap<>();
    private long hostId = 0;
    private static final int UNKNOWN_STORAGE_ID = -1;

    public HiveRemoteFileIO(Configuration configuration) {
        this.configuration = configuration;
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
        Map<RemotePathKey, List<RemoteFileDesc>> result = null;
        long startTime = System.currentTimeMillis();

        boolean forceScheduleLocal = isForceScheduleLocal(pathKey);
        if (forceScheduleLocal) {
            result = getRemoteFiles(pathKey, false);
        } else {
            result = getRemoteFilesWithFileStatus(pathKey, false);
        }
        long elapseMs = System.currentTimeMillis() - startTime;
        if (MetricRepo.hasInit) {
            MetricRepo.HISTO_GET_REMOTE_FILES_LATENCY.update(elapseMs);
        }
        return result;
    }

    /**
     * Parse the forceScheduleLocal configuration from RemotePathKey properties
     * @param pathKey the remote path key
     * @return true if the configuration is true or not set, false otherwise
     */
    private boolean isForceScheduleLocal(RemotePathKey pathKey) {
        Map<String, String> properties = pathKey.getProperties();
        if (properties == null) {
            return true;
        }
        String forceScheduleLocalStr = properties.get("forceScheduleLocal");
        if (forceScheduleLocalStr == null) {
            return true;
        }
        return Boolean.parseBoolean(forceScheduleLocalStr);
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey, boolean expandWildCards) {
        boolean forceScheduleLocal = isForceScheduleLocal(pathKey);
        if (forceScheduleLocal) {
            return getRemoteFilesInternal(pathKey, expandWildCards, true);
        } else {
            return getRemoteFilesInternal(pathKey, expandWildCards, false);
        }
    }

    private RemoteIterator<LocatedFileStatus> listFilesRecursive(FileSystem fileSystem, Path f)
        throws FileNotFoundException, IOException {
        return new RemoteIterator<LocatedFileStatus>() {
            private Stack<RemoteIterator<LocatedFileStatus>> itors = new Stack<>();
            private RemoteIterator<LocatedFileStatus> curItor = fileSystem.listLocatedStatus(f);
            private LocatedFileStatus curFile;

            @Override
            public boolean hasNext() throws IOException {
                while (curFile == null) {
                    if (curItor.hasNext()) {
                        handleFileStat(curItor.next());
                    } else if (!itors.empty()) {
                        curItor = itors.pop();
                    } else {
                        return false;
                    }
                }
                return true;
            }

            // Process the input stat.
            // If it is a file, return the file stat.
            // If it is a valid directory, traverse it.
            private void handleFileStat(LocatedFileStatus stat) throws IOException {
                if (stat.isFile()) {
                    curFile = stat;
                } else if (isValidDirectory(stat)) {
                    try {
                        RemoteIterator<LocatedFileStatus> newDirItor = fileSystem.listLocatedStatus(stat.getPath());
                        itors.push(curItor);
                        curItor = newDirItor;
                    } catch (FileNotFoundException ignored) {
                        LOG.debug("Directory {} deleted while attempting for recursive listing", stat.getPath());
                    }
                }
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                if (hasNext()) {
                    LocatedFileStatus result = curFile;
                    curFile = null;
                    return result;
                }
                throw new java.util.NoSuchElementException("No more entry in " + f);
            }
        };
    }

    private boolean isValidDataFile(FileStatus fileStatus) {
        if (!fileStatus.isFile()) {
            return false;
        }
        String lcFileName = fileStatus.getPath().getName().toLowerCase();
        return !(lcFileName.startsWith(".") || lcFileName.startsWith("_") ||
                lcFileName.endsWith(".copying") || lcFileName.endsWith(".tmp"));
    }

    private boolean isValidDirectory(FileStatus fileStatus) {
        if (!fileStatus.isDirectory()) {
            return false;
        }
        String dirName = fileStatus.getPath().getName();
        return !(dirName.startsWith(".") || dirName.startsWith("_"));
    }

    protected List<RemoteFileBlockDesc> getRemoteFileBlockDesc(BlockLocation[] blockLocations) throws IOException {
        List<RemoteFileBlockDesc> fileBlockDescs = Lists.newArrayList();
        for (BlockLocation blockLocation : blockLocations) {
            fileBlockDescs.add(buildRemoteFileBlockDesc(
                    blockLocation.getOffset(),
                    blockLocation.getLength(),
                    getReplicaHostIds(blockLocation.getNames()))
            );
        }
        return fileBlockDescs;
    }

    public RemoteFileBlockDesc buildRemoteFileBlockDesc(long offset, long length, long[] replicaHostIds) {
        return new RemoteFileBlockDesc(offset,
                length,
                replicaHostIds,
                new long[] {UNKNOWN_STORAGE_ID},
                this);
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFilesWithFileStatus(RemotePathKey pathKey, boolean expandWildCards) {
        return getRemoteFilesInternal(pathKey, expandWildCards, false);
    }

    /**
     * Internal method to get remote files with common logic.
     *
     * @param pathKey the remote path key
     * @param expandWildCards whether to expand wildcards
     * @param useLocatedFileStatus if true, use LocatedFileStatus with block locations;
     *                             if false, use FileStatus and calculate blocks
     * @return map of remote path key to list of remote file descriptors
     */
    private Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFilesInternal(
            RemotePathKey pathKey, boolean expandWildCards, boolean useLocatedFileStatus) {
        long startTime = System.currentTimeMillis();
        ImmutableMap.Builder<RemotePathKey, List<RemoteFileDesc>> resultPartitions = ImmutableMap.builder();
        String path = pathKey.getPath();
        List<RemoteFileDesc> fileDescs = Lists.newArrayList();
        long totalSize = 0;
        StringBuilder pathBuilder = new StringBuilder();

        try {
            URI uri = new Path(path).toUri();
            FileSystem fileSystem = getFileSystem(uri, path, pathKey.getProperties());

            // Expand wildcards if needed
            List<Path> expandedPaths = expandPaths(fileSystem, uri, expandWildCards);

            // Process each expanded path
            for (Path expandedPath : expandedPaths) {
                RemoteIterator<? extends FileStatus> fileIterator = createFileIterator(
                        fileSystem, expandedPath, pathKey.isRecursive(), useLocatedFileStatus);

                while (fileIterator.hasNext()) {
                    FileStatus fileStatus = fileIterator.next();
                    if (!isValidDataFile(fileStatus)) {
                        continue;
                    }

                    String locateName = fileStatus.getPath().toUri().getPath();
                    String fileName = PartitionUtil.getSuffixName(expandedPath.toUri().getPath(), locateName);

                    // Get block descriptors based on the mode
                    List<RemoteFileBlockDesc> fileBlockDescs = useLocatedFileStatus
                            ? getRemoteFileBlockDesc(((LocatedFileStatus) fileStatus).getBlockLocations())
                            : getRemoteFileBlockDesc(fileStatus);

                    RemoteFileDesc fileDesc = new RemoteFileDesc(
                            fileName, "", fileStatus.getLen(),
                            fileStatus.getModificationTime(),
                            ImmutableList.copyOf(fileBlockDescs));

                    totalSize += fileStatus.getLen();
                    pathBuilder.append(fileName).append(",");

                    if (expandWildCards) {
                        fileDesc.setFullPath(fileStatus.getPath().toString());
                    }

                    fileDescs.add(fileDesc);
                }
            }
        } catch (FileNotFoundException e) {
            LOG.warn("Hive remote file on path: {} not existed, ignore it", path, e);
        } catch (Exception e) {
            LOG.error("Failed to get hive remote file's metadata on path: {}", path, e);
            throw new StarRocksConnectorException(
                    "Failed to get hive remote file's metadata on path: %s. msg: %s",
                    pathKey, e.getMessage());
        }

        logPerformanceMetrics(pathKey, startTime, fileDescs.size(), totalSize, pathBuilder);

        return resultPartitions.put(pathKey, fileDescs).build();
    }

    /**
     * Expand paths with wildcard support.
     */
    private List<Path> expandPaths(FileSystem fileSystem, URI uri, boolean expandWildCards) throws IOException {
        List<Path> expandedPaths = Lists.newArrayList();
        if (!expandWildCards) {
            expandedPaths.add(new Path(uri.getPath()));
        } else {
            FileStatus[] status = fileSystem.globStatus(new Path(uri.getPath()));
            for (FileStatus s : status) {
                expandedPaths.add(s.getPath());
            }
        }
        return expandedPaths;
    }

    /**
     * Create appropriate file iterator based on mode.
     */
    private RemoteIterator<? extends FileStatus> createFileIterator(
            FileSystem fileSystem, Path path, boolean recursive, boolean useLocatedFileStatus)
            throws IOException {
        if (useLocatedFileStatus) {
            return recursive
                    ? listFilesRecursive(fileSystem, path)
                    : fileSystem.listLocatedStatus(path);
        } else {
            return recursive
                    ? listFileStatusRecursive(fileSystem, path)
                    : fileSystem.listStatusIterator(path);
        }
    }

    /**
     * Log performance metrics for file retrieval.
     */
    private void logPerformanceMetrics(
            RemotePathKey pathKey, long startTime, int fileCount, long totalSize, StringBuilder pathBuilder) {
        long elapsedTime = System.currentTimeMillis() - startTime;

        if (elapsedTime > Config.remote_file_warn_response_time) {
            LOG.warn("Get remote file for {} take too much time {} ms.", pathKey.toString(), elapsedTime);
        }

        if (Config.print_get_remote_file_info) {
            int traceLogMaxLength = Config.print_remote_file_names_max_length;
            int truncatedLength = Math.min(pathBuilder.length(), traceLogMaxLength);
            String truncatedFlag = truncatedLength == pathBuilder.length() ? "" : " <TRUNCATED>";
            String fileNames = pathBuilder.subSequence(0, truncatedLength) + truncatedFlag;
            LOG.info("Get remote file for {} take time {} ms, get {} num files, totalSize is {}, fileNames is {}",
                    pathKey.toString(), elapsedTime, fileCount, totalSize, fileNames);
        }
    }

    protected List<RemoteFileBlockDesc> getRemoteFileBlockDesc(FileStatus fileStatus) {
        long fileLen = fileStatus.getLen();
        long blockSize = fileStatus.getBlockSize();
        List<RemoteFileBlockDesc> fileBlockDescs = Lists.newArrayList();

        // If file is empty, return empty list
        if (fileLen == 0) {
            return fileBlockDescs;
        }

        // Calculate number of blocks based on file size and block size
        long offset = 0;
        while (offset < fileLen) {
            long length = Math.min(blockSize, fileLen - offset);
            // Use empty replica host IDs since we don't have actual block location info
            fileBlockDescs.add(buildRemoteFileBlockDesc(offset, length, new long[0]));
            offset += length;
        }

        return fileBlockDescs;
    }

    private RemoteIterator<FileStatus> listFileStatusRecursive(FileSystem fileSystem, Path f)
            throws FileNotFoundException, IOException {
        return new RemoteIterator<FileStatus>() {
            private Stack<RemoteIterator<FileStatus>> itors = new Stack<>();
            private RemoteIterator<FileStatus> curItor = fileSystem.listStatusIterator(f);
            private FileStatus curFile;

            @Override
            public boolean hasNext() throws IOException {
                while (curFile == null) {
                    if (curItor.hasNext()) {
                        handleFileStat(curItor.next());
                    } else if (!itors.empty()) {
                        curItor = itors.pop();
                    } else {
                        return false;
                    }
                }
                return true;
            }

            // Process the input stat.
            // If it is a file, return the file stat.
            // If it is a valid directory, traverse it.
            private void handleFileStat(FileStatus stat) throws IOException {
                if (stat.isFile()) {
                    curFile = stat;
                } else if (isValidDirectory(stat)) {
                    try {
                        RemoteIterator<FileStatus> newDirItor = fileSystem.listStatusIterator(stat.getPath());
                        itors.push(curItor);
                        curItor = newDirItor;
                    } catch (FileNotFoundException ignored) {
                        LOG.debug("Directory {} deleted while attempting for recursive listing", stat.getPath());
                    }
                }
            }

            @Override
            public FileStatus next() throws IOException {
                if (hasNext()) {
                    FileStatus result = curFile;
                    curFile = null;
                    return result;
                }
                throw new java.util.NoSuchElementException("No more entry in " + f);
            }
        };
    }

    public long[] getReplicaHostIds(String[] hostNames) {
        long[] replicaHostIds = new long[hostNames.length];
        for (int j = 0; j < hostNames.length; j++) {
            String name = hostNames[j];
            replicaHostIds[j] = getHostId(name);
        }
        return replicaHostIds;
    }

    public long getHostId(String hostName) {
        return blockHostToId.computeIfAbsent(hostName, k -> {
            long newId = hostId++;
            idToBlockHost.put(newId, hostName);
            return newId;
        });
    }

    public String getHdfsDataNodeIp(long hostId) {
        String hostPort = idToBlockHost.get(hostId);
        return hostPort.split(":")[0];
    }

    @VisibleForTesting
    public void setFileSystem(FileSystem fs) {
        this.fileSystem = fs;
    }

    private FileSystem getFileSystem(URI uri, String path, Map<String, String> properties)
            throws IOException, UserException {
        if (FeConstants.runningUnitTest) {
            return this.fileSystem;
        }
        if (Config.enable_hdfs_tauth_authentication) {
            HdfsFs hdfsFs = HdfsUtil.getHdfsService().getHdfsFsManager().getFileSystem(path, properties, null);
            return hdfsFs.getDFSFileSystem();
        } else {
            return FileSystem.get(uri, configuration);
        }
    }
}
