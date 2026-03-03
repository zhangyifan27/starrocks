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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.connector.hudi.HudiRemoteFileDesc;
import com.starrocks.datacache.DataCacheExprRewriter;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.datacache.DataCacheOptions;
import com.starrocks.datacache.DataCacheRemoteFileDesc;
import com.starrocks.datacache.DataCacheRule;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDataCacheOptions;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RemoteScanRangeLocations {
    private static final Logger LOG = LogManager.getLogger(RemoteScanRangeLocations.class);

    private final List<TScanRangeLocations> result = new ArrayList<>();
    private List<DescriptorTable.ReferencedPartitionInfo> partitionInfos = new ArrayList<>();
    private boolean forceScheduleLocal = false;
    private boolean canBackendSplitFile = false;

    private List<RemoteFileInfo> partitions = new ArrayList<>();
    private long fileNum = 0;
    private long fileSizeBytes = 0;
    private long maxScanSizeBytes = Long.MAX_VALUE;
    private long maxScanRowCount = Long.MAX_VALUE;

    // Scan range count and limit tracking
    private boolean isThiveTable = false;
    private int scanRangeLimit = 0;
    private int scanRangeCount = 0;

    public void setup(DescriptorTable descTbl, Table table, HDFSScanNodePredicates scanNodePredicates) {
        Collection<Long> selectedPartitionIds = scanNodePredicates.getSelectedPartitionIds();
        if (selectedPartitionIds.isEmpty()) {
            return;
        }

        // Sort selectedPartitionIds by PartitionKey in descending order only when pruning for simple query
        List<Long> partitionIdList = new ArrayList<>(selectedPartitionIds);
        if (shouldPrunePartitionForSimpleQuery()) {
            try {
                // Only sort when partition count is less than the configured limit
                int sortMaxNum = ConnectContext.get().getSessionVariable().getPrunePartitionSimpleQuerySortMaxNum();
                // sortMaxNum <= 0 means no limit, always sort
                if (sortMaxNum <= 0 || partitionIdList.size() <= sortMaxNum) {
                    Map<Long, PartitionKey> idToPartitionKey = scanNodePredicates.getIdToPartitionKey();
                    Collections.sort(partitionIdList, (o1, o2) -> {
                        return idToPartitionKey.get(o2).compareTo(idToPartitionKey.get(o1));
                    });
                }
            } catch (Exception e) {
                LOG.warn("Failed to sort partition ids by partition key, skip sorting, queryId={}: {}",
                        ConnectContext.get().getQueryId(), e.getMessage());
            }
        }

        List<PartitionKey> partitionKeys = Lists.newArrayList();
        for (long partitionId : partitionIdList) {
            PartitionKey partitionKey = scanNodePredicates.getIdToPartitionKey().get(partitionId);
            DescriptorTable.ReferencedPartitionInfo partitionInfo =
                    new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey);
            partitionInfos.add(partitionInfo);
            partitionKeys.add(partitionKey);
            descTbl.addReferencedPartitions(table, partitionInfo);
        }

        forceScheduleLocal = false;
        ConnectContext connectContext = ConnectContext.get();
        SessionVariable sessionVariable = null;
        int datafileLimit = 0;
        boolean isThiveTable = false;

        if (connectContext != null) {
            // ConnectContext sometimes will be nullptr, we need to cover it up
            sessionVariable = connectContext.getSessionVariable();
            if (sessionVariable != null) {
                datafileLimit = sessionVariable.getScanHiveDatafileNumLimit();
                forceScheduleLocal = sessionVariable.getForceScheduleLocal();
            }

            isThiveTable = table instanceof HiveTable && ((HiveTable) table).isThiveTable();
        }

        HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;
        String catalogName = hiveMetaStoreTable.getCatalogName();
        if (shouldPrunePartitionForSimpleQuery()) {
            tryPrunePartitionForSimpleQuery(descTbl, catalogName, table, partitionKeys);
        } else {
            try {
                partitions = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                        catalogName, table, partitionKeys);
            } catch (Exception e) {
                LOG.error("Failed to get remote files", e);
                throw e;
            }
        }

        int scannableFileCount = 0;

        for (int i = 0; i < partitions.size(); i++) {
            for (RemoteFileDesc fileDesc : partitions.get(i).getFiles()) {
                fileNum++;
                long fileLength = fileDesc.getLength();
                if (fileLength > 0) {
                    fileSizeBytes += fileLength;
                    scannableFileCount++;

                    if (isThiveTable && datafileLimit > 0 && scannableFileCount > datafileLimit) {
                        String msg = "Exceeded the limit of " + datafileLimit + " max scan thive external data files";
                        LOG.warn("{} queryId: {}", msg, DebugUtil.printId(connectContext.getQueryId()));
                        throw new SemanticException(msg);
                    }
                }
            }
        }
    }

    void tryPrunePartitionForSimpleQuery(DescriptorTable descTbl, String catalogName, Table table,
                                                 List<PartitionKey> partitionKeys) {
        ConnectContext context = ConnectContext.get();
        long simpleLimit = context.getSimpleLimit();
        if (simpleLimit <= context.getSessionVariable().getPrunePartitionSimpleQueryMaxLimit()) {
            long totalSize = 0;
            long totalFileRowCount = 0;
            long selectedPartitionNum = 0;
            boolean firstPartition = true;
            boolean useRowCount = false;
            long estimatedRowSize = context.getSessionVariable().getPrunePartitionSimpleQueryAvgRowSize();

            // Batch size for fetching partition file info
            final int batchSize = context.getSessionVariable().getPrunePartitionSimpleQueryBatchSize();
            List<DescriptorTable.ReferencedPartitionInfo> tmpPartitionInfos = new ArrayList<>();
            boolean shouldBreak = false;

            for (int batchStart = 0; batchStart < partitionKeys.size() && !shouldBreak; batchStart += batchSize) {
                int batchEnd = Math.min(batchStart + batchSize, partitionKeys.size());
                List<PartitionKey> batchPartitionKeys = partitionKeys.subList(batchStart, batchEnd);

                try {
                    // Batch fetch file info for current batch of partitions
                    List<RemoteFileInfo> batchRemoteFileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .getRemoteFileInfos(catalogName, table, batchPartitionKeys);

                    // Iterate over each partition in current batch
                    for (int i = 0; i < batchPartitionKeys.size() && !shouldBreak; i++) {
                        if (i >= batchRemoteFileInfos.size()) {
                            continue;
                        }
                        RemoteFileInfo remoteFileInfo = batchRemoteFileInfos.get(i);
                        long partitionBytes = calculatePartitionBytes(remoteFileInfo);
                        if (partitionBytes <= 0) {
                            continue;
                        }

                        // Determine mode on first valid partition
                        if (firstPartition) {
                            long fileRowCount = tryGetRowCountFromFileName(remoteFileInfo);
                            if (fileRowCount > 0) {
                                useRowCount = true;
                            } else {
                                estimatedRowSize = Math.max(estimatedRowSize, getEstimatedRowSize(table));
                            }
                            firstPartition = false;
                        }

                        // Add partition to result (common for both modes)
                        partitions.add(remoteFileInfo);
                        tmpPartitionInfos.add(partitionInfos.get(batchStart + i));
                        totalSize += partitionBytes;
                        selectedPartitionNum++;

                        // Check if we have enough data
                        if (useRowCount) {
                            long fileRowCount = tryGetRowCountFromFileName(remoteFileInfo);
                            if (fileRowCount > 0) {
                                totalFileRowCount += fileRowCount;
                            }
                            if (totalFileRowCount >= simpleLimit) {
                                partitionInfos = tmpPartitionInfos;
                                maxScanSizeBytes = Long.MAX_VALUE;
                                maxScanRowCount = simpleLimit;
                                LOG.info("prune partition for simple query (row count mode) {}, limit {}, partitions {}, " +
                                                "file sizes {}, with file row count {}",
                                        context.getQueryId(), simpleLimit, selectedPartitionNum, totalSize, totalFileRowCount);
                                shouldBreak = true;
                            }
                        } else {
                            // File size mode: estimate based on avg row size
                            if (totalSize > estimatedRowSize * simpleLimit) {
                                partitionInfos = tmpPartitionInfos;
                                maxScanSizeBytes = estimatedRowSize * simpleLimit;
                                maxScanRowCount = Long.MAX_VALUE;
                                LOG.info("prune partition for simple query (file size mode) {}, limit {}, partitions {}, " +
                                                "file sizes {}, estimatedRowSize {}", context.getQueryId(), simpleLimit,
                                        selectedPartitionNum, totalSize, estimatedRowSize);
                                shouldBreak = true;
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Failed to get remote files", e);
                    throw e;
                }
            }
            if (tmpPartitionInfos.size() > 0) {
                partitionInfos = tmpPartitionInfos;
                descTbl.cleanReferencedPartitions(table);
                for (DescriptorTable.ReferencedPartitionInfo partitionInfo : tmpPartitionInfos) {
                    descTbl.addReferencedPartitions(table, partitionInfo);
                }
            }
        } else {
            context.setSimpleLimit(-1);
            try {
                partitions = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                        catalogName, table, partitionKeys);
            } catch (Exception e) {
                LOG.error("Failed to get remote files", e);
                throw e;
            }
        }
    }

    /**
     * Calculate total bytes of all files in a partition.
     * @param remoteFileInfo the partition file info
     * @return total bytes of all files with positive length, 0 if no valid files
     */
    long calculatePartitionBytes(RemoteFileInfo remoteFileInfo) {
        long partitionBytes = 0;
        for (RemoteFileDesc fileDesc : remoteFileInfo.getFiles()) {
            if (fileDesc.getLength() > 0) {
                partitionBytes += fileDesc.getLength();
            }
        }
        return partitionBytes;
    }

    long tryGetRowCountFromFileName(RemoteFileInfo remoteFileInfo) {
        long totalRowCount = -1;
        for (RemoteFileDesc fileDesc : remoteFileInfo.getFiles()) {
            long rowCount = getFileRowCount(fileDesc.getFileName());
            if (rowCount >= 0) {
                if (totalRowCount == -1) {
                    totalRowCount = 0;
                }
                totalRowCount += rowCount;
            }
        }
        return totalRowCount;
    }

    /**
     * Check if partition pruning should be performed for simple query.
     * Returns true when:
     * 1. ConnectContext exists
     * 2. SessionVariable exists
     * 3. SimpleLimit is greater than 0
     */
    boolean shouldPrunePartitionForSimpleQuery() {
        ConnectContext context = ConnectContext.get();
        return context != null && context.getSessionVariable() != null && context.getSimpleLimit() > 0;
    }

    long getFileRowCount(String fileName) {
        try {
            if (fileName.endsWith(".rcf")) {
                int index = fileName.lastIndexOf("_");
                String sub = fileName.substring(index + 1, fileName.length() - 4);
                return Long.parseLong(sub);
            } else if (fileName.endsWith(".orcf")) {
                int index = fileName.lastIndexOf("_");
                String sub = fileName.substring(index + 1, fileName.length() - 5);
                return Long.parseLong(sub);
            } else {
                return -1;
            }
        } catch (Throwable e) {
            return -1;
        }
    }

    /**
     * Estimate the row size in bytes based on the data columns of a HiveMetaStoreTable.
     */
    long getEstimatedRowSize(Table table) {
        if (!(table instanceof HiveMetaStoreTable)) {
            return -1;
        }
        try {
            HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;
            List<String> dataColumnNames = hiveMetaStoreTable.getDataColumnNames();
            return table.getColumns().stream()
                    .filter(column -> dataColumnNames.contains(column.getName()))
                    .mapToLong(column -> column.getType().getTypeSize())
                    .sum();
        } catch (Throwable e) {
            return -1;
        }
    }

    private void addScanRangeLocations(long partitionId, RemoteFileInfo partition, RemoteFileDesc fileDesc,
                                       Optional<RemoteFileBlockDesc> blockDesc, DataCacheOptions dataCacheOptions) {
        SessionVariable sv = SessionVariable.DEFAULT_SESSION_VARIABLE;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null) {
            sv = connectContext.getSessionVariable();
        }

        long totalSize = fileDesc.getLength();
        long offset = 0;
        if (blockDesc.isPresent()) {
            // If blockDesc existed, we will split according block desc
            RemoteFileBlockDesc block = blockDesc.get();
            totalSize = block.getLength();
            offset = block.getOffset();
        } else if (fileDesc instanceof DataCacheRemoteFileDesc) {
            DataCacheRemoteFileDesc desc = (DataCacheRemoteFileDesc) fileDesc;
            totalSize = desc.getLength();
            offset = desc.getOffset();
        }

        // assume we can not split at all.
        long splitSize = totalSize;
        if (fileDesc.isSplittable()) {
            // if splittable, then use max split size.
            splitSize = sv.getConnectorMaxSplitSize();
            if (canBackendSplitFile && sv.isEnableConnectorSplitIoTasks() && partition.getFormat().isBackendSplittable()) {
                // if BE can split, use a higher threshold.
                splitSize = sv.getConnectorHugeFileSize();
            }
        }
        boolean needSplit = (totalSize > splitSize);
        if (needSplit) {
            splitScanRangeLocations(partitionId, partition, fileDesc, blockDesc, offset, totalSize, splitSize,
                    dataCacheOptions);
        } else {
            createScanRangeLocationsForSplit(partitionId, partition, fileDesc, blockDesc, offset, totalSize,
                    dataCacheOptions);
        }
    }

    private void splitScanRangeLocations(long partitionId, RemoteFileInfo partition,
                                         RemoteFileDesc fileDesc,
                                         Optional<RemoteFileBlockDesc> blockDesc,
                                         long offset, long length, long splitSize, DataCacheOptions dataCacheOptions) {
        long remainingBytes = length;
        do {
            if (remainingBytes < 2 * splitSize) {
                createScanRangeLocationsForSplit(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes,
                        remainingBytes, dataCacheOptions);
                remainingBytes = 0;
            } else {
                createScanRangeLocationsForSplit(partitionId, partition, fileDesc,
                        blockDesc, offset + length - remainingBytes,
                        splitSize, dataCacheOptions);
                remainingBytes -= splitSize;
            }
        } while (remainingBytes > 0);
    }

    private void createScanRangeLocationsForSplit(long partitionId, RemoteFileInfo partition,
                                                  RemoteFileDesc fileDesc,
                                                  Optional<RemoteFileBlockDesc> blockDesc,
                                                  long offset, long length, DataCacheOptions dataCacheOptions) {
        // Check scan range count limit for thive tables
        if (isThiveTable && scanRangeLimit > 0 && scanRangeCount > scanRangeLimit) {
            String msg = "Exceeded the limit of " + scanRangeLimit + " max HDFS scan ranges for thive external table";
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                LOG.warn("{} queryId: {}", msg, DebugUtil.printId(connectContext.getQueryId()));
            }
            throw new SemanticException(msg);
        }

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(offset);
        hdfsScanRange.setLength(length);
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setModification_time(fileDesc.getModificationTime());
        hdfsScanRange.setFile_format(partition.getFormat().toThrift());
        if (isTextFormat(hdfsScanRange.getFile_format())) {
            hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        }

        if (dataCacheOptions != null) {
            TDataCacheOptions tDataCacheOptions = new TDataCacheOptions();
            tDataCacheOptions.setPriority(dataCacheOptions.getPriority());
            hdfsScanRange.setDatacache_options(tDataCacheOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        if (blockDesc.isPresent()) {
            if (blockDesc.get().getReplicaHostIds().length == 0) {
                String message = String.format("hdfs file corrupt, file block has no host, block Missing. file = %s/%s",
                        partition.getFullPath(), fileDesc.getFileName());
                throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
            }

            for (long hostId : blockDesc.get().getReplicaHostIds()) {
                String host = blockDesc.get().getDataNodeIp(hostId);
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(host, -1));
                scanRangeLocations.addToLocations(scanRangeLocation);
            }
        } else if (fileDesc instanceof DataCacheRemoteFileDesc) {
            DataCacheRemoteFileDesc cacheFile = (DataCacheRemoteFileDesc) fileDesc;
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackend_id(cacheFile.getBackendId());
            ComputeNode backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getBackendOrComputeNode(cacheFile.getBackendId());
            if (backend != null) {
                location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));
            } else {
                location.setServer(new TNetworkAddress("-1", -1));
                LOG.error("Backend {} referenced by cache delete file {} not found", cacheFile.getBackendId(),
                        cacheFile.getFullPath());
            }
            scanRangeLocations.addToLocations(location);
        } else {
            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
            ArrayList<TScanRangeLocation> locations = new ArrayList<>(1);
            locations.add(scanRangeLocation);
            scanRangeLocations.setLocations(locations);
        }

        result.add(scanRangeLocations);
        scanRangeCount++;
    }

    public static boolean isTextFormat(THdfsFileFormat format) {
        return format == THdfsFileFormat.TEXT || format == THdfsFileFormat.LZO_TEXT;
    }

    private void createHudiScanRangeLocations(long partitionId,
                                              RemoteFileInfo partition,
                                              HudiRemoteFileDesc fileDesc,
                                              boolean useJNIReader, DataCacheOptions dataCacheOptions) {

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path(fileDesc.getFileName());
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(fileDesc.getLength());
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setFile_format(partition.getFormat().toThrift());
        if (isTextFormat(hdfsScanRange.getFile_format())) {
            hdfsScanRange.setText_file_desc(fileDesc.getTextFileFormatDesc().toThrift());
        }
        for (String log : fileDesc.getHudiDeltaLogs()) {
            hdfsScanRange.addToHudi_logs(log);
        }
        hdfsScanRange.setUse_hudi_jni_reader(useJNIReader);
        if (dataCacheOptions != null) {
            TDataCacheOptions tDataCacheOptions = new TDataCacheOptions();
            tDataCacheOptions.setPriority(dataCacheOptions.getPriority());
            hdfsScanRange.setDatacache_options(tDataCacheOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        // TODO: get block info
        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        result.add(scanRangeLocations);
    }

    private Optional<List<DataCacheOptions>> generateDataCacheOptions(final QualifiedName qualifiedName,
                                                                      final List<String> partitionColumnNames,
                                                                      final List<PartitionKey> partitionKeys) {
        if (!ConnectContext.get().getSessionVariable().isEnableScanDataCache()) {
            return Optional.empty();
        }

        Optional<DataCacheRule> dataCacheRule = DataCacheMgr.getInstance().getCacheRule(qualifiedName);
        if (!dataCacheRule.isPresent()) {
            return Optional.empty();
        }

        List<DataCacheOptions> dataCacheOptions = new ArrayList<>(partitionKeys.size());
        Expr predicates = dataCacheRule.get().getPredicates();
        if (predicates == null) {
            for (int i = 0; i < partitionKeys.size(); i++) {
                dataCacheOptions.add(DataCacheOptions.DataCacheOptionsBuilder.builder()
                        .setPriority(dataCacheRule.get().getPriority()).build());
            }
        } else {
            // evaluate partition predicates
            for (PartitionKey partitionKey : partitionKeys) {
                // key is ColumnName, value is Expr(Literal)
                Map<String, Expr> mapping = new HashMap<>(partitionColumnNames.size());
                Preconditions.checkArgument(partitionColumnNames.size() == partitionKey.getKeys().size(),
                        "PartitionColumnName size must equal with PartitionKey keys' size.");
                for (int i = 0; i < partitionKey.getKeys().size(); i++) {
                    mapping.put(partitionColumnNames.get(i), partitionKey.getKeys().get(i));
                }
                // Must clone expr first, avoid change original expr
                Expr clonedExpr = predicates.clone();
                Expr rewritedExpr = DataCacheExprRewriter.rewrite(clonedExpr, mapping);
                ScalarOperator op = SqlToScalarOperatorTranslator.translate(rewritedExpr);
                ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
                op = scalarRewriter.rewrite(op, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                if (op.isConstantTrue()) {
                    // matched partition predicates
                    dataCacheOptions.add(DataCacheOptions.DataCacheOptionsBuilder.builder()
                            .setPriority(dataCacheRule.get().getPriority()).build());
                } else {
                    // not matched, add null DataCacheOption
                    dataCacheOptions.add(null);
                    if (!op.isConstantRef()) {
                        LOG.warn(String.format("ConstFolding failed for expr: %s, rewrite scalarOperator is %s",
                                rewritedExpr.toMySql(), op.debugString()));
                    }
                }
            }
        }
        return Optional.of(dataCacheOptions);
    }

    private void updateCanBackendSplitFile(List<RemoteFileInfo> partitions) {
        canBackendSplitFile = true;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return;
        }
        // if we let backend do split file work, how many splits we will get.
        int splits = getSplitsIfBackendSplitFile(partitions, connectContext);
        // if splits is small comparing to nodes, then better not let backend do split.
        int nodes = connectContext.getAliveComputeNumber() + connectContext.getAliveBackendNumber();
        if ((nodes * 2) >= splits) {
            canBackendSplitFile = false;
        }
    }

    private static int getSplitsIfBackendSplitFile(List<RemoteFileInfo> partitions, ConnectContext connectContext) {
        SessionVariable sv = connectContext.getSessionVariable();
        int splits = 0;
        long splitSize = sv.getConnectorHugeFileSize();
        for (int i = 0; i < partitions.size(); i++) {
            for (RemoteFileDesc fileDesc : partitions.get(i).getFiles()) {
                if (fileDesc.isSplittable()) {
                    splits += (fileDesc.getLength() + splitSize - 1) / splitSize;
                } else {
                    splits += 1;
                }
            }
        }
        return splits;
    }

    private void createScanRangeLocationsForStorageFormatSplit(long partitionId, RemoteFileInfo partition,
                                                               RemoteFileDesc fileDesc,
                                                               CombineFileSplit split,
                                                               DataCacheOptions dataCacheOptions) {
        // Check scan range count limit for thive tables
        if (isThiveTable && scanRangeLimit > 0 && scanRangeCount > scanRangeLimit) {
            String msg = "Exceeded the limit of " + scanRangeLimit + " max HDFS scan ranges for thive external table";
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                LOG.warn("{} queryId: {}", msg, DebugUtil.printId(connectContext.getQueryId()));
            }
            throw new SemanticException(msg);
        }

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path("");
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(split.getLength());
        hdfsScanRange.setPartition_id(partitionId);
        hdfsScanRange.setFile_length(split.getLength());
        hdfsScanRange.setModification_time(0);
        hdfsScanRange.setFile_format(partition.getFormat().toThrift());
        hdfsScanRange.setStorage_format_split_info(StorageFormatUtils.encodeSplitToString(split));

        if (dataCacheOptions != null) {
            TDataCacheOptions tDataCacheOptions = new TDataCacheOptions();
            tDataCacheOptions.setPriority(dataCacheOptions.getPriority());
            hdfsScanRange.setDatacache_options(tDataCacheOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        result.add(scanRangeLocations);
        scanRangeCount++;
    }

    public List<TScanRangeLocations> getScanRangeLocations(DescriptorTable descTbl, Table table,
                                                           HDFSScanNodePredicates scanNodePredicates) {
        result.clear();
        scanRangeCount = 0;

        // Initialize scan range limit tracking for thive tables
        ConnectContext connectContext = ConnectContext.get();
        isThiveTable = table instanceof HiveTable && ((HiveTable) table).isThiveTable();
        scanRangeLimit = connectContext.getSessionVariable().getScanHiveDatafileNumLimit();

        HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;

        long start = System.currentTimeMillis();
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        for (long partitionId : scanNodePredicates.getSelectedPartitionIds()) {
            PartitionKey partitionKey = scanNodePredicates.getIdToPartitionKey().get(partitionId);
            partitionKeys.add(partitionKey);
        }
        String catalogName = hiveMetaStoreTable.getCatalogName();
        QualifiedName qualifiedName = QualifiedName.of(ImmutableList.of(catalogName,
                hiveMetaStoreTable.getDbName(), hiveMetaStoreTable.getTableName()));
        Optional<List<DataCacheOptions>> dataCacheOptionsList = generateDataCacheOptions(qualifiedName,
                hiveMetaStoreTable.getPartitionColumnNames(), partitionKeys);

        updateCanBackendSplitFile(partitions);

        if (table instanceof HiveTable) {
            long sum = 0;
            long row = 0;
            partitionLoop:
            for (int i = 0; i < partitions.size(); i++) {
                DataCacheOptions dataCacheOptions = null;
                if (dataCacheOptionsList.isPresent()) {
                    dataCacheOptions = dataCacheOptionsList.get().get(i);
                }
                RemoteFileInfo remoteFileInfo = partitions.get(i);
                for (RemoteFileDesc fileDesc : remoteFileInfo.getFiles()) {
                    if (fileDesc.getLength() == 0) {
                        continue;
                    }
                    // Check if we have reached the simple limit threshold
                    if (sum > maxScanSizeBytes || row > maxScanRowCount) {
                        break partitionLoop;
                    }
                    sum += fileDesc.getLength();
                    if (maxScanRowCount != Long.MAX_VALUE) {
                        long rowCount = getFileRowCount(fileDesc.getFileName());
                        if (rowCount > 0) {
                            row += rowCount;
                        }
                    }
                    if (remoteFileInfo.getFormat().equals(RemoteFileInputFormat.FORMATFILE)) {
                        if (fileDesc instanceof StorageFormatRemoteFileDesc) {
                            StorageFormatRemoteFileDesc storageFormatFileDesc = (StorageFormatRemoteFileDesc) fileDesc;
                            for (CombineFileSplit split : storageFormatFileDesc.getStorageFormatSplitsInfo()) {
                                createScanRangeLocationsForStorageFormatSplit(partitionInfos.get(i).getId(),
                                        remoteFileInfo, fileDesc, split, dataCacheOptions);
                            }
                        } else {
                            createScanRangeLocationsForSplit(partitionInfos.get(i).getId(), partitions.get(i), fileDesc,
                                    Optional.empty(), 0, fileDesc.getLength(), dataCacheOptions);
                        }
                        continue;
                    }
                    if (forceScheduleLocal) {
                        if (fileDesc instanceof DataCacheRemoteFileDesc) {
                            addScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), fileDesc,
                                    Optional.empty(),
                                    dataCacheOptions);
                            LOG.debug("Add scan range success. partition: {}, file: {}, range: {}-{}",
                                    partitions.get(i).getFullPath(), fileDesc.getFileName(), 0, fileDesc.getLength());
                        } else {
                            for (RemoteFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                                addScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), fileDesc,
                                        Optional.of(blockDesc),
                                        dataCacheOptions);
                                LOG.debug("Add scan range success. partition: {}, file: {}, block: {}-{}",
                                        partitions.get(i).getFullPath(), fileDesc.getFileName(), blockDesc.getOffset(),
                                        blockDesc.getLength());
                            }
                        }
                    } else {
                        addScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), fileDesc,
                                Optional.empty(),
                                dataCacheOptions);
                        LOG.debug("Add scan range success. partition: {}, file: {}, range: {}-{}",
                                partitions.get(i).getFullPath(), fileDesc.getFileName(), 0, fileDesc.getLength());
                    }
                }
            }
        } else if (table instanceof HudiTable) {
            HudiTable hudiTable = (HudiTable) table;
            String tableInputFormat = hudiTable.getHudiInputFormat();
            boolean morTable = hudiTable.getTableType() == HoodieTableType.MERGE_ON_READ;
            boolean readOptimized = tableInputFormat.equals(HudiTable.MOR_RO_INPUT_FORMAT)
                    || tableInputFormat.equals(HudiTable.MOR_RO_INPUT_FORMAT_LEGACY);
            boolean snapshot = tableInputFormat.equals(HudiTable.MOR_RT_INPUT_FORMAT)
                    || tableInputFormat.equals(HudiTable.MOR_RT_INPUT_FORMAT_LEGACY);
            boolean forceJNIReader = ConnectContext.get().getSessionVariable().getHudiMORForceJNIReader();
            for (int i = 0; i < partitions.size(); i++) {
                DataCacheOptions dataCacheOptions = null;
                if (dataCacheOptionsList.isPresent()) {
                    dataCacheOptions = dataCacheOptionsList.get().get(i);
                }
                descTbl.addReferencedPartitions(table, partitionInfos.get(i));
                for (RemoteFileDesc fileDesc : partitions.get(i).getFiles()) {
                    HudiRemoteFileDesc hudiFiledesc = (HudiRemoteFileDesc) fileDesc;
                    if (fileDesc.getLength() == -1 && hudiFiledesc.getHudiDeltaLogs().isEmpty()) {
                        String message = "Error: get a empty hudi fileSlice";
                        throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
                    }
                    // ignore the scan range when read optimized mode and file slices contain logs only
                    if (morTable && readOptimized && fileDesc.getLength() == -1 && fileDesc.getFileName().isEmpty()) {
                        continue;
                    }
                    boolean useJNIReader =
                            forceJNIReader || (morTable && snapshot && !hudiFiledesc.getHudiDeltaLogs().isEmpty());
                    createHudiScanRangeLocations(partitionInfos.get(i).getId(), partitions.get(i), hudiFiledesc,
                            useJNIReader, dataCacheOptions);
                }
            }
        } else {
            String message = "Only Hive/Hudi table is supported.";
            throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
        }

        // Previously, the order of the scan range was from front to back, which would cause some probing sql to
        // encounter very bad cases (scan ranges that meet the predicate conditions are in the later partitions),
        // making BE have to scan more data to find rows that meet the conditions.
        // So shuffle scan ranges can naturally disrupt the scan ranges' order to avoid very bad cases.
        Collections.shuffle(result);

        LOG.debug("Get {} scan range locations cost: {} ms",
                getScanRangeLocationsSize(), (System.currentTimeMillis() - start));
        return result;
    }

    public int getScanRangeLocationsSize() {
        return result.size();
    }

    public long getFileNum() {
        return fileNum;
    }

    public long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public int getPartitionNum() {
        return partitionInfos.size();
    }

    public List<RemoteFileInfo> getRemoteFiles() {
        return Collections.unmodifiableList(partitions);
    }

    public List<DescriptorTable.ReferencedPartitionInfo> getPartitionInfos() {
        return Collections.unmodifiableList(partitionInfos);
    }
}
