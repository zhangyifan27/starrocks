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

import com.starrocks.common.Pair;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class RemoteScanRangeLocationsTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        connectContext.getSessionVariable().setConnectorMaxSplitSize(512 * 1024 * 1024);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setConnectorMaxSplitSize(64 * 1024 * 1024);
        connectContext.getSessionVariable().setForceScheduleLocal(false);
    }

    @Test
    public void testHiveSplit() throws Exception {
        String executeSql = "select * from hive0.file_split_db.file_split_tbl;";

        {
            connectContext.getSessionVariable().setEnableConnectorSplitIoTasks(true);
            connectContext.getSessionVariable().setConnectorHugeFileSize(1024 * 1024 * 1024);
            connectContext.getSessionVariable().setConnectorMaxSplitSize(64 * 1024 * 1024);
            // in this case, if we split in huge file size, we will get two splits
            // which is not suitable for backend split, then it will fall back to fe split.
            // 2 * 1G / 64MB = 32
            Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
            List<TScanRangeLocations> scanRangeLocations = pair.second.getFragments().get(1).collectScanNodes()
                    .get(new PlanNodeId(0)).getScanRangeLocations(100);
            Assert.assertEquals(32, scanRangeLocations.size());
        }
        {
            connectContext.getSessionVariable().setEnableConnectorSplitIoTasks(true);
            // in this case, if we split in huge file size, we will get 4 splits
            // which is suitable for backend split.
            // 2 * 1G / 512MB = 4
            connectContext.getSessionVariable().setConnectorHugeFileSize(512 * 1024 * 1024);
            connectContext.getSessionVariable().setConnectorMaxSplitSize(64 * 1024 * 1024);
            Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
            List<TScanRangeLocations> scanRangeLocations = pair.second.getFragments().get(1).collectScanNodes()
                    .get(new PlanNodeId(0)).getScanRangeLocations(100);
            Assert.assertEquals(4, scanRangeLocations.size());
        }
        {
            connectContext.getSessionVariable().setEnableConnectorSplitIoTasks(false);
            connectContext.getSessionVariable().setConnectorMaxSplitSize(512 * 1024 * 1024);
            Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
            List<TScanRangeLocations> scanRangeLocations = pair.second.getFragments().get(1).collectScanNodes()
                    .get(new PlanNodeId(0)).getScanRangeLocations(100);
            Assert.assertEquals(4, scanRangeLocations.size());
            scanRangeLocations.sort((o1, o2) -> {
                THdfsScanRange scanRange1 = o1.scan_range.hdfs_scan_range;
                THdfsScanRange scanRange2 = o2.scan_range.hdfs_scan_range;
                if (scanRange1.relative_path.equalsIgnoreCase(scanRange2.relative_path)) {
                    return (int) (scanRange1.offset - scanRange2.offset);
                } else {
                    return scanRange1.compareTo(scanRange2);
                }
            });

            TScanRange scanRange1 = scanRangeLocations.get(0).scan_range;
            TScanRange scanRange2 = scanRangeLocations.get(1).scan_range;

            Assert.assertEquals(scanRange1.hdfs_scan_range.length, scanRange2.hdfs_scan_range.offset);
        }
    }

    @Test
    public void testHiveSplitWithForceLocalSchedule() throws Exception {
        connectContext.getSessionVariable().setForceScheduleLocal(true);

        String executeSql = "select * from hive0.file_split_db.file_split_tbl;";
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        List<TScanRangeLocations> scanRangeLocations = pair.second.getFragments().get(1).collectScanNodes()
                .get(new PlanNodeId(0)).getScanRangeLocations(100);
        Assert.assertEquals(8, scanRangeLocations.size());

        scanRangeLocations.sort((o1, o2) -> {
            THdfsScanRange scanRange1 = o1.scan_range.hdfs_scan_range;
            THdfsScanRange scanRange2 = o2.scan_range.hdfs_scan_range;
            if (scanRange1.relative_path.equalsIgnoreCase(scanRange2.relative_path)) {
                return (int) (scanRange1.offset - scanRange2.offset);
            } else {
                return scanRange1.compareTo(scanRange2);
            }
        });

        Assert.assertEquals(0, scanRangeLocations.get(0).scan_range.hdfs_scan_range.offset);
        long previousOffset = scanRangeLocations.get(0).scan_range.hdfs_scan_range.length;
        for (int i = 1; i < 4; i++) {
            Assert.assertEquals(previousOffset, scanRangeLocations.get(i).scan_range.hdfs_scan_range.offset);
            previousOffset += scanRangeLocations.get(i).scan_range.hdfs_scan_range.length;
        }

        Assert.assertEquals(0, scanRangeLocations.get(4).scan_range.hdfs_scan_range.offset);
        previousOffset = scanRangeLocations.get(4).scan_range.hdfs_scan_range.length;
        for (int i = 5; i < 8; i++) {
            Assert.assertEquals(previousOffset, scanRangeLocations.get(i).scan_range.hdfs_scan_range.offset);
            previousOffset += scanRangeLocations.get(i).scan_range.hdfs_scan_range.length;
        }
    }

    // ==================== getFileRowCount method tests ====================

    /**
     * Test row count parsing for .rcf file format
     */
    @Test
    public void testGetFileRowCount_RcfFile() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        // Normal case: filename contains valid row count
        Assert.assertEquals(1000L, locations.getFileRowCount("part_00001_1000.rcf"));
        Assert.assertEquals(0L, locations.getFileRowCount("data_0.rcf"));
        Assert.assertEquals(999999L, locations.getFileRowCount("file_999999.rcf"));
        Assert.assertEquals(1L, locations.getFileRowCount("test_1.rcf"));
    }

    /**
     * Test row count parsing for .orcf file format
     */
    @Test
    public void testGetFileRowCount_OrcfFile() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        // Normal case: filename contains valid row count
        Assert.assertEquals(2000L, locations.getFileRowCount("part_00001_2000.orcf"));
        Assert.assertEquals(0L, locations.getFileRowCount("data_0.orcf"));
        Assert.assertEquals(123456L, locations.getFileRowCount("file_123456.orcf"));
    }

    /**
     * Test unsupported file formats return -1
     */
    @Test
    public void testGetFileRowCount_UnsupportedFormat() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        // Unsupported formats should return -1
        Assert.assertEquals(-1L, locations.getFileRowCount("file_1000.parquet"));
        Assert.assertEquals(-1L, locations.getFileRowCount("file_1000.orc"));
        Assert.assertEquals(-1L, locations.getFileRowCount("file_1000.csv"));
        Assert.assertEquals(-1L, locations.getFileRowCount("file_1000.txt"));
        Assert.assertEquals(-1L, locations.getFileRowCount("file_1000"));
    }

    /**
     * Test invalid filename formats return -1 (exception handling)
     */
    @Test
    public void testGetFileRowCount_InvalidFileName() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        // Cases where row count cannot be parsed should return -1
        Assert.assertEquals(-1L, locations.getFileRowCount("file_abc.rcf"));  // non-numeric
        Assert.assertEquals(-1L, locations.getFileRowCount("file.rcf"));       // no underscore
        Assert.assertEquals(-1L, locations.getFileRowCount(".rcf"));           // extension only
        Assert.assertEquals(-1L, locations.getFileRowCount("file_abc.orcf")); // non-numeric
        Assert.assertEquals(-1L, locations.getFileRowCount("file_.rcf"));     // empty number
    }

    /**
     * Test edge cases
     */
    @Test
    public void testGetFileRowCount_EdgeCases() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        // Large values
        Assert.assertEquals(Long.MAX_VALUE, locations.getFileRowCount("file_" + Long.MAX_VALUE + ".rcf"));

        // Multiple underscores, use the last one
        Assert.assertEquals(500L, locations.getFileRowCount("part_00001_data_500.rcf"));
        Assert.assertEquals(100L, locations.getFileRowCount("a_b_c_d_100.orcf"));
    }

    // ==================== tryGetRowCountFromFileName method tests ====================

    /**
     * Test empty file list returns -1
     */
    @Test
    public void testTryGetRowCountFromFileName_EmptyList() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        List<RemoteFileInfo> emptyList = new java.util.ArrayList<>();
        Assert.assertEquals(-1L, locations.tryGetRowCountFromFileName(emptyList));
    }

    /**
     * Test all files have valid row counts
     */
    @Test
    public void testTryGetRowCountFromFileName_AllValidFiles() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        // Create mock RemoteFileInfo and RemoteFileDesc
        List<RemoteFileInfo> fileInfos = new java.util.ArrayList<>();
        RemoteFileInfo info1 = createRemoteFileInfo("part_00001_100.rcf", "part_00002_200.rcf");
        RemoteFileInfo info2 = createRemoteFileInfo("part_00003_300.orcf");
        fileInfos.add(info1);
        fileInfos.add(info2);

        // 100 + 200 + 300 = 600
        Assert.assertEquals(600L, locations.tryGetRowCountFromFileName(fileInfos));
    }

    /**
     * Test partial files cannot parse row count
     */
    @Test
    public void testTryGetRowCountFromFileName_PartialValidFiles() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        List<RemoteFileInfo> fileInfos = new java.util.ArrayList<>();
        // Contains both parseable and non-parseable files
        RemoteFileInfo info = createRemoteFileInfo("part_00001_100.rcf", "invalid.parquet", "part_00002_200.rcf");
        fileInfos.add(info);

        // Only count parseable files: 100 + 200 = 300
        Assert.assertEquals(300L, locations.tryGetRowCountFromFileName(fileInfos));
    }

    /**
     * Test all files cannot parse row count
     */
    @Test
    public void testTryGetRowCountFromFileName_NoValidFiles() {
        RemoteScanRangeLocations locations = new RemoteScanRangeLocations();

        List<RemoteFileInfo> fileInfos = new java.util.ArrayList<>();
        RemoteFileInfo info = createRemoteFileInfo("file1.parquet", "file2.orc", "file3.csv");
        fileInfos.add(info);

        // All files cannot be parsed, return -1
        Assert.assertEquals(-1L, locations.tryGetRowCountFromFileName(fileInfos));
    }

    /**
     * Helper method: create RemoteFileInfo object
     */
    private RemoteFileInfo createRemoteFileInfo(String... fileNames) {
        List<RemoteFileDesc> fileDescs = new java.util.ArrayList<>();
        for (String fileName : fileNames) {
            RemoteFileDesc desc = new RemoteFileDesc(
                    fileName,           // fileName
                    "",                 // compression
                    1024L,              // length
                    0L,                 // modificationTime
                    null                // blockDescs (ImmutableList)
            );
            fileDescs.add(desc);
        }
        return new RemoteFileInfo(null, fileDescs, null);
    }
}
