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

import com.starrocks.catalog.HiveTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_HIVE_TABLE;

public class HiveWriteUtilsTest {
    static {
        System.setProperty("TQ_PLATFORM_USER_NAME", "olap_metadata");
        System.setProperty("TQ_PLATFORM_USER_CMK", "xxx");
        System.setProperty("TDW_PRI_USER_NAME", "tdwadmin");
    }

    @Test
    public void testIsS3Url() {
        Assert.assertTrue(HiveWriteUtils.isS3Url("obs://"));
    }

    @Test
    public void checkLocationProp() {
        Map<String, String> conf = new HashMap<>();
        conf.put("external_location", "xxx");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Can't create non-managed Hive table. Only supports creating hive table under Database location. " +
                        "You could execute command without external_location properties",
                () -> HiveWriteUtils.checkLocationProperties(conf));
    }

    @Test
    public void testPathExists() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to check path",
                () -> HiveWriteUtils.pathExists(path, new Configuration()));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        Assert.assertFalse(HiveWriteUtils.pathExists(path, new Configuration()));
    }

    @Test
    public void testIsDirectory() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed checking path",
                () -> HiveWriteUtils.isDirectory(path, new Configuration()));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        Assert.assertFalse(HiveWriteUtils.isDirectory(path, new Configuration()));
    }

    @Test
    public void testCreateDirectory() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        new MockUp<HiveWriteUtils>() {
            @Mock
            public FileSystem getTAuthFileSystem(Path path, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to create directory",
                () -> HiveWriteUtils.createDirectory(path, new Configuration()));
    }

    @Test
    public void testFileCreateByQuery() {
        Assert.assertFalse(HiveWriteUtils.fileCreatedByQuery("000000_0", "aaaa-bbbb"));
    }

    @Test
    public void testGetStagingDirForHdfsUrl() {
        HiveTable table = new HiveTable.Builder()
                .setTableName("test_table")
                .setTableLocation("hdfs://ss-teg-7-v3/user/hive/warehouse/test_db.db/test_table")
                .build();
        String tempStagingDir = "/tmp/starrocks";
        String stagingDir = HiveWriteUtils.getStagingDir(table, tempStagingDir);

        // Verify the staging dir format: hdfs://[host]/[tempStagingDir]/[yearMonthDay]/[databaseName]/[tableName]/[UUID]/
        Assert.assertTrue(stagingDir.startsWith("hdfs://ss-teg-7-v3/tmp/starrocks/"));
        Assert.assertTrue(stagingDir.contains("/test_db.db/"));
        Assert.assertTrue(stagingDir.contains("/test_table/"));
        Assert.assertTrue(stagingDir.endsWith("/"));

        // Verify date format (YYYYMMDD)
        String datePattern = "\\d{8}";
        Assert.assertTrue(stagingDir.matches(".*/" + datePattern + "/.*"));

        // Verify UUID format (8-4-4-4-12)
        String uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
        Assert.assertTrue(stagingDir.matches(".*/" + uuidPattern + "/"));
    }

    @Test
    public void testGetStagingDirForHdfsUrlWithoutLeadingSlash() {
        HiveTable table = new HiveTable.Builder()
                .setTableName("test_table")
                .setTableLocation("hdfs://host/user/hive/warehouse/db_name.db/table_name")
                .build();
        String tempStagingDir = "tmp/starrocks";  // without leading slash
        String stagingDir = HiveWriteUtils.getStagingDir(table, tempStagingDir);

        // Verify the staging dir format
        Assert.assertTrue(stagingDir.startsWith("hdfs://host/tmp/starrocks/"));
        Assert.assertTrue(stagingDir.contains("/db_name.db/"));
        Assert.assertTrue(stagingDir.contains("/table_name/"));
        Assert.assertTrue(stagingDir.endsWith("/"));
    }
}
