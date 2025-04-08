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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class IcebergConnectorTest {
    private IcebergConnector connector;
    private ConnectorContext context;
    private Map<String, String> properties;

    @BeforeEach
    void setUp() throws Exception {
        context = Mockito.mock(ConnectorContext.class);
        properties = new HashMap<>();
        when(context.getCatalogName()).thenReturn("test_iceberg");
        when(context.getProperties()).thenReturn(properties);

        // 方式1：设置当前线程的ConnectContext
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQualifiedUser("test_user");  // 设置有效用户名
    }

    // 测试不同Catalog类型初始化
    @ParameterizedTest
    @ValueSource(strings = {"hive", "glue", "hadoop"})
    void testCatalogTypeInitialization(String catalogType) {
        properties.put("iceberg.catalog.type", catalogType);
        // 设置必要参数
        switch (catalogType) {
            case "hive":
                properties.put("hive.metastore.uris", "thrift://localhost:9083");
                break;
            case "glue":
                properties.put("aws.glue.region", "us-west-2");
                break;
            case "rest":
                properties.put("uri", "http://iceberg-rest:8181");
                break;
            case "hadoop":
                properties.put("warehouse", "hdfs://localhost:9000/warehouse");
                break;
        }

        connector = new IcebergConnector(context);
        assertNotNull(connector.getMetadata());
    }

    @Test
    void testHiveCatalogRequiredParameters() {
        properties.put("type", "hive");
        // 不提供hive.metastore.uris应该抛出异常
        assertThrows(StarRocksConnectorException.class, () -> new IcebergConnector(context));
    }

    @Test
    void testThreadPoolInitialization() {
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://localhost:9083");
        properties.put("iceberg.catalog-impl", "org.apache.iceberg.hive.HiveCatalog");
        connector = new IcebergConnector(context);

        // 验证线程池初始化

        ExecutorService planningPool = connector.buildIcebergJobPlanningExecutor();
        assertNotNull(planningPool);
        assertFalse(planningPool.isShutdown());

        ExecutorService refreshPool = connector.buildRefreshOtherFeExecutor();
        assertNotNull(refreshPool);
        assertFalse(refreshPool.isShutdown());
    }

    @Test
    void testCacheConfigurationDisabled() {
        properties.put("type", "hive");
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://localhost:9083");
        properties.put("enable_iceberg_metadata_cache", "false");
        properties.put("iceberg.catalog-impl", "org.apache.iceberg.hive.HiveCatalog");

        connector = new IcebergConnector(context);
        IcebergCatalog catalog = connector.getNativeCatalog();
        assertFalse(catalog instanceof CachingIcebergCatalog);
    }

    @Test
    void testShutdownWithResources() {
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://localhost:9083");
        properties.put("iceberg.catalog-impl", "org.apache.iceberg.hive.HiveCatalog");
        connector = new IcebergConnector(context);

        // 初始化线程池
        connector.buildIcebergJobPlanningExecutor();
        connector.buildRefreshOtherFeExecutor();

        assertDoesNotThrow(() -> connector.shutdown());
        assertTrue(connector.buildIcebergJobPlanningExecutor().isShutdown());
        assertTrue(connector.buildRefreshOtherFeExecutor().isShutdown());
    }

    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.shutdown();
        }
    }
}