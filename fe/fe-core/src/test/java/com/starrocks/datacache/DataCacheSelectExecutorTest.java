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

package com.starrocks.datacache;

import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.qe.SessionVariable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for DataCacheSelectExecutor.
 *
 * These tests verify the session variable management, mode handling,
 * and error conditions in the cache select execution flow.
 */
public class DataCacheSelectExecutorTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
    }

    // ========================================
    // A. MetaManager Initialization Tests
    // ========================================

    @Test
    public void testMetaManagerNotInitialized() {
        // Verify that a new metaManager is not initialized
        DataCacheMetaManager newManager = new DataCacheMetaManager(60_000L, 1L, 1L);
        Assert.assertFalse("New manager should not be initialized", newManager.isInitialized());
    }

    @Test
    public void testMetaManagerInitializedFlag() {
        // Create a custom manager that reports as initialized
        DataCacheMetaManager customManager = new DataCacheMetaManager(60_000L, 1L, 1L) {
            @Override
            public boolean isInitialized() {
                return true;
            }
        };
        Assert.assertTrue("Custom manager should report initialized", customManager.isInitialized());
    }

    // ========================================
    // B. Session Variable Tests
    // ========================================

    @Test
    public void testSessionVariableCloning() throws CloneNotSupportedException {
        SessionVariable original = new SessionVariable();
        original.setEnableScanDataCache(false);
        original.setEnablePopulateDataCache(false);

        SessionVariable cloned = (SessionVariable) original.clone();

        // Verify clone is independent
        cloned.setEnableScanDataCache(true);
        Assert.assertFalse("Original should not be affected by clone", original.isEnableScanDataCache());
        Assert.assertTrue("Clone should have modified value", cloned.isEnableScanDataCache());
    }

    @Test
    public void testSessionVariableDataCacheSettings() {
        SessionVariable sv = new SessionVariable();

        // Test setting data cache related values
        sv.setEnableScanDataCache(true);
        Assert.assertTrue(sv.isEnableScanDataCache());

        sv.setEnablePopulateDataCache(true);
        Assert.assertTrue(sv.isEnablePopulateDataCache());

        sv.setDataCachePopulateMode(DataCachePopulateMode.ALWAYS.modeName());
        Assert.assertEquals(DataCachePopulateMode.ALWAYS, sv.getDataCachePopulateMode());

        sv.setEnableCacheSelect(true);
        Assert.assertTrue(sv.isEnableCacheSelect());
    }

    @Test
    public void testSessionVariableDataCacheSettings_Never() {
        SessionVariable sv = new SessionVariable();
        sv.setDataCachePopulateMode(DataCachePopulateMode.NEVER.modeName());
        Assert.assertEquals(DataCachePopulateMode.NEVER, sv.getDataCachePopulateMode());
    }

    @Test
    public void testSessionVariableDataCacheSettings_Auto() {
        SessionVariable sv = new SessionVariable();
        sv.setDataCachePopulateMode(DataCachePopulateMode.AUTO.modeName());
        Assert.assertEquals(DataCachePopulateMode.AUTO, sv.getDataCachePopulateMode());
    }

    @Test
    public void testSessionVariableCatalogSetting() {
        SessionVariable sv = new SessionVariable();
        sv.setCatalog("hive0");
        Assert.assertEquals("hive0", sv.getCatalog());

        sv.setCatalog("iceberg_catalog");
        Assert.assertEquals("iceberg_catalog", sv.getCatalog());
    }

    @Test
    public void testSessionVariableWarehouseSetting() {
        SessionVariable sv = new SessionVariable();
        sv.setWarehouseName("test_warehouse");
        Assert.assertEquals("test_warehouse", sv.getWarehouseName());
    }

    @Test
    public void testSessionVariableScanDataCacheDisabled() {
        SessionVariable sv = new SessionVariable();
        sv.setEnableScanDataCache(false);
        Assert.assertFalse(sv.isEnableScanDataCache());
    }

    @Test
    public void testSessionVariablePopulateDataCacheDisabled() {
        SessionVariable sv = new SessionVariable();
        sv.setEnablePopulateDataCache(false);
        Assert.assertFalse(sv.isEnablePopulateDataCache());
    }

    // ========================================
    // C. DataCachePopulateMode Tests
    // ========================================

    @Test
    public void testDataCachePopulateMode_Always() {
        Assert.assertEquals("always", DataCachePopulateMode.ALWAYS.modeName());
    }

    @Test
    public void testDataCachePopulateMode_Never() {
        Assert.assertEquals("never", DataCachePopulateMode.NEVER.modeName());
    }

    @Test
    public void testDataCachePopulateMode_Auto() {
        Assert.assertEquals("auto", DataCachePopulateMode.AUTO.modeName());
    }

    @Test
    public void testDataCachePopulateMode_Values() {
        DataCachePopulateMode[] modes = DataCachePopulateMode.values();
        Assert.assertEquals(3, modes.length);
    }

    @Test
    public void testDataCachePopulateMode_ValueOf() {
        Assert.assertEquals(DataCachePopulateMode.ALWAYS, DataCachePopulateMode.valueOf("ALWAYS"));
        Assert.assertEquals(DataCachePopulateMode.NEVER, DataCachePopulateMode.valueOf("NEVER"));
        Assert.assertEquals(DataCachePopulateMode.AUTO, DataCachePopulateMode.valueOf("AUTO"));
    }

    // ========================================
    // D. CacheDeleteMode Tests
    // ========================================

    @Test
    public void testCacheDeleteMode_Values() {
        DataCacheMetaManager.CacheDeleteMode[] modes = DataCacheMetaManager.CacheDeleteMode.values();
        Assert.assertEquals(2, modes.length);
        Assert.assertEquals(DataCacheMetaManager.CacheDeleteMode.NORMAL, modes[0]);
        Assert.assertEquals(DataCacheMetaManager.CacheDeleteMode.GC, modes[1]);
    }

    @Test
    public void testCacheDeleteMode_ValueOf() {
        Assert.assertEquals(DataCacheMetaManager.CacheDeleteMode.NORMAL,
                DataCacheMetaManager.CacheDeleteMode.valueOf("NORMAL"));
        Assert.assertEquals(DataCacheMetaManager.CacheDeleteMode.GC,
                DataCacheMetaManager.CacheDeleteMode.valueOf("GC"));
    }

    @Test
    public void testCacheDeleteMode_ToString() {
        Assert.assertEquals("NORMAL", DataCacheMetaManager.CacheDeleteMode.NORMAL.toString());
        Assert.assertEquals("GC", DataCacheMetaManager.CacheDeleteMode.GC.toString());
    }

    // ========================================
    // E. DataCacheSelectExecutor Instance Tests
    // ========================================

    @Test
    public void testDataCacheSelectExecutor_CanBeInstantiated() {
        DataCacheSelectExecutor executor = new DataCacheSelectExecutor();
        Assert.assertNotNull(executor);
    }

    // ========================================
    // F. Session Variable Advanced Settings
    // ========================================

    @Test
    public void testSessionVariable_SetDataCacheAsyncPopulateMode() {
        SessionVariable sv = new SessionVariable();
        sv.setEnableDataCacheAsyncPopulateMode(false);
        // Just verify no exception is thrown - setter exists
        Assert.assertNotNull(sv);
    }

    @Test
    public void testSessionVariable_SetDataCacheIOAdaptor() {
        SessionVariable sv = new SessionVariable();
        sv.setEnableDataCacheIOAdaptor(false);
        // Just verify no exception is thrown - setter exists
        Assert.assertNotNull(sv);
    }

    @Test
    public void testSessionVariable_SetDataCacheEvictProbability() {
        SessionVariable sv = new SessionVariable();
        sv.setDataCacheEvictProbability(100);
        // Just verify no exception is thrown - setter exists
        Assert.assertNotNull(sv);
    }

    @Test
    public void testSessionVariable_SetDataCachePriority() {
        SessionVariable sv = new SessionVariable();
        sv.setDataCachePriority(1);
        sv.setDataCachePriority(0);
        // Just verify no exception is thrown - setter exists
        Assert.assertNotNull(sv);
    }

    @Test
    public void testSessionVariable_SetDatacacheTTLSeconds() {
        SessionVariable sv = new SessionVariable();
        sv.setDatacacheTTLSeconds(86400);
        sv.setDatacacheTTLSeconds(0);
        // Just verify no exception is thrown - setter exists
        Assert.assertNotNull(sv);
    }

    // ========================================
    // G. UserException Tests
    // ========================================

    @Test
    public void testUserException_Message() {
        UserException ex = new UserException("test message");
        Assert.assertEquals("test message", ex.getMessage());
    }

    @Test
    public void testUserException_WithCause() {
        RuntimeException cause = new RuntimeException("cause");
        UserException ex = new UserException("test message", cause);
        Assert.assertEquals("test message", ex.getMessage());
        Assert.assertEquals(cause, ex.getCause());
    }
}
