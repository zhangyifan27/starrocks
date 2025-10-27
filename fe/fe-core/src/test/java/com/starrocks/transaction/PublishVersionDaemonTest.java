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

package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

public class PublishVersionDaemonTest {
    public int oldValue;
    public int oldPublishCheckerCount;
    public boolean oldUseNewPublishChecker;

    @Before
    public void setUp() {
        oldValue = Config.lake_publish_version_max_threads;
        oldPublishCheckerCount = Config.publish_finish_task_max_threads;
        oldUseNewPublishChecker = Config.use_new_publish_checker;
    }

    @After
    public void tearDown() {
        Config.lake_publish_version_max_threads = oldValue;
        Config.publish_finish_task_max_threads = oldPublishCheckerCount;
        Config.use_new_publish_checker = oldUseNewPublishChecker;
    }

    @Test
    public void testUpdateLakeExecutorThreads()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        PublishVersionDaemon daemon = new PublishVersionDaemon();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");
        Assert.assertNotNull(executor);
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());

        ConfigRefreshDaemon configDaemon = GlobalStateMgr.getCurrentState().getConfigRefreshDaemon();

        // scale out
        Config.lake_publish_version_max_threads += 10;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());


        // scale in
        Config.lake_publish_version_max_threads -= 5;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());

        int oldNumber = executor.getMaximumPoolSize();

        // config set to < 0
        Config.lake_publish_version_max_threads = -1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(oldNumber, executor.getMaximumPoolSize());
        Assert.assertEquals(oldNumber, executor.getCorePoolSize());


        // config set to > LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        Config.lake_publish_version_max_threads = PublishVersionDaemon.LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE + 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(oldNumber, executor.getMaximumPoolSize());
        Assert.assertEquals(oldNumber, executor.getCorePoolSize());


        // config set to LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        Config.lake_publish_version_max_threads = PublishVersionDaemon.LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());

        // config set to 1
        Config.lake_publish_version_max_threads = 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());
    }

    @Test
    public void testInvalidInitConfiguration()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        int hardCodeDefaultMaxThreads = (int) FieldUtils.readDeclaredStaticField(PublishVersionDaemon.class,
                "LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE", true);

        // <= 0
        int initValue = 0;
        Config.lake_publish_version_max_threads = initValue;
        {
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");

            Assert.assertNotNull(executor);
            Assert.assertNotEquals(initValue, executor.getMaximumPoolSize());
            Assert.assertEquals(hardCodeDefaultMaxThreads, executor.getMaximumPoolSize());
            Assert.assertEquals(hardCodeDefaultMaxThreads, executor.getCorePoolSize());
            // configVar set to default value.
            Assert.assertEquals(hardCodeDefaultMaxThreads, Config.lake_publish_version_max_threads);
        }

        // > LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        initValue = PublishVersionDaemon.LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE + 1;
        Config.lake_publish_version_max_threads = initValue;
        {
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");
            Assert.assertNotNull(executor);
            Assert.assertNotEquals(initValue, executor.getMaximumPoolSize());
            Assert.assertEquals(hardCodeDefaultMaxThreads, executor.getMaximumPoolSize());
            Assert.assertEquals(hardCodeDefaultMaxThreads, executor.getCorePoolSize());
            // configVar set to default value.
            Assert.assertEquals(hardCodeDefaultMaxThreads, Config.lake_publish_version_max_threads);
        }
    }

    @Test
    public void testUpdateDeleteTxnLogExecutorThreads()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        PublishVersionDaemon daemon = new PublishVersionDaemon();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getDeleteTxnLogExecutor");
        Assert.assertNotNull(executor);
        ConfigRefreshDaemon configDaemon = GlobalStateMgr.getCurrentState().getConfigRefreshDaemon();

        // scale out
        Config.lake_publish_delete_txnlog_max_threads += 10;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_delete_txnlog_max_threads, executor.getMaximumPoolSize());

        // scale in
        Config.lake_publish_delete_txnlog_max_threads -= 5;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_delete_txnlog_max_threads, executor.getMaximumPoolSize());
    }

    @Test
    public void testInitPublishChecker() throws Exception {
        // 1. Use new publish checker
        Config.use_new_publish_checker = true;
        Config.publish_finish_task_max_threads = 5;
        PublishVersionDaemon daemon = new PublishVersionDaemon();
        List<PublishChecker> checkers = (List<PublishChecker>) getPrivateFieldValue(daemon, "publishCheckers");
        Assert.assertEquals(5, checkers.size());
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) getPrivateFieldValue(daemon, "finishTaskExecutor");
        Assert.assertNotNull(threadPoolExecutor);


        for (int i = 0; i < 9; i++) {
            TransactionState state = new TransactionState();
            state.setTransactionStatus(TransactionStatus.COMMITTED);
            state.getTableIdList().add((long) i);
            MethodUtils.invokeMethod(daemon, true, "addTxnToChecker", state);
        }

        Map<PublishChecker, Set<Long>> publishCheckerSetMap =
                (Map<PublishChecker, Set<Long>>) getPrivateFieldValue(daemon, "checkerToTablesMap");
        Assert.assertEquals(5, publishCheckerSetMap.size());
        for (int i = 0; i < checkers.size(); i++) {
            PublishChecker checker = checkers.get(i);
            if (i == checkers.size() - 1) {
                Assert.assertEquals(1, publishCheckerSetMap.get(checker).size());
            } else {
                Assert.assertEquals(2, publishCheckerSetMap.get(checker).size());
            }
        }

        daemon.stopPublishCheckers();
        Assert.assertEquals(0, threadPoolExecutor.getActiveCount());


        // 2. Use old publish checker
        Config.use_new_publish_checker = false;
        daemon = new PublishVersionDaemon();
        checkers = (List<PublishChecker>) getPrivateFieldValue(daemon, "publishCheckers");
        Assert.assertEquals(1, checkers.size());
        threadPoolExecutor = (ThreadPoolExecutor) getPrivateFieldValue(daemon, "finishTaskExecutor");
        Assert.assertNull(threadPoolExecutor);
    }

    public Object getPrivateFieldValue(Object obj, String fieldName) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }
}
