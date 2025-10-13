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

package com.starrocks.scheduler;

import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class TaskRunManagerTest {

    private static final int N = 100;
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority) {
        return makeExecuteOption(isMergeRedundant, isSync, priority, Maps.newHashMap());
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority,
                                                   Map<String, String> properties) {
        ExecuteOption executeOption = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(), isMergeRedundant,
                properties);
        executeOption.setSync(isSync);
        executeOption.setPriority(priority);
        return executeOption;
    }

    private Map<String, String> makeTaskRunProperties(String partitionStart,
                                                      String partitionEnd,
                                                      boolean isForce) {
        Map<String, String> result = Maps.newHashMap();
        result.put(TaskRun.PARTITION_START, partitionStart);
        result.put(TaskRun.PARTITION_END, partitionEnd);
        result.put(TaskRun.FORCE, String.valueOf(isForce));
        return result;
    }

    private Map<String, String> makeMVTaskRunProperties(String partitionStart,
                                                      String partitionEnd,
                                                      boolean isForce) {
        Map<String, String> result = Maps.newHashMap();
        result.put(TaskRun.PARTITION_START, partitionStart);
        result.put(TaskRun.PARTITION_END, partitionEnd);
        result.put(TaskRun.MV_ID, "1");
        result.put(TaskRun.FORCE, String.valueOf(isForce));
        return result;
    }

    @Test
    public void testExecutionOption() {
        {
            ExecuteOption option1 = makeExecuteOption(true, false, 1);
            ExecuteOption option2 = makeExecuteOption(true, false, 10);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            ExecuteOption option1 = makeExecuteOption(true, false, 1);
            ExecuteOption option2 = makeExecuteOption(false, false, 10);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeTaskRunProperties("2023-01-01", "2023-01-02", true);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", true);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            prop1.put("a", "a");
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop11 = option1.getTaskRunComparableProperties();
            Assert.assertTrue(prop11.size() == 4);

            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            prop2.put("a", "b");
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
    }
}
