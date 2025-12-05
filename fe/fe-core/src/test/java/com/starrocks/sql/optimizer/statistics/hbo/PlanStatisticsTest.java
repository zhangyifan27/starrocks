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

package com.starrocks.sql.optimizer.statistics.hbo;

import com.starrocks.proto.NodeExecStatsItemPB;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PlanStatisticsTest {

    @Test
    public void testConstructorAndGetters() {
        PlanStatistics stats = new PlanStatistics(11, 1, 2, 3, 4, 5);
        Assertions.assertEquals(11, stats.nodeId);
        Assertions.assertEquals(1, stats.pushRows);
        Assertions.assertEquals(2, stats.pullRows);
        Assertions.assertEquals(3, stats.predFilterRows);
        Assertions.assertEquals(4, stats.indexFilterRows);
        Assertions.assertEquals(5, stats.rfFilterRows);
    }

    @Test
    public void testBuildFromPB() {
        NodeExecStatsItemPB pb = new NodeExecStatsItemPB();
        pb.setNodeId(7);
        pb.setPushRows(10L);
        pb.setPullRows(20L);
        pb.setPredFilterRows(1L);
        pb.setIndexFilterRows(2L);
        pb.setRfFilterRows(3L);
        PlanStatistics stats = PlanStatistics.buildFromStatsItem(pb, null, null);
        Assertions.assertEquals(7, stats.nodeId);
        Assertions.assertEquals(10, stats.pushRows);
        Assertions.assertEquals(20, stats.pullRows);
        Assertions.assertEquals(1, stats.predFilterRows);
        Assertions.assertEquals(2, stats.indexFilterRows);
        Assertions.assertEquals(3, stats.rfFilterRows);
    }
}
