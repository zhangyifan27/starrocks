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

package com.starrocks.qe.feedback;

import com.starrocks.qe.feedback.NodeExecStats;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeExecStatsTest {

    @Test
    public void testBuilderAndGetters() {
        NodeExecStats stats = new NodeExecStats.Builder()
                .setNodeId(1)
                .setPushRows(10)
                .setPullRows(20)
                .setPredFilterRows(3)
                .setIndexFilterRows(4)
                .setRfFilterRows(2)
                .build();

        Assertions.assertEquals(1, stats.getNodeId());
        Assertions.assertEquals(10, stats.getPushRows());
        Assertions.assertEquals(20, stats.getPullRows());
        Assertions.assertEquals(3, stats.getPredFilterRows());
        Assertions.assertEquals(4, stats.getIndexFilterRows());
        Assertions.assertEquals(2, stats.getRfFilterRows());
    }

    @Test
    public void testBuildFromExisting() {
        NodeExecStats original = new NodeExecStats(5, 50, 60, 7, 8, 9);
        NodeExecStats copy = NodeExecStats.Builder.buildFrom(original).build();
        Assertions.assertEquals(original.getNodeId(), copy.getNodeId());
        Assertions.assertEquals(original.getPushRows(), copy.getPushRows());
        Assertions.assertEquals(original.getPullRows(), copy.getPullRows());
        Assertions.assertEquals(original.getPredFilterRows(), copy.getPredFilterRows());
        Assertions.assertEquals(original.getIndexFilterRows(), copy.getIndexFilterRows());
        Assertions.assertEquals(original.getRfFilterRows(), copy.getRfFilterRows());
    }
}
