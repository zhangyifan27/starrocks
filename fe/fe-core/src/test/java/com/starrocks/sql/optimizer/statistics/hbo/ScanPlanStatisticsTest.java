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

import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class ScanPlanStatisticsTest {
    private ScanPlanStatistics buildStats(List<Long> partitionIds) {
        PhysicalScanOperator scan = Mockito.mock(PhysicalScanOperator.class);
        // empty map so that buildPartitionColumnPredicatesAndOthers doesn't NPE.
        Mockito.when(scan.getColRefToColumnMetaMap()).thenReturn(Collections.emptyMap());
        return new ScanPlanStatistics(1, 10, 20, 1, 2, 3,
                scan, /*tableFilterSet*/ null, /*isPartitioned*/ true,
                Collections.emptyList(), partitionIds);
    }

    @Test
    public void testPartitionIdComparison() {
        ScanPlanStatistics s1 = buildStats(List.of(1L, 2L));
        ScanPlanStatistics s2 = buildStats(List.of(1L, 2L));
        ScanPlanStatistics s3 = buildStats(List.of(3L));

        Assertions.assertTrue(s1.hasSamePartitionId(s2));
        Assertions.assertFalse(s1.hasSamePartitionId(s3));
    }

    @Test
    public void testPredicateSetsEmptyEqual() {
        ScanPlanStatistics s1 = buildStats(List.of(1L));
        ScanPlanStatistics s2 = buildStats(List.of(1L));
        // No predicates -> should be equal for both checks
        Assertions.assertTrue(s1.hasSameOtherPredicates(s2));
        Assertions.assertTrue(s1.hasSamePartitionColumnPredicates(s2));
    }
}
