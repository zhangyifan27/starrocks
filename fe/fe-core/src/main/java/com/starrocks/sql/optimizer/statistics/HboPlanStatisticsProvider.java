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

package com.starrocks.sql.optimizer.statistics;


import com.starrocks.planner.PlanNodeAndHash;
import com.starrocks.sql.optimizer.statistics.hbo.RecentRunsPlanStatistics;

import java.util.List;
import java.util.Map;

/**
 * HboPlanStatisticsProvider provides recent runs' plan stats. info as a cache.
 */
public interface HboPlanStatisticsProvider {
    Map<PlanNodeAndHash, RecentRunsPlanStatistics> getHboPlanStats(List<PlanNodeAndHash> nodeIds);

    RecentRunsPlanStatistics getHboPlanStats(PlanNodeAndHash planNodeAndHash);

    void putHboPlanStats(Map<PlanNodeAndHash, RecentRunsPlanStatistics> hashesAndStatistics);

    void updatePlanStats(PlanNodeAndHash hash, RecentRunsPlanStatistics planStatistics);
}
