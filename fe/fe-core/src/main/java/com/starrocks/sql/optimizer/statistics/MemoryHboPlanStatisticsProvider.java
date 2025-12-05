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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.planner.PlanNodeAndHash;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HboPlanStatisticsProvider's in-memory implementation.
 */
public class MemoryHboPlanStatisticsProvider implements HboPlanStatisticsProvider {
    private static final Logger LOG = LogManager.getLogger(MemoryHboPlanStatisticsProvider.class);
    private volatile Cache<String, RecentRunsPlanStatistics> hboPlanStatsCache;

    public MemoryHboPlanStatisticsProvider() {
        hboPlanStatsCache = buildHboPlanStatsCaches(
                Config.hbo_plan_stats_cache_num,
                Config.expire_hbo_plan_stats_cache_in_fe_second
        );
    }

    @Override
    public RecentRunsPlanStatistics getHboPlanStats(PlanNodeAndHash planNodeAndHash) {
        if (planNodeAndHash.getHash().isPresent()) {
            return hboPlanStatsCache.asMap().getOrDefault(planNodeAndHash.getHash().get(),
                    RecentRunsPlanStatistics.empty());
        }
        return RecentRunsPlanStatistics.empty();
    }

    @Override
    public Map<PlanNodeAndHash, RecentRunsPlanStatistics> getHboPlanStats(List<PlanNodeAndHash> planNodeHashes) {
        return planNodeHashes.stream().collect(Collectors.toMap(
                planNodeAndHash -> planNodeAndHash,
                planNodeAndHash -> {
                    if (planNodeAndHash.getHash().isPresent()) {
                        return hboPlanStatsCache.asMap().getOrDefault(planNodeAndHash.getHash().get(),
                                RecentRunsPlanStatistics.empty());
                    }
                    return RecentRunsPlanStatistics.empty();
                }));
    }

    @Override
    public void putHboPlanStats(Map<PlanNodeAndHash, RecentRunsPlanStatistics> hashStatisticsMap) {
        hashStatisticsMap.forEach((planNodeAndHash, recentRunsPlanStatistics) -> {
            if (planNodeAndHash.getHash().isPresent()) {
                hboPlanStatsCache.put(planNodeAndHash.getHash().get(), recentRunsPlanStatistics);
            }
        });
    }

    @Override
    public void updatePlanStats(PlanNodeAndHash hash, RecentRunsPlanStatistics planStatistics) {
        hboPlanStatsCache.put(hash.getHash().get(), planStatistics);
    }

    private static Cache<String, RecentRunsPlanStatistics> buildHboPlanStatsCaches(
            int cacheNum, long expireAfterAccessSeconds) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                .softValues();
        if (cacheNum > 0) {
            cacheBuilder.maximumSize(cacheNum);
        }
        if (expireAfterAccessSeconds > 0) {
            cacheBuilder = cacheBuilder.expireAfterAccess(Duration.ofSeconds(expireAfterAccessSeconds));
        }

        return cacheBuilder.build();
    }

    /**
     * NOTE: used in Config.hbo_plan_stats_cache_num.callbackClassString and
     * Config.expire_hbo_plan_stats_cache_in_fe_second.callbackClassString,
     */
    public static class UpdateConfig extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            MemoryHboPlanStatisticsProvider.updateConfig();
        }
    }

    /**
     * Reference the above UpdateConfig comments.
     */
    public static synchronized void updateConfig() {
        HboPlanStatisticsManager hboManger = GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager();
        if (hboManger == null) {
            return;
        }
        HboPlanStatisticsProvider hboPlanStatsProvider = hboManger.getHboPlanStatisticsProvider();
        if (!(hboPlanStatsProvider instanceof MemoryHboPlanStatisticsProvider)) {
            return;
        }

        MemoryHboPlanStatisticsProvider inMemHboPlanStatsProvider =
                (MemoryHboPlanStatisticsProvider) hboPlanStatsProvider;

        Cache<String, RecentRunsPlanStatistics> hboPlanStatsCache = buildHboPlanStatsCaches(
                Config.hbo_plan_stats_cache_num,
                Config.expire_hbo_plan_stats_cache_in_fe_second
        );
        hboPlanStatsCache.putAll(inMemHboPlanStatsProvider.hboPlanStatsCache.asMap());
        inMemHboPlanStatsProvider.hboPlanStatsCache = hboPlanStatsCache;
    }
}
