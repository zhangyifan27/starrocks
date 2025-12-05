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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HboPlanInfoProvider maintains 3 kinds of cache for each queryId:
 * - scanToFilterCache:
 *   scan relation id <-> filter expr sets on the scan
 *   collected during rewriting stage
 * - idToPlanCache:
 *   real plan id(not nereids id) <-> physical plan
 *   collected after physical plan generation
 * - planToIdCache:
 *   physical plan <-> real plan id(not nereids id)
 *   collected the same time as idToPlanCache
 */
public class HboPlanInfoProvider {
    private volatile Cache<String, ConcurrentHashMap<Integer, PhysicalOperator>> idToPlanCache;
    private volatile Cache<String, ConcurrentHashMap<PhysicalOperator, Integer>> planToIdCache;
    private volatile Cache<String, ConcurrentHashMap<String, ScalarOperator>> scanToFilterCache;

    /**
     * Hbo plan info provider.
     */
    public HboPlanInfoProvider() {
        idToPlanCache = buildHboIdToPlanCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        planToIdCache = buildHboPlanToIdCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        scanToFilterCache = buildHboScanToFilterCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
    }

    private static Cache<String, ConcurrentHashMap<String, ScalarOperator>> buildHboScanToFilterCache(
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

    private static Cache<String, ConcurrentHashMap<Integer, PhysicalOperator>> buildHboIdToPlanCache(
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

    private static Cache<String, ConcurrentHashMap<PhysicalOperator, Integer>> buildHboPlanToIdCache(
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

    public ConcurrentHashMap<Integer, PhysicalOperator> getIdToPlanMap(String queryId) {
        return idToPlanCache.asMap().getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putIdToPlanMap(String queryId, ConcurrentHashMap<Integer, PhysicalOperator> idToPlanMap) {
        idToPlanCache.put(queryId, idToPlanMap);
    }

    public ConcurrentHashMap<PhysicalOperator, Integer> getPlanToIdMap(String queryId) {
        return planToIdCache.asMap().getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putPlanToIdMap(String queryId, ConcurrentHashMap<PhysicalOperator, Integer> idToPlanMap) {
        planToIdCache.put(queryId, idToPlanMap);
    }

    public ConcurrentHashMap<String, ScalarOperator> getScanToFilterMap(String queryId) {
        return scanToFilterCache.asMap().getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putScanToFilterMap(String queryId, ConcurrentHashMap<String, ScalarOperator> scanToFilterMap) {
        scanToFilterCache.put(queryId, scanToFilterMap);
    }

    /**
     * NOTE: used in Config.hbo_plan_info_cache_num.callbackClassString and
     * Config.expire_hbo_plan_info_cache_in_fe_second.callbackClassString,
     */
    public static class UpdateConfig extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            HboPlanInfoProvider.updateConfig();
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
        HboPlanInfoProvider planInfoProvider = hboManger.getHboPlanInfoProvider();
        if (planInfoProvider == null) {
            return;
        }

        Cache<String, ConcurrentHashMap<Integer, PhysicalOperator>> idToPlanCache = buildHboIdToPlanCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        Cache<String, ConcurrentHashMap<PhysicalOperator, Integer>> planToIdCache = buildHboPlanToIdCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        Cache<String, ConcurrentHashMap<String, ScalarOperator>> scanToFilterCache = buildHboScanToFilterCache(
                Config.hbo_plan_info_cache_num,
                Config.expire_hbo_plan_info_cache_in_fe_second
        );
        idToPlanCache.putAll(planInfoProvider.idToPlanCache.asMap());
        planInfoProvider.idToPlanCache = idToPlanCache;
        planToIdCache.putAll(planInfoProvider.planToIdCache.asMap());
        planInfoProvider.planToIdCache = planToIdCache;
        scanToFilterCache.putAll(planInfoProvider.scanToFilterCache.asMap());
        planInfoProvider.scanToFilterCache = scanToFilterCache;
    }
}
