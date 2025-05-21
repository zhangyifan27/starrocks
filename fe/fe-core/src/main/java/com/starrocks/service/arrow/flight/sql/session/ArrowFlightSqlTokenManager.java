// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.service.arrow.flight.sql.session;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlTokenManager implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlTokenManager.class);

    private LoadingCache<String, ArrowFlightSqlTokenInfo> tokenCache;

    private ScheduledExecutorService cleanupExecutor;

    public ArrowFlightSqlTokenManager() {
        this.tokenCache =
                CacheBuilder.newBuilder()
                        .maximumSize(Config.arrow_token_cache_size)
                        .expireAfterWrite(Config.arrow_token_cache_expire, TimeUnit.MINUTES)
                        .removalListener((RemovalNotification<String, ArrowFlightSqlTokenInfo> notification) -> {
                            ArrowFlightSqlConnectContext context =
                                    ExecuteEnv.getInstance().getScheduler()
                                            .getArrowFlightSqlConnectContext(notification.getKey());
                            if (context != null) {
                                ExecuteEnv.getInstance().getScheduler().unregisterArrowFlightConnection(context);
                            }
                        })
                        .build(new CacheLoader<String, ArrowFlightSqlTokenInfo>() {
                            @Override
                            public ArrowFlightSqlTokenInfo load(String key) throws Exception {
                                return new ArrowFlightSqlTokenInfo();
                            }
                        });
        this.cleanupExecutor = ThreadPoolManager.newDaemonScheduledThreadPool(1,
                "Arrow-Flight-Sql-Token-Cleanup", true);
        this.cleanupExecutor.scheduleAtFixedRate(() -> {
            tokenCache.cleanUp();
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String createToken(UserIdentity currentUser) throws Exception {
        String token = UUIDUtil.genUUID().toString();
        ArrowFlightSqlTokenInfo arrowFlightSqlTokenInfo = new ArrowFlightSqlTokenInfo();
        arrowFlightSqlTokenInfo.setToken(token);
        arrowFlightSqlTokenInfo.setCurrentUser(currentUser);
        tokenCache.put(token, arrowFlightSqlTokenInfo);
        LOG.info("create token: {} for user: {}", token, currentUser);
        return token;
    }

    public ArrowFlightSqlTokenInfo validateToken(String token) {
        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("Invalid Token");
        }

        try {
            return tokenCache.getIfPresent(token);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Token");
        }
    }

    public void invalidateToken(String token) {
        tokenCache.invalidate(token);
    }

    @Override
    public void close() throws Exception {
        tokenCache.invalidateAll();
    }

}