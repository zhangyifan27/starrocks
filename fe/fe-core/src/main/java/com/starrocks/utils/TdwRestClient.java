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

package com.starrocks.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.tencent.tdw.security.exceptions.SecureException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by andrewcheng on 2022/9/16.
 */
public class TdwRestClient extends RestClient {
    private static final Logger LOG = LogManager.getLogger(TdwRestClient.class);
    private final ExecutorService executor =
            ThreadPoolManager.newDaemonCacheThreadPool(256, 256, "get-user-group-cache", false);
    private LoadingCache<String, String> userCache;
    private LoadingCache<String, Set<String>> userGroupCache;
    private static TdwRestClient instance;
    public static TdwRestClient getInstance() {
        if (instance == null) {
            instance = new TdwRestClient();
        }
        return instance;
    }

    public TdwRestClient() {
        super(Config.tdw_check_priv_timeout);
        init();
    }
    private void init() {
        userCache  = newCacheBuilder(Config.tdw_user_cache_count, Config.tdw_user_cache_ttl_s)
                .build(asyncReloading(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return loadPassword(key);
                    }
                }, executor));

        userGroupCache  = newCacheBuilder(Config.tdw_user_cache_count, Config.tdw_user_cache_ttl_s)
                .build(asyncReloading(new CacheLoader<String, Set<String>>() {
                    @Override
                    public Set<String> load(String key) throws Exception {
                        return loadUserGroup(key);
                    }
                }, executor));
    }

    private CacheBuilder<Object, Object> newCacheBuilder(long maximumSize, long expireTime) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.expireAfterWrite(expireTime, SECONDS);
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    public String getPassword(String userName) {
        try {
            return userCache.get(userName);
        } catch (Exception e) {
            LOG.error("Failed to get user password {}", userName, e);
            return null;
        }
    }

    private String loadPassword(String userName) throws IOException, SecureException, URISyntaxException {
        TdwResponse response;
        if (Config.tdw_ups_tauth_api_enabled) {
            // see: https://iwiki.woa.com/p/4009977259
            URIBuilder builder = new URIBuilder();
            builder.setPath(TAuthUtils.getTdwPrivilegeApiUrl() + "/tdwprivapi")
                    .setParameter("method", "mulcluster.gid.uid.passwd.get.with.auth")
                    .setParameter("messageFormat", "json")
                    .setParameter("v", "1.0")
                    .setParameter("appKey", "00001")
                    .setParameter("appgroupName", "xxx")
                    .setParameter("userName", userName)
                    .setParameter("token",
                            TAuthUtils.getEncodedAuthentication(null, Config.tdw_privapi_service_target));
            response = getTdwResponse(builder.build().toString());
        } else {
            JSONArray params = new JSONArray();
            params.put(userName);
            params.put("xxx");
            URIBuilder builder = new URIBuilder();
            builder.setPath(TAuthUtils.getTdwPrivilegeApiUrl() + "/AppGroupInternalService")
                    .setParameter("m", "mulClusterAppUserPasswdGet")
                    .setParameter("p", params.toString());
            response = getDsfResponse(builder.build().toString());
        }

        if (response.getCode() == 100002) {
            LOG.warn("User " + userName + " not found in tdw, return default password.");
            return "";
        }

        if (response.getCode() != 0) {
            throw new IOException("Failed to get user password, due to code: "
                    + response.getCode() + ", message: " + response.getMessage()
                    + ", solution: " + response.getSolution());
        }
        return response.getMessage();
    }

    public Set<String> getUserGroup(String userName) {
        try {
            return userGroupCache.get(userName);
        } catch (Exception e) {
            if (e instanceof ExecutionException && e.getCause() instanceof SocketTimeoutException) {
                LOG.error("Get user group of {} timeout, please increase `tdw_check_priv_timeout`", userName, e);
            } else {
                LOG.error("Failed to get user group of {}", userName, e);
            }

            return null;
        }
    }

    private Set<String> loadUserGroup(String userName) throws IOException, URISyntaxException {
        JSONArray params = new JSONArray();
        params.put(userName);
        params.put((Object) null);
        URIBuilder builder = new URIBuilder();
        builder.setPath(TAuthUtils.getTdwPrivilegeApiUrl() + "/AppGroupInternalService")
                .setParameter("m", "queryAppGroupInfoByUserInfo")
                .setParameter("p", params.toString());
        TdwResponse response = getDsfResponse(builder.build().toString());

        if (response.getCode() != 0) {
            throw new IOException("Failed to get user group, due to code: "
                    + response.getCode() + ", message: " + response.getMessage()
                    + ", solution: " + response.getSolution());
        }
        return response.getAppGroups().stream().map(TdwAppGroup::getAppGroupName).collect(Collectors.toSet());
    }

    private TdwResponse getTdwResponse(String url) throws IOException {
        String content = httpGet(url);
        return JsonUtil.readValue(content, TdwResponse.class);
    }

    private TdwResponse getDsfResponse(String url) throws IOException {
        String content = httpGet(url);
        DsfResponse dsfResponse = JsonUtil.readValue(content, DsfResponse.class);
        if (dsfResponse.getRetCode() != 0) {
            throw new IOException("Failed to get tdw response, due to retCode: "
                    + dsfResponse.getRetCode() + ", retMsg: " + dsfResponse.getRetMsg());
        }
        return dsfResponse.getRetObj();
    }
}