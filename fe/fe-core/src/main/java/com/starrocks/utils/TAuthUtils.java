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

import com.tencent.tdw.security.authentication.v2.TauthClient;
import com.tencent.tdw.security.authentication.v2.TauthService;
import com.tencent.tdw.security.exceptions.SecureException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by andrewcheng on 2024/4/20.
 */
public class TAuthUtils {

    private static final Logger LOG = LogManager.getLogger(TAuthUtils.class);
    private static TauthClient CLIENT;
    private static TauthService SERVICE;
    private static UserGroupInformation PLATFORM_USER = null;
    private static final String TDW_PRI_USER_NAME = "TDW_PRI_USER_NAME";
    private static final String TDW_PRIVILEGE_API_URL = "TDW_PRIVILEGE_API_URL";
    private static final String TQ_PLATFORM_USER_NAME = "TQ_PLATFORM_USER_NAME";
    private static final String TQ_PLATFORM_USER_CMK = "TQ_PLATFORM_USER_CMK";
    private static final String TQ_TAUTH_CUSTOM_PRINCIPAL = "TQ_TAUTH_CUSTOM_PRINCIPAL";
    private static final String TQ_TAUTH_CUSTOM_KEY = "TQ_TAUTH_CUSTOM_KEY";
    private static final String TAUTH_AUTHENTICATION_SERVICE_KEY = "TAUTH_AUTHENTICATION_SERVICE_KEY";
    private static final String TAUTH_AUTHENTICATION_SERVICE_NAME = "TAUTH_AUTHENTICATION_SERVICE_NAME";

    private static synchronized TauthClient getTauthClient() throws SecureException {
        if (CLIENT == null) {
            String tqAuthPlatformUser = getTauthPlatformUser();
            String tqAuthPlatformCmk = getTauthPlatformCMK();
            if (StringUtils.isEmpty(tqAuthPlatformUser) || StringUtils.isEmpty(tqAuthPlatformCmk)) {
                String err = "you must set TQ_PLATFORM_USER_NAME & TQ_PLATFORM_USER_CMK";
                LOG.error("initTauthClient failed: " + err);
                throw new SecureException(err);
            }
            CLIENT = new TauthClient(tqAuthPlatformUser, tqAuthPlatformCmk);
        }
        return CLIENT;
    }

    public static synchronized TauthService getTauthService() {
        if (SERVICE == null) {
            String serviceName = getSystemValue(TAUTH_AUTHENTICATION_SERVICE_NAME);
            String serviceKey = getSystemValue(TAUTH_AUTHENTICATION_SERVICE_KEY);
            LOG.debug("serviceName = " + serviceName + ", serviceKey = " + serviceKey);
            if (StringUtils.isBlank(serviceName) || StringUtils.isBlank(serviceKey)) {
                LOG.warn("Disable authentication, because serviceName or serviceKey not set.");
                SERVICE = null;
            } else {
                SERVICE = new TauthService(serviceName, serviceKey);
            }
        }
        return SERVICE;
    }

    public static synchronized UserGroupInformation getPlatformUser() throws SecureException {
        if (PLATFORM_USER == null) {
            String tqAuthPlatformUser = getTauthPlatformUser();
            String tqAuthPlatformCmk = getTauthPlatformCMK();
            if (StringUtils.isEmpty(tqAuthPlatformUser) || StringUtils.isEmpty(tqAuthPlatformCmk)) {
                String err = "you must set TQ_PLATFORM_USER_NAME & TQ_PLATFORM_USER_CMK";
                LOG.error("initTauthClient failed: " + err);
                throw new SecureException(err);
            }
            PLATFORM_USER = UserGroupInformation.createUserByTAuthKey(TAuthUtils.getTauthPlatformUser(),
                    TAuthUtils.getTauthPlatformCMK());
        }
        return PLATFORM_USER;
    }

    public static String getTdwPrivilegeApiUrl() {
        return getSystemValue(TDW_PRIVILEGE_API_URL);
    }

    public static String getDefaultTdwUser() {
        return getSystemValue(TDW_PRI_USER_NAME);
    }

    public static String getTauthPlatformUser() {
        String user = getSystemValue(TQ_PLATFORM_USER_NAME);
        if (StringUtils.isEmpty(user)) {
            user = getSystemValue(TQ_TAUTH_CUSTOM_PRINCIPAL);
        }
        return user;
    }

    public static String getTauthPlatformCMK() {
        String user = getSystemValue(TQ_PLATFORM_USER_CMK);
        if (StringUtils.isEmpty(user)) {
            user = getSystemValue(TQ_TAUTH_CUSTOM_KEY);
        }
        return user;
    }

    public static String getSystemValue(String key) {
        String user = System.getProperty(key);
        if (StringUtils.isEmpty(user)) {
            user = System.getenv(key);
        }
        return user;
    }

    public static String getEncodedAuthentication(String proxyUser, String serviceTarget) throws SecureException {
        String encodedAuthentication;
        if (StringUtils.isEmpty(proxyUser)) {
            encodedAuthentication = getTauthClient().getAuthentication(serviceTarget);
        } else {
            encodedAuthentication = getTauthClient().getAuthentication(serviceTarget, proxyUser);
        }
        LOG.debug("initTAuthClient successfully. platformUser -> {}, serviceTarget -> {}",
                getTauthPlatformUser(), serviceTarget);
        return encodedAuthentication;
    }
}
