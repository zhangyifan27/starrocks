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

package com.starrocks.broker.hdfs;

import com.tencent.tdw.security.exceptions.SecureException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.util.Locale;

public class TAuthUtils {
    private static Logger logger = Logger
            .getLogger(TAuthUtils.class.getName());

    private static final String TQ_PLATFORM_USER_NAME = "TQ_PLATFORM_USER_NAME";
    private static final String TQ_PLATFORM_USER_CMK = "TQ_PLATFORM_USER_CMK";
    private static final String TQ_TAUTH_CUSTOM_PRINCIPAL = "TQ_TAUTH_CUSTOM_PRINCIPAL";
    private static final String TQ_TAUTH_CUSTOM_KEY = "TQ_TAUTH_CUSTOM_KEY";

    private static UserGroupInformation PLATFORM_USER = null;

    private static synchronized UserGroupInformation getPlatformUser() throws SecureException {
        if (PLATFORM_USER == null) {
            String tqAuthPlatformUser = getTauthPlatformUser();
            String tqAuthPlatformCmk = getTauthPlatformCMK();
            if (StringUtils.isEmpty(tqAuthPlatformUser) || StringUtils.isEmpty(tqAuthPlatformCmk)) {
                String err = "you must set TQ_PLATFORM_USER_NAME & TQ_PLATFORM_USER_CMK";
                logger.error("initTauthClient failed: " + err);
                throw new SecureException(err);
            }
            PLATFORM_USER = UserGroupInformation.createUserByTAuthKey(tqAuthPlatformUser,
                    tqAuthPlatformCmk);
        }
        return PLATFORM_USER;
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

    public static UserGroupInformation getUserGroupInformation(String username) throws Exception {
        return UserGroupInformation.createProxyUser(getTdwUserName(username), getPlatformUser());
    }

    public static String getTdwUserName(String username) {
        if (StringUtils.isNotEmpty(username)) {
            return username.startsWith("tdw_") ?
                    username : String.format(Locale.ROOT, "tdw_%s", username);
        }
        return username;
    }
}
