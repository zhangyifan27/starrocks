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

import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.ConnectContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Locale;

/**
 * Created by andrewcheng on 2022/9/16.
 */
public class TdwUtil {

    public static boolean doesUserExist(String userName) {
        return !Arrays.equals(getPassword(userName), MysqlPassword.EMPTY_PASSWORD);
    }

    public static boolean checkPassword(String userName, byte[] remotePasswd, byte[] randomString) {
        // check password
        if (randomString == null) {
            return Arrays.equals(remotePasswd, getPassword(userName));
        }
        byte[] saltPassword = MysqlPassword.getSaltFromPassword(getPassword(userName));
        if (saltPassword.length != remotePasswd.length) {
            return false;
        }

        if (remotePasswd.length == 0) {
            return true;
        }
        return MysqlPassword.checkScramble(remotePasswd, randomString, saltPassword);
    }

    private static byte[] getPassword(String userName) {
        userName = getTdwUserName(userName);
        return MysqlPassword.makeScrambledPassword(TdwRestClient.getInstance().getPassword(userName));
    }

    public static String getTdwUserName(String username) {
        if (StringUtils.isNotEmpty(username)) {
            return username.startsWith("tdw_") ?
                    username : String.format(Locale.ROOT, "tdw_%s", username);
        }
        return username;
    }

    public static String getUserName(String username) {
        if (StringUtils.isNotEmpty(username)) {
            return username.startsWith("tdw_") ?
                    username.substring(4) : username;
        }
        return username;
    }

    public static String getConnectionTdwUserName(ConnectContext context) {
        return context != null ? context.getQualifiedUser() : null;
    }

    public static String getTdwUserName() {
        String username = getTdwUserName(getConnectionTdwUserName(ConnectContext.get()));
        if (StringUtils.isNotEmpty(username)) {
            if (isRoot(username)) {
                username = TAuthUtils.getDefaultTdwUser();
            }
        } else {
            username = TAuthUtils.getDefaultTdwUser();
        }
        return username;
    }

    public static boolean isRoot(String username) {
        if (username.equalsIgnoreCase("root") || username.equalsIgnoreCase("tdw_root")) {
            return true;
        }
        return false;
    }
}
