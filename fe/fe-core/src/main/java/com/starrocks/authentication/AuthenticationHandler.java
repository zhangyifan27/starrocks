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

package com.starrocks.authentication;

import com.starrocks.mysql.security.TdwAuthenticate;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AuthenticationHandler {
    private static final Logger LOG = LogManager.getLogger(AuthenticationHandler.class);

    public static UserIdentity authenticate(ConnectContext context, String username, String remoteHost, byte[] authResponse)
            throws AccessDeniedException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        UserIdentity currentUser = null;

        if (TdwAuthenticate.useTAUTH(username)) {
            currentUser = TdwAuthenticate.tauthAuthenticate(globalStateMgr.getAuthenticationMgr(), username);
        } else {
            currentUser = globalStateMgr.getAuthenticationMgr().checkPassword(username, remoteHost, authResponse, null);
        }
        if (currentUser == null) {
            LOG.error("Get user null for {}", username);
            throw new AccessDeniedException("Get user null for " + username);
        }
        context.setCurrentUserIdentity(currentUser);
        if (!currentUser.isEphemeral()) {
            context.setCurrentRoleIds(currentUser);
        }
        context.setQualifiedUser(currentUser.getUser());
        return currentUser;
    }
}
