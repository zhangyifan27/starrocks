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

package com.starrocks.mysql.security;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.UserDesc;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.LeaderOpExecutor;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.TDWUserIdentity;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utils.TAuthUtils;
import com.starrocks.utils.TdwUtil;
import com.tencent.tdw.security.authentication.Authenticator;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by andrewcheng on 2022/8/8.
 */
public class TdwAuthenticate {
    private static final Logger LOG = LogManager.getLogger(TdwAuthenticate.class);

    public static UserIdentity tauthAuthenticate(AuthenticationMgr authenticationManager, String encodedAuthentication)
            throws AccessDeniedException {
        if (TAuthUtils.getTauthService() == null) {
            LOG.warn("tauth service is null");
            return null;
        }
        // check user password by TAUTH server.
        Authenticator authenticator;
        try {
            if (StringUtils.isEmpty(encodedAuthentication)) {
                LOG.warn("encodedAuthentication is empty");
                return null;
            }
            authenticator = authenticateAndGetUser(encodedAuthentication);
        } catch (Exception e) {
            LOG.warn("encodedAuthentication: " + encodedAuthentication);
            throw new AccessDeniedException("failed to check tauth authentication, " + e.getMessage());
        }
        UserIdentity authUser = createUserIfNotExist(authenticationManager, TdwUtil.getUserName(authenticator.getUser()));
        return authUser == null ? null : new TDWUserIdentity(authUser, authenticator.getRealUser());
    }

    private static Authenticator authenticateAndGetUser(String encodedAuthentication) throws Exception {
        LOG.debug("encodedAuthentication: " + encodedAuthentication);
        Authenticator authenticator = TAuthUtils.getTauthService().authenticate(encodedAuthentication);
        LOG.debug("authenticator: " + authenticator);
        return authenticator;
    }

    public static UserIdentity authenticate(AuthenticationMgr authenticationManager, String user, byte[] remotePasswd,
                                            byte[] randomString) throws AccessDeniedException {
        user = TdwUtil.getUserName(user);
        // check user password by TDW server.
        try {
            if (!TdwUtil.checkPassword(user, remotePasswd, randomString)) {
                LOG.debug("user:{} use error TDW password", user);
                return null;
            }
        } catch (Exception e) {
            LOG.error("failed to check tdw password", e);
            throw new AccessDeniedException("failed to check tdw password, " + e.getMessage());
        }

        UserIdentity authUser =  createUserIfNotExist(authenticationManager, user);
        return authUser == null ? null : new TDWUserIdentity(authUser);
    }

    private static UserIdentity createUserIfNotExist(AuthenticationMgr authenticationManager, String user) {
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
        // Search the user in starrocks.
        if (!authenticationManager.doesUserExist(userIdentity)) {
            LOG.debug("User:{} does not exists in starrocks, create by internal.", user);
            if (!createUser(authenticationManager, userIdentity)) {
                LOG.error("Failed to create user internally.");
                return null;
            }
        }
        return userIdentity;
    }

    public static boolean useTAUTH(String user) {
        return user.startsWith("tauth");
    }

    public static boolean useTdwAuthenticate(String user) {
        // The root and admin cannot use tdw authentication.
        if (user.equals(AuthenticationMgr.ROOT_USER)) {
            return false;
        }
        // If Tdw authentication is enabled and the user exists in TDW, use TDW authentication,
        // otherwise use default authentication.
        return Config.enable_tdw_authentication && TdwUtil.doesUserExist(TdwUtil.getUserName(user));
    }

    private static boolean createUser(AuthenticationMgr authenticationManager, UserIdentity userIdentity) {
        String userName = ClusterNamespace.getNameFromFullName(userIdentity.getUser());
        // forward to master if necessary
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            String showProcStmt = "CREATE USER \"" + userName + "\"";
            // ConnectContext build in RestBaseAction
            ConnectContext context = ConnectContext.get();
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setQueryId(UUIDUtil.genUUID());
            context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            LeaderOpExecutor leaderOpExecutor = new LeaderOpExecutor(new OriginStatement(showProcStmt, 0), context,
                    RedirectStatus.FORWARD_NO_SYNC);
            LOG.debug("need to transfer to Leader. stmt: {}", context.getStmtId());

            try {
                leaderOpExecutor.execute();
            } catch (Exception e) {
                LOG.warn("failed to forward stmt", e);
                return false;
            }
        } else {
            try {
                CreateUserStmt stmt = new CreateUserStmt(true, new UserDesc(userIdentity), Lists.newArrayList(),
                        Maps.newTreeMap());
                String authPluginUsing = authenticationManager.getDefaultPlugin();
                AuthenticationProvider provider = AuthenticationProviderFactory.create(authPluginUsing);
                UserAuthenticationInfo info =
                        provider.validAuthenticationInfo(userIdentity, "", stmt.getAuthStringUnResolved());
                info.setAuthPlugin(authPluginUsing);
                info.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());
                stmt.setAuthenticationInfo(info);
                authenticationManager.createUser(stmt);
            } catch (DdlException | AuthenticationException e) {
                LOG.error("failed to create user " + userIdentity.getUser());
                return false;
            }
        }
        return true;
    }

    public static boolean match(String whitelists, String user) {
        String[] tdwTauthPlatformWhiteList = whitelists.split(",");
        for (String candidate : tdwTauthPlatformWhiteList) {
            if (candidate.trim().equalsIgnoreCase(user)) {
                return true;
            }
        }
        return false;
    }

    public static void checkRequestSource(UserIdentity currentUser) {
        if (Config.enable_supersql_proxy_authentication
                && !currentUser.getUser().equalsIgnoreCase(AuthenticationMgr.ROOT_USER)) {
            boolean isAccept;
            if (currentUser instanceof TDWUserIdentity) {
                isAccept = match(Config.tdw_supersql_platform_name,
                        ((TDWUserIdentity) currentUser).getRealUser());
            } else {
                isAccept = false;
            }

            if (!isAccept) {
                throw new StarRocksConnectorException("Access denied; only accept requests from SuperSQL to use tdw catalog");
            }
        }
    }
}