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

package com.starrocks.privilege.ranger;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.common.Config;
import com.starrocks.privilege.AccessController;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.ranger.hive.HiveAccessType;
import com.starrocks.sql.ast.TDWUserIdentity;
import com.starrocks.sql.ast.UserIdentity;
import com.tencent.tdw.auth.RangerDorisAuthorizer;
import com.tencent.tdw.auth.doris.DorisPrivilege;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Set;

public class RangerTDWAccessController implements AccessController, AccessTypeConverter {
    private static final Logger LOG = LogManager.getLogger(RangerTDWAccessController.class);

    private static RangerDorisAuthorizer authorizer;

    public RangerTDWAccessController(String serviceType, String serviceName) {
        authorizer = new RangerDorisAuthorizer();
        RangerConfiguration configuration = RangerConfiguration.getInstance();
        configuration.addResourcesForServiceType(StringUtils.isEmpty(serviceType) ? "hive" : serviceType);
        if (StringUtils.isNotEmpty(serviceName)) {
            configuration.set("ranger.plugin.hive.service.name", serviceName);
        }
        authorizer.init("hive", "doris", configuration);
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) {

    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {

    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(currentUser, tableName, privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        hasPermission(currentUser, tableName, PrivilegeType.SELECT);
    }

    @Override
    public String convertToAccessType(PrivilegeType privilegeType) {
        HiveAccessType hiveAccessType;
        if (privilegeType == PrivilegeType.SELECT) {
            hiveAccessType = HiveAccessType.SELECT;
        } else {
            hiveAccessType = HiveAccessType.NONE;
        }

        return hiveAccessType.name().toLowerCase(Locale.ENGLISH);
    }

    public void hasPermission(UserIdentity currentUser, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        if (currentUser == null || (Config.tdw_authentication_skip_root
                && currentUser.getUser().equals(AuthenticationMgr.ROOT_USER))) {
            return;
        }
        DorisPrivilege privilege = new DorisPrivilege(tableName.getDb(), tableName.getTbl(),
                Lists.newArrayList(privilegeType.name().toLowerCase(Locale.ROOT)));
        UserGroupInformation ugi = UserGroupInformation.createUserForTesting(currentUser.getUser(),
                currentUser instanceof TDWUserIdentity ?
                        ((TDWUserIdentity) currentUser).getUserGroup().toArray(new String[0]) : new String[0]);
        try {
            ugi.doAs(
                    (PrivilegedExceptionAction<Boolean>) () -> {
                        authorizer.checkPrivileges(Lists.newArrayList(privilege));
                        return true;
                    }
            );
        } catch (Throwable e) {
            if (e instanceof UndeclaredThrowableException) {
                String deniedMessage = ((UndeclaredThrowableException) e).getUndeclaredThrowable().getMessage();
                LOG.warn(deniedMessage, e);
                if (deniedMessage.length() >= 512) {
                    throw new AccessDeniedException(deniedMessage.substring(0, 300) + " ... "
                            + deniedMessage.substring(deniedMessage.length() - 200));
                } else {
                    throw new AccessDeniedException(deniedMessage);
                }
            } else {
                String message = String.format("An exception was encountered while checking privileges, user: %s, table: %s",
                        currentUser.getUser(), tableName);
                LOG.warn(message, e);
                throw new AccessDeniedException(message + ", " + e.getMessage());
            }
        }
    }
}
