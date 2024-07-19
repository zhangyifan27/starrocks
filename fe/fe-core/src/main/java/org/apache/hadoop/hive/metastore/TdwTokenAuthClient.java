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
package org.apache.hadoop.hive.metastore;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.common.Config;
import com.starrocks.mysql.security.TdwAuthenticate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utils.TAuthUtils;
import com.starrocks.utils.TdwUtil;
import com.tencent.tdw.security.exceptions.SecureException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TdwTokenAuthClient implements InvocationHandler {
    private static final Logger LOG = LogManager.getLogger(TdwTokenAuthClient.class);

    private final ThriftHiveMetastore.Iface iface;
    private final Configuration hiveConf;

    public TdwTokenAuthClient(Configuration hiveConf, ThriftHiveMetastore.Iface baseIface) throws MetaException {
        this.hiveConf = hiveConf;
        this.iface = baseIface;
    }

    public static ThriftHiveMetastore.Iface getProxy(Configuration hiveConf, ThriftHiveMetastore.Client client)
            throws MetaException {
        return getProxy(hiveConf, (ThriftHiveMetastore.Iface) client);
    }

    public static ThriftHiveMetastore.Iface getProxy(Configuration hiveConf, ThriftHiveMetastore.Iface baseIface)
            throws MetaException {
        TdwTokenAuthClient client = new TdwTokenAuthClient(hiveConf, baseIface);
        return (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
                TdwTokenAuthClient.class.getClassLoader(), new Class[] {ThriftHiveMetastore.Iface.class}, client);
    }

    private void preRun(Method method) throws SecureException, TException {
        String user = TdwUtil.getConnectionTdwUserName(ConnectContext.get());
        if (user == null || user.equalsIgnoreCase(AuthenticationMgr.ROOT_USER)) {
            // use starrocks root
            user = TAuthUtils.getDefaultTdwUser();
        }

        if (StringUtils.isEmpty(user)) {
            throw new SecureException("proxy user is null, please set TDW_PRI_USER_NAME");
        }

        LOG.debug("platformUser -> {} , proxy user -> {}", TAuthUtils.getTauthPlatformUser(), user);
        String token = TAuthUtils.getEncodedAuthentication(user, Config.tdw_metadata_service_target);
        LOG.debug("try setMetaConf. method -> {}, token -> {}", method, token);
        this.iface.setMetaConf("secure-authentication", token);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!"setMetaConf".equals(method.getName()) && !"set_ugi".equals(method.getName())) {
            preRun(method);
        }
        try {
            return method.invoke(iface, args);
        } catch (Throwable throwable) {
            String platformUser = TdwAuthenticate.getPlatformUser();
            if (StringUtils.isNotEmpty(platformUser) && !"setMetaConf".equals(method.getName()) &&
                    !"set_ugi".equals(method.getName())) {
                String token = TAuthUtils.getEncodedAuthentication(platformUser, Config.tdw_metadata_service_target);
                LOG.debug("platformUser -> {} , proxy user -> {}", TAuthUtils.getTauthPlatformUser(), platformUser);
                this.iface.setMetaConf("secure-authentication", token);
                return method.invoke(iface, args);
            } else {
                throw throwable;
            }
        }
    }
}
