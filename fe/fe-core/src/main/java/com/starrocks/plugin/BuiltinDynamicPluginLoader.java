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

package com.starrocks.plugin;

import com.starrocks.common.UserException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class BuiltinDynamicPluginLoader extends DynamicPluginLoader {
    private static final Logger LOG = LogManager.getLogger(BuiltinDynamicPluginLoader.class);
    public static final String CLUSTER_NAME = "cluster_name";
    protected String pluginName;
    protected String clusterName;

    BuiltinDynamicPluginLoader(String pluginDir, String pluginName, String source, String clusterName) {
        super(pluginDir, source, "");
        this.pluginName = pluginName;
        this.clusterName = clusterName;
    }

    public void install() throws UserException, IOException {
        boolean reload = true;
        Path targetPath = FileSystems.getDefault().getPath(this.pluginDir.toString(), new String[] { this.pluginName });
        if (Files.exists(targetPath, new java.nio.file.LinkOption[0])) {
            this.pluginInfo = PluginInfo.readFromProperties(targetPath, "");
            this.installPath = targetPath;
            if (needToReinstall(targetPath)) {
                LOG.info("Plugin {} has update, re-install it", this.pluginName);
                reload = true;
            } else {
                LOG.info("Plugin {} has installed on local", this.pluginName);
                reload = false;
            }
        }
        if (reload) {
            uninstall();
            this.pluginInfo = null;
            this.installPath = null;
            getPluginInfo();
            movePlugin();
        }
        this.plugin = dynamicLoadPlugin(true);
        pluginInstallValid();
        this.pluginContext.setPluginPath(this.installPath.toString());
        if (StringUtils.isNotBlank(clusterName)) {
            this.pluginInfo.getProperties().put(CLUSTER_NAME, clusterName);
        }
        this.pluginInfo.getProperties().put(MD5SUM_KEY, getSourceChecksum(this.installPath));
        this.plugin.init(this.pluginInfo, this.pluginContext);
        this.pluginInfo.setName("__builtin_" + this.pluginInfo.getName());

        this.source = "";
    }

    private boolean needToReinstall(Path installParth) {
        String sourceChecksum = getSourceChecksum(installParth);
        String expectedChecksum = "";
        try {
            InputStream in = PluginZip.getInputStreamFromUrl(this.source + ".md5");
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                expectedChecksum = br.readLine();
                if (in != null) {
                    in.close();
                }
            } catch (Throwable throwable) {
                if (in != null) {
                    try {
                        in.close();
                    } catch (Throwable throwable1) {
                        throwable.addSuppressed(throwable1);
                    }
                }
                throw throwable;
            }
        } catch (Throwable e) {
            LOG.debug("Failed to get expected checksum, {}", e.getMessage());
            return false;
        }
        return !sourceChecksum.equals(expectedChecksum);
    }

    private String getSourceChecksum(Path installParth) {
        try {
            BufferedReader br = Files.newBufferedReader(installParth.resolve(MD5SUM_KEY));
            try {
                String line;
                if ((line = br.readLine()) != null) {
                    String str = line;
                    if (br != null) {
                        br.close();
                    }
                    return str;
                }
                if (br != null) {
                    br.close();
                }
            } catch (Throwable throwable) {
                if (br != null) {
                    try {
                        br.close();
                    } catch (Throwable throwable1) {
                        throwable.addSuppressed(throwable1);
                    }
                }
                throw throwable;
            }
        } catch (Throwable e) {
            LOG.debug("Failed to get source checksum {}", e.getMessage());
        }
        return "";
    }
}
