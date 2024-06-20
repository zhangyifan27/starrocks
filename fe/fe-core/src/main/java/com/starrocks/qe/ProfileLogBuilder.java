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

package com.starrocks.qe;

import com.google.gson.Gson;
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.plugin.Plugin;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.plugin.ProfileEvent;
import com.starrocks.plugin.ProfilePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProfileLogBuilder extends Plugin implements ProfilePlugin {
    private static final Logger PROFILE_LOG = LogManager.getLogger("profiletwo");
    private static final Gson GSON = new Gson();

    private final PluginInfo pluginInfo;

    public ProfileLogBuilder() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "ProfileLogBuilder", PluginInfo.PluginType.PROFILE,
                "builtin profile logger", DigitalVersion.fromString("0.12.0"),
                DigitalVersion.fromString("1.8.31"), ProfileLogBuilder.class.getName(), null, null);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public boolean eventFilter(ProfileEvent.EventType type) {
        return true;
    }

    @Override
    public void exec(ProfileEvent event) {
        String jsonString = GSON.toJson(event);
        PROFILE_LOG.info(jsonString);
    }

}
