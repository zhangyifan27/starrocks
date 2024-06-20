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

import com.google.common.collect.Queues;
import com.starrocks.plugin.Plugin;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.plugin.ProfileEvent;
import com.starrocks.plugin.ProfilePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProfileEventProcessor {
    private static final Logger LOG = LogManager.getLogger(ProfileEventProcessor.class);
    private static final long UPDATE_PLUGIN_INTERVAL_MS = 60L * 1000L; // 1min

    private PluginMgr pluginMgr;
    private List<Plugin> profilePlugins;
    private long lastUpdateTime = 0;
    private BlockingQueue<ProfileEvent> eventQueue = Queues.newLinkedBlockingDeque(10000);
    private Thread workerThread;

    private volatile boolean isStopped = false;

    public ProfileEventProcessor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    public void start() {
        workerThread = new Thread(new Worker(), "ProfileEventProcessor");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public void stop() {
        isStopped = true;
        if (workerThread != null) {
            try {
                workerThread.join();
            } catch (InterruptedException e) {
                LOG.warn("join worker join failed.", e);
            }
        }
    }

    public void handleProfileElement(ProfileEvent profileEvent) {
        try {
            eventQueue.put(profileEvent);
        } catch (InterruptedException e) {
            LOG.debug("encounter exception when handle profile event, ignore", e);
        }
    }

    public class Worker implements Runnable {

        @Override
        public void run() {
            ProfileEvent profileEvent;
            while (!isStopped) {
                // update profile plugin list every UPDATE_PLUGIN_INTERVAL_MS.
                // because some of plugins may be installed or uninstalled at runtime.
                if (profilePlugins == null || System.currentTimeMillis() - lastUpdateTime > UPDATE_PLUGIN_INTERVAL_MS) {
                    profilePlugins = pluginMgr.getActivePluginList(PluginInfo.PluginType.PROFILE);
                    lastUpdateTime = System.currentTimeMillis();
                    LOG.debug("update profile plugins. num: {}", profilePlugins.size());
                }

                try {
                    profileEvent = eventQueue.poll(5, TimeUnit.SECONDS);
                    if (profileEvent == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.debug("encounter exception when getting profile event from queue, ignore", e);
                    continue;
                }

                for (Plugin plugin : profilePlugins) {
                    try {
                        if (((ProfilePlugin) plugin).eventFilter(profileEvent.getEventType())) {
                            ((ProfilePlugin) plugin).exec(profileEvent);
                        }
                    } catch (Exception e) {
                        LOG.debug("encounter exception when processing profile event.", e);
                    }
                }
            }
        }

    }

}
