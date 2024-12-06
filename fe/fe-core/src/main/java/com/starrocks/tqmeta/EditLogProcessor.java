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

package com.starrocks.tqmeta;

import com.google.common.collect.Queues;
import com.starrocks.common.Config;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalTask;
import com.starrocks.metric.MetricRepo;
import com.starrocks.plugin.EditLogEvent;
import com.starrocks.plugin.EditLogPlugin;
import com.starrocks.plugin.Plugin;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.tqmeta.replay.ReplayData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class EditLogProcessor {
    private static final Logger LOG = LogManager.getLogger(EditLogProcessor.class);
    private static final long UPDATE_PLUGIN_INTERVAL_MS = 60L * 1000L; // 1min
    private static final long SEND_DISCARD_INTERVAL_MS = 60L * 1000L; // 1min

    private PluginMgr pluginMgr;
    private List<Plugin> editLogPlugins;
    private long lastUpdateTime = 0;
    private BlockingQueue<EditLogEvent> eventQueue = Queues.newLinkedBlockingDeque(Config.max_queue_size_for_report_metadata);
    private Thread workerThread;
    private AtomicLong discard = new AtomicLong(0);
    private long lastSendDiscard = 0;

    private volatile boolean isStopped = false;

    public EditLogProcessor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    public void start() {
        workerThread = new Thread(new EditLogProcessor.Worker(), "EditLogProcessor");
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

    public static boolean supportSend() {
        return GlobalStateMgr.getCurrentState().isLeader()
                && !GlobalStateMgr.isCheckpointThread();
    }

    public void handleEditLogElement(EditLogEvent editLogEvent) {
        try {
            eventQueue.offer(editLogEvent);
        } catch (Throwable e) {
            discard.incrementAndGet();
            LOG.debug("encounter exception when handle edit log event, ignore", e);
            MetricRepo.COUNTER_LOST_TQ_METADATA_NUM.increase(1L);
        }
    }

    public void sendJournalAsync(List<JournalTask> reportEvents, long startJournalId) {
        if (!supportSend()) {
            return;
        }
        try {
            EditLogEvent editLogEvent = new EditLogEvent(reportEvents, startJournalId);
            eventQueue.offer(editLogEvent);
        } catch (Throwable e) {
            discard.incrementAndGet();
            LOG.error("Send event report error", e);
            MetricRepo.COUNTER_LOST_TQ_METADATA_NUM.increase((long) reportEvents.size());
        }
    }

    public void sendReplayLogAsync(JournalEntity journal, long journalId) {
        if (!GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        sendReplayLogAsync(new ReplayData(journal.getOpCode(), journal.getData()), journalId);
    }

    public void sendReplayLogAsync(ReplayData data, long journalId) {
        if (!GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        try {
            EditLogEvent editLogEvent = new EditLogEvent(data, journalId);
            eventQueue.offer(editLogEvent);
        } catch (Throwable e) {
            discard.incrementAndGet();
            LOG.error("Send event report error", e);
            MetricRepo.COUNTER_LOST_TQ_METADATA_NUM.increase(1L);
        }
    }

    public class Worker implements Runnable {

        @Override
        public void run() {
            EditLogEvent editLogEvent;
            while (!isStopped) {
                // update edit log plugin list every UPDATE_PLUGIN_INTERVAL_MS.
                // because some of plugins may be installed or uninstalled at runtime.
                if (editLogPlugins == null || System.currentTimeMillis() - lastUpdateTime > UPDATE_PLUGIN_INTERVAL_MS) {
                    editLogPlugins = pluginMgr.getActivePluginList(PluginInfo.PluginType.EDITLOG);
                    lastUpdateTime = System.currentTimeMillis();
                    LOG.debug("update edit log plugins. num: {}", editLogPlugins.size());
                }

                try {
                    if (discard.get() > 0 &&
                            (System.currentTimeMillis() - lastSendDiscard > SEND_DISCARD_INTERVAL_MS)) {
                        long currentDiscard = discard.getAndSet(0);
                        editLogEvent = new EditLogEvent(currentDiscard);
                        lastSendDiscard = System.currentTimeMillis();
                    } else {
                        editLogEvent = eventQueue.poll(5, TimeUnit.SECONDS);
                    }
                    if (editLogEvent == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.debug("encounter exception when getting edit log event from queue, ignore", e);
                    continue;
                }
                // only fe leader process
                if (!supportSend()) {
                    continue;
                }
                for (Plugin plugin : editLogPlugins) {
                    try {
                        if (((EditLogPlugin) plugin).eventFilter(editLogEvent.getEventType())) {
                            ((EditLogPlugin) plugin).exec(editLogEvent);
                        }
                    } catch (Throwable e) {
                        LOG.debug("encounter exception when processing edit log event.", e);
                    }
                }
            }
        }

    }
}
