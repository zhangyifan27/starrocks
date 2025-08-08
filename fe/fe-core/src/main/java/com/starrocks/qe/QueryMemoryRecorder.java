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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/QeProcessorImpl.java

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

import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class QueryMemoryRecorder {
    private static final Logger LOG = LogManager.getLogger(QueryMemoryRecorder.class);

    private static final long MEM_CHUNK_SIZE = 2 * 1024 * 1024; //2M
    public static final int MEMORY_RECORD_LIMIT_SIZE = 10;

    @SerializedName(value = "memoryRecordMap")
    private final Map<String, MemoryRecord> memoryRecordMap;
    private final Map<UUID, QueryInfo> idMap;
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();

    public QueryMemoryRecorder() {
        memoryRecordMap = new ConcurrentHashMap<>();
        idMap = new ConcurrentHashMap<>();
        // clear expire record every day
        cleaner.scheduleAtFixedRate(this::cleanExpiredKeys, 1, 1, TimeUnit.DAYS);
    }

    public void recordDigestWithFlowIdByQueryId(UUID queryId, QueryInfo queryInfo) {
        idMap.put(queryId, queryInfo);
    }

    public double recordQueryMemory(UUID queryId, double queryPeakMemoryUsagePerNode) {
        QueryInfo queryInfo = idMap.get(queryId);
        double queryMemory = 0;
        if (queryInfo != null) {
            queryMemory = queryPeakMemoryUsagePerNode * queryInfo.getWorkerNum() + (queryInfo.getInstanceNum() * MEM_CHUNK_SIZE);
            long time = System.currentTimeMillis();
            put(queryInfo.getDigestWithFlowId(), queryMemory, time);
            if (GlobalStateMgr.getCurrentState().isLeader()) {
                MemoryRecordInfo memoryRecordInfo = new MemoryRecordInfo(queryInfo.getDigestWithFlowId(), queryMemory, time);
                GlobalStateMgr.getCurrentState().getEditLog().logRecordQueryMemory(memoryRecordInfo);
            }
            idMap.remove(queryId);
        }
        return queryMemory;
    }

    public void put(String id, double value, long time) {
        MemoryRecord record = memoryRecordMap.computeIfAbsent(id, k -> new MemoryRecord(id, time));
        record.addRecord(value, time);
    }

    public MemoryRecord get(String key) {
        return memoryRecordMap.get(key);
    }

    public double getMaxMemoryRecently(String key) {
        MemoryRecord memoryRecord = memoryRecordMap.get(key);
        if (memoryRecord == null) {
            return 0;
        }
        ConcurrentLinkedDeque<Double> records = memoryRecord.getRecords();
        double maxMemory = 0;
        StringBuilder msg = new StringBuilder();
        for (double record : records) {
            maxMemory = Math.max(maxMemory, record);
            msg.append(record).append("|");
        }
        LOG.info("feedback memory record : \n" +
                "id: {}\n" +
                "recently memory usage : {}\n" +
                "recentlyMaxMemory : {}\n" +
                "historyMaxMemory : {}\n" +
                "lastUpdateTime : {}",
                memoryRecord.getId(), msg, maxMemory, memoryRecord.getMaxValue(), new Date(memoryRecord.getTime()));
        return maxMemory;
    }

    private void cleanExpiredKeys() {
        long currentTime = System.currentTimeMillis();
        memoryRecordMap.entrySet().removeIf(entry ->
                (currentTime - entry.getValue().getTime()) > TimeUnit.DAYS.toMillis(10)
        );
    }

    public void shutdown() {
        cleaner.shutdown();
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.QUERY_MEM_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        QueryMemoryRecorder queryMemoryRecorder = reader.readJson(QueryMemoryRecorder.class);
        memoryRecordMap.putAll(queryMemoryRecorder.memoryRecordMap);
    }
}
