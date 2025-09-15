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
import com.starrocks.analysis.LimitElement;
import com.starrocks.common.Config;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.DateUtils;
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
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class QueryMemoryRecorder {
    private static final Logger LOG = LogManager.getLogger(QueryMemoryRecorder.class);

    private static final long MEM_CHUNK_SIZE = 2 * 1024 * 1024; //2M
    public static final int MEMORY_RECORD_LIMIT_SIZE = 10;
    private static final double QUERY_POOL_RATIO = 0.75;

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
        double queryMemory = Config.max_cost_by_feedback;
        if (queryInfo != null) {
            queryMemory = recordQueryMemory(queryId, queryInfo.getDigestWithFlowId(),
                    queryInfo.getWorkerNum(), queryInfo.getInstanceNum(), queryPeakMemoryUsagePerNode);
        }
        return queryMemory;
    }

    public double recordQueryMemory(UUID queryId, String digestWithFlowId, int workerNum, int instanceNum,
                                    double queryPeakMemoryUsagePerNode) {
        double queryMemory = queryPeakMemoryUsagePerNode * workerNum + (instanceNum * MEM_CHUNK_SIZE);
        long time = System.currentTimeMillis();
        put(digestWithFlowId, queryMemory, time);
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            MemoryRecordInfo memoryRecordInfo = new MemoryRecordInfo(digestWithFlowId, queryMemory, time);
            GlobalStateMgr.getCurrentState().getEditLog().logRecordQueryMemory(memoryRecordInfo);
        }
        idMap.remove(queryId);
        // return the cost with buffer weight
        return getMaxMemoryRecently(digestWithFlowId);
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
            return Config.max_cost_by_feedback * Config.cost_weight * Config.cost_buffer_weight / QUERY_POOL_RATIO;
        }
        ConcurrentLinkedDeque<Double> records = memoryRecord.getRecords();
        double maxMemory = 0;
        StringBuilder msg = new StringBuilder();
        for (double record : records) {
            maxMemory = Math.max(maxMemory, record);
            msg.append(record).append("|");
        }
        if (maxMemory <= 0) {
            maxMemory = Config.max_cost_by_feedback;
        }
        // cost_weight value of 2.5 is applied to calibrate the cost calculated by CBO in historical version

        // The purpose of setting the cost_buffer_weight parameter to 1.6 is to compensate for omissions in memory
        // statistics reported by the Backend (BE) and errors caused by data skew through coefficient adjustment,
        // thereby improving the accuracy of resource cost calculation.

        // the query pool is 80% of the node total memory, so / 0.75 to prevent query pool OOM.
        double cost = maxMemory * Config.cost_weight * Config.cost_buffer_weight / QUERY_POOL_RATIO;
        LOG.info("feedback memory record :\t" +
                "id: {}\t" +
                "recently memory usage : {}\t" +
                "recentlyMaxMemory : {}\t" +
                "historyMaxMemory : {}\t" +
                "lastUpdateTime : {}",
                memoryRecord.getId(), msg, maxMemory, memoryRecord.getMaxValue(), new Date(memoryRecord.getTime()));
        return cost;
    }

    public List<List<String>> getFeedbackCostInfo(PatternMatcher matcher,
                                                  LimitElement limitElement) {
        cleanExpiredKeys();
        List<List<String>> result = new ArrayList<>();

        List<MemoryRecord> matchedRecords;

        if (matcher != null) {
            matchedRecords = memoryRecordMap.entrySet().stream()
                    .filter(entry -> matcher.match(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
        } else {
            matchedRecords = new ArrayList<>(memoryRecordMap.values());
        }

        List<MemoryRecord> sortedRecords = matchedRecords.stream()
                .sorted(Comparator.comparingLong(MemoryRecord::getTime).reversed())
                .collect(Collectors.toList());

        List<MemoryRecord> resultMemoryRecords;
        if (limitElement != null && limitElement.getLimit() > 0) {
            resultMemoryRecords = sortedRecords.stream()
                    .skip(limitElement.getOffset())
                    .limit(limitElement.getLimit())
                    .collect(Collectors.toList());
        } else {
            resultMemoryRecords = sortedRecords;
        }

        for (MemoryRecord memoryRecord : resultMemoryRecords) {
            double maxMemory = 0;
            StringBuilder recentlyMemoryUsage = new StringBuilder();
            for (double record : memoryRecord.getRecords()) {
                maxMemory = Math.max(maxMemory, record);
                recentlyMemoryUsage.append(record).append("|");
            }
            List<String> line = new ArrayList<>();
            line.add(memoryRecord.getId());
            line.add(String.valueOf(maxMemory));
            line.add(recentlyMemoryUsage.toString());
            line.add(String.valueOf(memoryRecord.getMaxValue()));
            Instant instant = Instant.ofEpochMilli(memoryRecord.getTime());
            String formattedTime = DateUtils.DATE_TIME_FORMATTER_UNIX.withZone(ZoneId.systemDefault()).format(instant);
            line.add(formattedTime);
            result.add(line);
        }
        return result;
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
