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


package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PulsarUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TLoadSourceType;
import com.starrocks.thrift.TPulsarLoadInfo;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PulsarTaskInfo extends RoutineLoadTaskInfo {
    private static final Logger LOG = LogManager.getLogger(PulsarTaskInfo.class);
    private RoutineLoadMgr routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();

    private Map<String, MessageId> initialPositions = Maps.newHashMap();
    private Map<String, MessageId> latestPartPositions = Maps.newHashMap();

    public PulsarTaskInfo(UUID id, RoutineLoadJob job, long taskScheduleIntervalMs, long timeToExecuteMs,
                          Map<String, MessageId> initialPositions, long tastTimeoutMs) {
        super(id, job, taskScheduleIntervalMs, timeToExecuteMs, tastTimeoutMs);
        this.initialPositions.putAll(initialPositions);
    }

    public PulsarTaskInfo(long timeToExecuteMs, PulsarTaskInfo pulsarTaskInfo, Map<String, MessageId> initialPositions) {
        super(UUID.randomUUID(), pulsarTaskInfo.getJob(), pulsarTaskInfo.getTaskScheduleIntervalMs(),
                timeToExecuteMs, pulsarTaskInfo.getBeId(), pulsarTaskInfo.getTimeoutMs(), pulsarTaskInfo.getStatistics());
        this.initialPositions.putAll(initialPositions);
    }

    public List<String> getPartitions() {
        return new ArrayList<>(initialPositions.keySet());
    }

    public Map<String, MessageId> getInitialPositions() {
        return initialPositions;
    }

    public Map<String, ByteBuffer> getTInitialPositions() {
        Map<String, ByteBuffer> result = Maps.newHashMap();
        for (Map.Entry<String, MessageId> initialPosition : initialPositions.entrySet()) {
            result.put(initialPosition.getKey(), ByteBuffer.wrap(initialPosition.getValue().toByteArray()));
        }
        return result;
    }

    @Override
    public boolean readyToExecute() throws UserException {
        boolean ready = false;
        List<String> partitions = new ArrayList<>(initialPositions.keySet());

        PulsarRoutineLoadJob pulsarRoutineLoadJob = (PulsarRoutineLoadJob) job;
        Map<String, byte[]> latestPositions = PulsarUtil.getPositions(pulsarRoutineLoadJob.getServiceUrl(),
                pulsarRoutineLoadJob.getTopic(), pulsarRoutineLoadJob.getSubscription(),
                ImmutableMap.copyOf(pulsarRoutineLoadJob.getConvertedCustomProperties()),
                partitions, warehouseId);
        for (String partition : partitions) {
            MessageId latestPosition;
            try {
                latestPosition = MessageId.fromByteArray(latestPositions.get(partition));
            } catch (IOException e) {
                throw new RoutineLoadPauseException(
                        "Failed to deserialize messageId for partition: " + partition +
                                ", latest position: " + latestPositions.get(partition));
            }
            if (initialPositions.get(partition).compareTo(MessageId.latest) == 0) {
                initialPositions.put(partition, latestPosition);
            } else if (initialPositions.get(partition).compareTo(latestPosition) == -1) {
                ready = true;
                latestPartPositions.put(partition, latestPosition);
            }
        }

        return ready;
    }

    // the bellowing method will preempt the slots of BEs. So return ture until we find a better way.
    @Override
    public boolean isProgressKeepUp(RoutineLoadProgress progress, Map<String, Long> consumeLagsRowNum) {
        PulsarProgress pProgress = (PulsarProgress) progress;
        if (latestPartPositions.isEmpty()) {
            return true;
        }

        for (Map.Entry<String, MessageId> entry : latestPartPositions.entrySet()) {
            String part = entry.getKey();
            MessageId latestPosition = entry.getValue();
            MessageId consumedPosition = pProgress.getInitialPositionByPartition(part);
            if (consumedPosition != null) {
                if (consumedPosition.compareTo(latestPosition) == -1) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws UserException {
        PulsarRoutineLoadJob routineLoadJob = (PulsarRoutineLoadJob) job;

        // init tRoutineLoadTask and create plan fragment
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJob_id(routineLoadJob.getId());
        tRoutineLoadTask.setTxn_id(txnId);
        Database database = GlobalStateMgr.getCurrentState().getDb(routineLoadJob.getDbId());
        if (database == null) {
            throw new MetaNotFoundException("database " + routineLoadJob.getDbId() + " does not exist");
        }
        tRoutineLoadTask.setDb(database.getFullName());
        Table tbl = database.getTable(routineLoadJob.getTableId());
        if (tbl == null) {
            throw new MetaNotFoundException("table " + routineLoadJob.getTableId() + " does not exist");
        }
        tRoutineLoadTask.setTbl(tbl.getName());
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuth_code(routineLoadJob.getAuthCode());
        TPulsarLoadInfo tPulsarLoadInfo = new TPulsarLoadInfo();
        tPulsarLoadInfo.setService_url((routineLoadJob).getServiceUrl());
        tPulsarLoadInfo.setTopic((routineLoadJob).getTopic());
        tPulsarLoadInfo.setSubscription((routineLoadJob).getSubscription());
        if (!initialPositions.isEmpty()) {
            Map<String, ByteBuffer> tTnitialPositions = getTInitialPositions();
            tPulsarLoadInfo.setInitial_positions(tTnitialPositions);
        }
        tPulsarLoadInfo.setProperties(routineLoadJob.getConvertedCustomProperties());
        tRoutineLoadTask.setPulsar_load_info(tPulsarLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.PULSAR);
        tRoutineLoadTask.setParams(plan(routineLoadJob));
        tRoutineLoadTask.setMax_interval_s(routineLoadJob.getTaskConsumeSecond());
        tRoutineLoadTask.setMax_batch_rows(routineLoadJob.getMaxBatchRows());
        tRoutineLoadTask.setMax_batch_size(Config.max_routine_load_batch_size);
        if (!routineLoadJob.getFormat().isEmpty() && routineLoadJob.getFormat().equalsIgnoreCase("json")) {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_JSON);
        } else {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_CSV_PLAIN);
        }
        if (Math.abs(routineLoadJob.getMaxFilterRatio() - 1) > 0.001) {
            tRoutineLoadTask.setMax_filter_ratio(routineLoadJob.getMaxFilterRatio());
        }

        String partitionIdToOffsetInfo = "partitionIdToOffset="
                + Joiner.on("|").withKeyValueSeparator("_").join(initialPositions) + "]";
        LOG.info("Pulsar routine load task created, label: {}, {}.", label, partitionIdToOffsetInfo);
        return tRoutineLoadTask;
    }

    private void getReadablePositionInfo(Map<String, String> showPartitionToPosition) {
        for (Map.Entry<String, MessageId> entry : initialPositions.entrySet()) {
            showPartitionToPosition.put(entry.getKey(), entry.getValue().toString());
        }
    }

    @Override
    protected String getTaskDataSourceProperties() {
        Map<String, String> showPartitionToPosition = Maps.newHashMap();
        getReadablePositionInfo(showPartitionToPosition);
        Gson gson = new Gson();
        return gson.toJson(showPartitionToPosition);
    }

    @Override
    String dataSourceType() {
        return "pulsar";
    }

    @Override
    public String toString() {
        Map<String, String> showPartitionToPosition = Maps.newHashMap();
        getReadablePositionInfo(showPartitionToPosition);
        return "Task id: " + getId()
                + "[" + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionToPosition) + "]";
    }

    private TExecPlanFragmentParams plan(RoutineLoadJob routineLoadJob) throws UserException {
        TUniqueId loadId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        // plan for each task, in case table has change(rollup or schema change)
        TExecPlanFragmentParams tExecPlanFragmentParams = routineLoadJob.plan(loadId, txnId, label);
        return tExecPlanFragmentParams;
    }
}
