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
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.thrift.TPulsarRLTaskProgress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.MessageId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this is description of pulsar routine load progress
 * the data before position was already loaded in StarRocks
 */
public class PulsarProgress extends RoutineLoadProgress implements GsonPreProcessable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(PulsarProgress.class);

    private Map<String, MessageId> partitionToInitialPosition = Maps.newConcurrentMap();

    @SerializedName(value = "ptpb")
    private Map<String, byte[]> partitionToInitialPositionBytes;

    public PulsarProgress() {
        super(LoadDataSourceType.PULSAR);
    }

    public PulsarProgress(TPulsarRLTaskProgress tPulsarRLTaskProgress) throws UserException {
        super(LoadDataSourceType.PULSAR);
        for (Map.Entry<String, ByteBuffer> initialPosition : tPulsarRLTaskProgress.getPartitionInitialPositions()
                .entrySet()) {
            try {
                partitionToInitialPosition.put(initialPosition.getKey(),
                        MessageId.fromByteArray(initialPosition.getValue().array()));
            } catch (IOException e) {
                throw new UserException(
                        "Failed to deserialize messageId for partition: " + initialPosition.getKey(), e);
            }
        }
    }

    public Map<String, MessageId> getPartitionToInitialPosition() {
        return partitionToInitialPosition;
    }

    public Map<String, MessageId> getPartitionToInitialPosition(List<String> partitions) {
        // TODO: tmp code for compatibility
        for (String partition : partitions) {
            if (!partitionToInitialPosition.containsKey(partition)) {
                partitionToInitialPosition.put(partition, MessageId.latest);
            }
        }

        Map<String, MessageId> result = Maps.newHashMap();
        for (Map.Entry<String, MessageId> entry : partitionToInitialPosition.entrySet()) {
            for (String partition : partitions) {
                if (entry.getKey().equals(partition)) {
                    result.put(partition, entry.getValue());
                }
            }
        }
        return result;
    }

    public MessageId getInitialPositionByPartition(String partition) {
        // TODO: tmp code for compatibility
        if (!partitionToInitialPosition.containsKey(partition)) {
            partitionToInitialPosition.put(partition, MessageId.latest);
        }
        return partitionToInitialPosition.get(partition);
    }

    public boolean containsPartition(String pulsarPartition) {
        return this.partitionToInitialPosition.containsKey(pulsarPartition);
    }

    public void addPartitionToInitialPosition(Pair<String, MessageId> partitionToInitialPosition) {
        this.partitionToInitialPosition.put(partitionToInitialPosition.first, partitionToInitialPosition.second);
    }

    public void modifyInitialPositions(List<Pair<String, MessageId>> partitionInitialPositions) {
        for (Pair<String, MessageId> pair : partitionInitialPositions) {
            this.partitionToInitialPosition.put(pair.first, pair.second);
        }
    }

    private void getReadableProgress(Map<String, String> showPartitionToPosition) {
        for (Map.Entry<String, MessageId> entry : partitionToInitialPosition.entrySet()) {
            showPartitionToPosition.put(entry.getKey(), entry.getValue().toString());
        }
    }

    @Override
    public String toString() {
        Map<String, String> showPartitionToPosition = Maps.newHashMap();
        getReadableProgress(showPartitionToPosition);
        return "PulsarProgress [partitionToPosition="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionToPosition) + "]";
    }

    @Override
    public String toJsonString() {
        Map<String, String> showPartitionToPosition = Maps.newHashMap();
        getReadableProgress(showPartitionToPosition);
        Gson gson = new Gson();
        return gson.toJson(showPartitionToPosition);
    }

    @Override
    public void update(RoutineLoadProgress progress) {
        PulsarProgress newProgress = (PulsarProgress) progress;
        this.partitionToInitialPosition.putAll(newProgress.partitionToInitialPosition);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionToInitialPosition.size());
        for (Map.Entry<String, MessageId> entry : partitionToInitialPosition.entrySet()) {
            Text.writeString(out, entry.getKey());
            byte[] messageId = entry.getValue().toByteArray();
            out.writeInt(messageId.length);
            out.write(messageId, 0, messageId.length);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int positionSize = in.readInt();
        partitionToInitialPosition = new HashMap<>();
        for (int i = 0; i < positionSize; i++) {
            String partition = Text.readString(in);
            int length = in.readInt();
            byte[] messageId = new byte[length];
            in.readFully(messageId, 0, length);
            partitionToInitialPosition.put(partition, MessageId.fromByteArray(messageId));
        }
    }

    @Override
    public void gsonPreProcess() throws IOException {
        partitionToInitialPositionBytes = Maps.newConcurrentMap();
        for (Map.Entry<String, MessageId> entry : partitionToInitialPosition.entrySet()) {
            partitionToInitialPositionBytes.put(entry.getKey(), entry.getValue().toByteArray());
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (partitionToInitialPositionBytes != null) {
            partitionToInitialPosition = Maps.newConcurrentMap();
            for (Map.Entry<String, byte[]> entry : partitionToInitialPositionBytes.entrySet()) {
                partitionToInitialPosition.put(entry.getKey(), MessageId.fromByteArray(entry.getValue()));
            }
        }
    }
}
