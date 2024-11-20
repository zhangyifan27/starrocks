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

package com.starrocks.connector;

import StorageEngineClient.CombineFileSplit;
import StorageEngineClient.FormatStorageInputFormat;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.Partition;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StorageFormatUtils {
    private static final Logger LOG = LogManager.getLogger(StorageFormatUtils.class);

    public static RemoteFileInfo buildRemoteFileInfoForStorageFormat(Partition partition, List<RemoteFileDesc> fileDescs) {
        try {
            FormatStorageInputFormat formatStorageInputFormat = new FormatStorageInputFormat();
            int count = 0;
            for (RemoteFileDesc desc : fileDescs) {
                count += desc.getBlockDescs().size();
            }
            JobConf job = new JobConf();
            job.set("mapreduce.input.fileinputformat.inputdir", partition.getFullPath());
            formatStorageInputFormat.configure(job);
            InputSplit[] splits = formatStorageInputFormat.getSplits(job, count);
            List<CombineFileSplit> splitList = new ArrayList<>(splits.length);
            long totLength = 0;
            for (InputSplit split : splits) {
                splitList.add((CombineFileSplit) split);
                totLength += split.getLength();
            }
            RemoteFileDesc remoteFileDesc = StorageFormatRemoteFileDesc.createStorageFormatRemoteFileDesc(totLength, splitList);
            List<RemoteFileDesc> sfFileDescs = new ArrayList<>(1);
            sfFileDescs.add(remoteFileDesc);
            RemoteFileInfo.Builder builder = RemoteFileInfo.builder()
                    .setFormat(partition.getInputFormat())
                    .setFullPath(partition.getFullPath())
                    .setFiles(sfFileDescs);
            return builder.build();
        } catch (Exception e) {
            LOG.error("Failed to get hive remote file's metadata on path: {}", partition.getFullPath(), e);
            throw new StarRocksConnectorException("Failed to get hive remote file's metadata on path: %s. msg: %s",
                    partition.getFullPath(), e.getMessage());
        }
    }

    private static final Base64.Encoder BASE64_ENCODER =
            java.util.Base64.getUrlEncoder().withoutPadding();

    public static String encodeSplitToString(CombineFileSplit split) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream w = new DataOutputStream(baos);
            split.write(w);
            return new String(BASE64_ENCODER.encode(baos.toByteArray()), UTF_8);
        } catch (Exception e) {
            LOG.error("Failed to serialize CombineFileSplit", e);
            throw new StarRocksConnectorException("Failed to serialize CombineFileSplit, msg: %s", e.getMessage());
        }
    }
}
