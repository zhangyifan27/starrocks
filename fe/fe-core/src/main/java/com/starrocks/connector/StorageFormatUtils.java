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
import org.apache.hadoop.conf.Configuration;
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
    private static final Configuration CONFIGURATION = new Configuration();
    private static final Base64.Encoder BASE64_ENCODER = java.util.Base64.getUrlEncoder().withoutPadding();

    public static List<RemoteFileDesc> buildRemoteFileDescsForStorageFormat(String path) {
        try {
            FormatStorageInputFormat formatStorageInputFormat = new FormatStorageInputFormat();
            JobConf job = new JobConf(CONFIGURATION);
            job.set("mapreduce.input.fileinputformat.inputdir", path);
            formatStorageInputFormat.configure(job);
            InputSplit[] splits = formatStorageInputFormat.getSplits(job, 1);
            List<CombineFileSplit> splitList = new ArrayList<>(splits.length);
            long totLength = 0;
            for (InputSplit split : splits) {
                splitList.add((CombineFileSplit) split);
                totLength += split.getLength();
            }
            RemoteFileDesc remoteFileDesc = StorageFormatRemoteFileDesc.createStorageFormatRemoteFileDesc(totLength, splitList);
            List<RemoteFileDesc> sfFileDescs = new ArrayList<>(1);
            sfFileDescs.add(remoteFileDesc);
            return sfFileDescs;
        } catch (Exception e) {
            LOG.error("Failed to get hive remote file's metadata on path: {}", path, e);
            throw new StarRocksConnectorException("Failed to get hive remote file's metadata on path: %s. msg: %s",
                    path, e.getMessage());
        }
    }

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
