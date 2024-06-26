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


package com.starrocks.connector.hive.glue.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionValueList;

import java.util.List;
import java.util.Map;

public final class PartitionUtils {

    public static Map<PartitionKey, Partition> buildPartitionMap(final List<Partition> partitions) {
        Map<PartitionKey, Partition> partitionValuesMap = Maps.newHashMap();
        for (Partition partition : partitions) {
            partitionValuesMap.put(new PartitionKey(partition), partition);
        }
        return partitionValuesMap;
    }

    public static List<PartitionValueList> getPartitionValuesList(final Map<PartitionKey, Partition> partitionMap) {
        List<PartitionValueList> partitionValuesList = Lists.newArrayList();
        for (Map.Entry<PartitionKey, Partition> entry : partitionMap.entrySet()) {
            partitionValuesList.add(PartitionValueList.builder().values(entry.getValue().values()).build());
        }
        return partitionValuesList;
    }

    public static boolean isInvalidUserInputException(Exception e) {
        // exceptions caused by invalid requests, in which case we know all partitions creation failed
        return e instanceof EntityNotFoundException || e instanceof InvalidInputException;
    }

}
