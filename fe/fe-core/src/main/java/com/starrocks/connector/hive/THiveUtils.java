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
package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.HiveTable;

public class THiveUtils {

    public static String toTHivePartitionKey(String fieldName) {
        return fieldName + THiveConstants.SUFFIX;
    }

    public static String toOriginalKey(String partitionKey) {
        return partitionKey.replaceAll(THiveConstants.SUFFIX + "$", "");
    }

    public static String getPartitionType(HiveTable table, String partitionColumn) {
        String thivePartitionColumns = table.getProperties().get(THiveConstants.THIVE_PARTITION_COLUMNS);
        String thivePartitionTypesStr = table.getProperties().get(THiveConstants.THIVE_PARTITION_TYPES);
        String[] columnSplits = thivePartitionColumns.split(THiveConstants.SEPARATOR);
        String[] typeSplits = thivePartitionTypesStr.split(THiveConstants.SEPARATOR);
        Preconditions.checkArgument(columnSplits.length == typeSplits.length);
        for (int i = 0; i < columnSplits.length; i++) {
            if (columnSplits[i].equals(partitionColumn)) {
                return typeSplits[i];
            }
        }
        return null;
    }

    public static boolean isListPartitionType(String partitionType) {
        return (partitionType != null) && partitionType.equalsIgnoreCase(THiveConstants.LIST);
    }

    public static boolean isListPartitionType(HiveTable table, String partitionColumn) {
        return isListPartitionType(getPartitionType(table, partitionColumn));
    }
}
