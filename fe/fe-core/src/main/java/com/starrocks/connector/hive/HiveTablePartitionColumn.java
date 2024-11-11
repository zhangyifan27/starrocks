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

package com.starrocks.connector.hive;

import java.util.Objects;

public class HiveTablePartitionColumn {
    private final String databaseName;
    private final String tableName;
    private final String partitionColumn;

    public HiveTablePartitionColumn(String databaseName,
                                    String tableName,
                                    String partitionColumn) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionColumn = partitionColumn;
    }

    public static HiveTablePartitionColumn of(String dbName, String tblName, String partitionColumn) {
        return new HiveTablePartitionColumn(dbName, tblName, partitionColumn);
    }

    public String getTableName() {
        return tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public boolean approximateMatchTable(String db, String tblName) {
        return this.databaseName.equals(db) && this.tableName.equals(tblName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiveTablePartitionColumn other = (HiveTablePartitionColumn) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName) &&
                Objects.equals(partitionColumn, other.partitionColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, partitionColumn);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HiveTablePartitionColumn{");
        sb.append("databaseName='").append(databaseName).append('\'');
        sb.append(", tableName='").append(tableName).append('\'');
        sb.append(", partitionColumn=").append(partitionColumn);
        sb.append('}');
        return sb.toString();
    }
}