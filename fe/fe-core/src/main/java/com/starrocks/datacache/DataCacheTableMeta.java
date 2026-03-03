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

package com.starrocks.datacache;

import java.time.LocalDateTime;

public final class DataCacheTableMeta {
    private final long tableId;
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final String tableType;
    private final LocalDateTime createdTime;
    private LocalDateTime updatedTime;
    private long cacheSize;
    // schedule task name of the data cache job, can be used to fetch detailed job info
    private String scheduleTaskName;

    public DataCacheTableMeta(long tableId, String catalogName, String dbName, String tableName, String tableType,
                              LocalDateTime createdTime, LocalDateTime updatedTime, long cacheSize,
                              String scheduleTaskName) {
        this.tableId = tableId;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.createdTime = createdTime;
        this.updatedTime = updatedTime;
        this.cacheSize = cacheSize;
        this.scheduleTaskName = scheduleTaskName;
    }

    public long getTableId() {
        return tableId;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public LocalDateTime getCreatedTime() {
        return createdTime;
    }

    public LocalDateTime getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(LocalDateTime updatedTime) {
        this.updatedTime = updatedTime;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public String getScheduleTaskName() {
        return scheduleTaskName;
    }

    public void setScheduleTaskName(String scheduleTaskName) {
        this.scheduleTaskName = scheduleTaskName;
    }

    public DataCacheTableMeta copy() {
        return new DataCacheTableMeta(tableId, catalogName, dbName, tableName, tableType,
                createdTime, updatedTime, cacheSize, scheduleTaskName);
    }
}
