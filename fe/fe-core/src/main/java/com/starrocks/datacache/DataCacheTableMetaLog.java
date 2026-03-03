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

import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Lightweight log entity for data cache table meta, used in edit log.
 */
public class DataCacheTableMetaLog implements Writable {
    private DataCacheTableMeta tableMeta;

    public DataCacheTableMetaLog() {
    }

    public DataCacheTableMetaLog(DataCacheTableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    public DataCacheTableMeta getTableMeta() {
        return tableMeta;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tableMeta.getTableId());
        out.writeUTF(nullToEmpty(tableMeta.getCatalogName()));
        out.writeUTF(nullToEmpty(tableMeta.getDbName()));
        out.writeUTF(nullToEmpty(tableMeta.getTableName()));
        out.writeUTF(nullToEmpty(tableMeta.getTableType()));
        out.writeLong(tableMeta.getCreatedTime() == null ? -1L
                : tableMeta.getCreatedTime().toEpochSecond(ZoneOffset.UTC));
        out.writeLong(tableMeta.getUpdatedTime() == null ? -1L
                : tableMeta.getUpdatedTime().toEpochSecond(ZoneOffset.UTC));
        out.writeLong(tableMeta.getCacheSize());
        out.writeUTF(nullToEmpty(tableMeta.getScheduleTaskName()));
    }

    public void readFields(DataInput in) throws IOException {
        long tableId = in.readLong();
        String catalog = emptyToNull(in.readUTF());
        String db = emptyToNull(in.readUTF());
        String table = emptyToNull(in.readUTF());
        String tableType = emptyToNull(in.readUTF());
        long createdEpoch = in.readLong();
        long updatedEpoch = in.readLong();
        long cacheSize = in.readLong();
        String scheduleTaskName = null;
        try {
            scheduleTaskName = emptyToNull(in.readUTF());
        } catch (EOFException ignored) {
            // schedule task name is optional for compatibility with older edit logs
        }
        LocalDateTime created = createdEpoch < 0 ? null : LocalDateTime.ofEpochSecond(createdEpoch, 0, ZoneOffset.UTC);
        LocalDateTime updated = updatedEpoch < 0 ? null : LocalDateTime.ofEpochSecond(updatedEpoch, 0, ZoneOffset.UTC);
        tableMeta = new DataCacheTableMeta(tableId, catalog, db, table, tableType, created, updated, cacheSize,
                scheduleTaskName);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }
}
