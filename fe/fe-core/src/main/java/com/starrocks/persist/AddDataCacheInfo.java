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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.datacache.DataCacheRecord;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AddDataCacheInfo implements Writable {

    @SerializedName(value = "be")
    private Long beid;
    @SerializedName(value = "tb")
    private TableName tableName;
    @SerializedName(value = "rd")
    private DataCacheRecord dataCacheRecord;

    public AddDataCacheInfo() {
    }

    public AddDataCacheInfo(Long beid, TableName tableName, DataCacheRecord dataCacheRecord) {
        this.beid = beid;
        this.tableName = tableName;
        this.dataCacheRecord = dataCacheRecord;
    }

    public Long getBeid() {
        return beid;
    }

    public TableName getTableName() {
        return tableName;
    }

    public DataCacheRecord getDataCacheRecord() {
        return dataCacheRecord;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static AddDataCacheInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AddDataCacheInfo.class);
    }
}
