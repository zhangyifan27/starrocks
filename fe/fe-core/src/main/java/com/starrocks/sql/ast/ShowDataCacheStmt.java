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

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowDataCacheStmt extends ShowStmt {

    private TableName tableName;

    public ShowDataCacheStmt(TableName tableName, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Partition Id", ScalarType.BIGINT))
                .addColumn(new Column("Table Id", ScalarType.BIGINT))
                .addColumn(new Column("Partition", ScalarType.createVarchar(128)))
                .addColumn(new Column("Version", ScalarType.BIGINT))
                .addColumn(new Column("Cache Status", ScalarType.createVarchar(32)))
                .addColumn(new Column("Created Time", ScalarType.createVarchar(32)))
                .addColumn(new Column("Last Refresh Time", ScalarType.createVarchar(32)))
                .addColumn(new Column("Expired Files", ScalarType.BIGINT))
                .addColumn(new Column("Expired Bytes", ScalarType.BIGINT))
                .addColumn(new Column("TTL Expire At", ScalarType.createVarchar(32)))
                .addColumn(new Column("Partition Field", ScalarType.createVarchar(64)))
                .addColumn(new Column("Partition Field Type", ScalarType.createVarchar(32)))
                .addColumn(new Column("Cache Data Size", ScalarType.createVarchar(32)))
                .addColumn(new Column("Partition Path", ScalarType.createVarchar(256)))
                .addColumn(new Column("Partition Unit", ScalarType.createVarchar(32)))
                .addColumn(new Column("Partition Field Format", ScalarType.createVarchar(64)))
                .addColumn(new Column("Hash Ring Signature", ScalarType.createVarchar(128)));
        return builder.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowDataCacheStmt(this, context);
    }
}
