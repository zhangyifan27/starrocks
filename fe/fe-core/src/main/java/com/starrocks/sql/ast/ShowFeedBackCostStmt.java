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

import com.starrocks.analysis.LimitElement;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowFeedBackCostStmt extends ShowStmt {

    private static final String ID_COL = "Id";
    private static final String RECENTLY_MAX_MEM =  "recentlyMaxMemory";
    private static final String RECENTLY_MEM_USAGE = "recentlyMemoryUsage";
    private static final String HISTORY_MAX_MEM = "historyMaxMemory";
    private static final String LAST_UPDATE_TIME = "lastUpdateTime";

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(ID_COL, ScalarType.createVarchar(50)))
                    .addColumn(new Column(RECENTLY_MAX_MEM, ScalarType.createVarchar(100)))
                    .addColumn(new Column(RECENTLY_MEM_USAGE, ScalarType.createVarchar(1000)))
                    .addColumn(new Column(HISTORY_MAX_MEM, ScalarType.createVarchar(100)))
                    .addColumn(new Column(LAST_UPDATE_TIME, ScalarType.createVarchar(30)))
                    .build();

    private final String pattern;
    private final LimitElement limitElement;

    public ShowFeedBackCostStmt(String pattern, LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.limitElement = limitElement;
    }

    public String getPattern() {
        return pattern;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowFeedBackCostStatement(this, context);
    }
}

