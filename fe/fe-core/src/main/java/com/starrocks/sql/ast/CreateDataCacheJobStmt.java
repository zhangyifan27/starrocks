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

import com.starrocks.analysis.TaskName;
import com.starrocks.catalog.Type;
import com.starrocks.sql.parser.NodePosition;

public class CreateDataCacheJobStmt extends SubmitTaskStmt {

    private int dataCacheSelectStart;
    private int dataCacheSelectPropertiesStart;

    private String partitionFiled;
    private Type partitionFiledType;
    private String partitionFiledFormat;
    private String partitionUnit;
    private int cachePartitionNum;

    public CreateDataCacheJobStmt(TaskName taskName, DataCacheSelectStatement dataCacheSelectStatement,
                                  int dataCacheSelectStart, int dataCacheSelectPropertiesStart, NodePosition pos) {
        super(taskName, dataCacheSelectStart, dataCacheSelectStatement, pos);
        this.dataCacheSelectStart = dataCacheSelectStart;
        this.dataCacheSelectPropertiesStart = dataCacheSelectPropertiesStart;
    }

    public DataCacheSelectStatement getDataCacheSelectStatement() {
        return super.dataCacheSelectStmt;
    }

    public void setDataCacheSelectStatement(DataCacheSelectStatement dataCacheSelectStatement) {
        super.dataCacheSelectStmt = dataCacheSelectStatement;
    }

    public int getDataCacheSelectStart() {
        return dataCacheSelectStart;
    }

    public int getDataCacheSelectPropertiesStart() {
        return dataCacheSelectPropertiesStart;
    }

    public String getPartitionFiled() {
        return partitionFiled;
    }

    public void setPartitionFiled(String partitionFiled) {
        this.partitionFiled = partitionFiled;
    }

    public Type getPartitionFiledType() {
        return partitionFiledType;
    }

    public void setPartitionFiledType(Type partitionFiledType) {
        this.partitionFiledType = partitionFiledType;
    }

    public String getPartitionFiledFormat() {
        return partitionFiledFormat;
    }

    public void setPartitionFiledFormat(String partitionFiledFormat) {
        this.partitionFiledFormat = partitionFiledFormat;
    }

    public String getPartitionUnit() {
        return partitionUnit;
    }

    public void setPartitionUnit(String partitionUnit) {
        this.partitionUnit = partitionUnit;
    }

    public int getCachePartitionNum() {
        return cachePartitionNum;
    }

    public void setCachePartitionNum(int cachePartitionNum) {
        this.cachePartitionNum = cachePartitionNum;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDataCacheJobStatement(this, context);
    }
}
