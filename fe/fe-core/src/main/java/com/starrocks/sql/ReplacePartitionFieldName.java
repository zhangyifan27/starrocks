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

package com.starrocks.sql;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;

import java.util.List;

public class ReplacePartitionFieldName {

    static final String HIVE_PART = "_hive_part";
    static final String SYS_THIVE = "sys_thive_";

    public static void checkReplacePartitionFieldName(ConnectContext connectContext, QueryRelation query, List<String> colNames) {
        if (connectContext == null) {
            return;
        }
        connectContext.setReplacePartitionFieldName(false);
        if (connectContext.getSessionVariable() == null) {
            return;
        }
        if (!connectContext.getSessionVariable().isEnableReplacePartitionFieldName()) {
            return;
        }
        if (!(query instanceof SelectRelation)) {
            return;
        }
        SelectRelation select = (SelectRelation) query;
        boolean hasStar = select.getSelectList()
                .getItems().stream().anyMatch(SelectListItem::isStar);
        if (!hasStar) {
            return;
        }
        // imp_date_hive_part
        for (int i = colNames.size() - 1; i >= 0; i--) {
            if (colNames.get(i).endsWith(HIVE_PART)) {
                connectContext.setReplacePartitionFieldName(true);
                return;
            }
        }
    }

    public static String checkAndReplacePartitionFieldName(ConnectContext connectContext, String colName) {
        if (connectContext == null) {
            return colName;
        }
        if (!connectContext.isReplacePartitionFieldName()) {
            return colName;
        }
        if (colName == null || !colName.endsWith(HIVE_PART)) {
            return colName;
        }
        // imp_date_hive_part -> sys_thive_imp_date
        String baseName = colName.substring(0, colName.length() - HIVE_PART.length());
        return SYS_THIVE + baseName;
    }
}
