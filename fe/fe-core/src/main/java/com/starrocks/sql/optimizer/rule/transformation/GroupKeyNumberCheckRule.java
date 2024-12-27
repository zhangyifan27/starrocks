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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/SessionVariable.java

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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.common.WarningCode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupKeyNumberCheckRule extends TransformationRule {


    public GroupKeyNumberCheckRule() {
        super(RuleType.TF_GROUP_KEY_NUMBER_CHECK_RULE, Pattern.create(OperatorType.LOGICAL_AGGR));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        final ConnectContext connectContext = ConnectContext.get();
        if (connectContext.getSessionVariable().isEnableSqlDialog()) {
            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
            int groupingKeySize = aggregationOperator.getGroupingKeys().size();
            int groupKeyLimit = connectContext.getSessionVariable().getGroupKeyLimit();
            if (groupingKeySize > groupKeyLimit) {
                List<String> warning = new ArrayList<>();
                warning.add("0");
                warning.add(String.valueOf(WarningCode.WARN_GROUP_KEY_LIMIT_EXCEEDED.getCode()));
                warning.add(WarningCode.WARN_GROUP_KEY_LIMIT_EXCEEDED.formatWarningMsg(groupKeyLimit, groupingKeySize));
                List<List<String>> queryWarnings = new ArrayList<>();
                if (connectContext.getQueryWarnings() != null) {
                    queryWarnings = connectContext.getQueryWarnings();
                }
                queryWarnings.add(warning);
                connectContext.setQueryWarnings(queryWarnings);
                connectContext.getState().setWarningRows(queryWarnings.size());
            }
        }
        return Collections.emptyList();
    }
}
