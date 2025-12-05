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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.planner.PlanNodeAndHash;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.hbo.PlanStatistics;
import com.starrocks.sql.optimizer.statistics.hbo.RecentRunsPlanStatistics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * StatsCalculator by using hbo plan stats. to do estimation.
 */
public class HboStatsCalculator extends StatisticsCalculator {
    private final HboPlanStatisticsProvider hboPlanStatisticsProvider;

    public HboStatsCalculator(ExpressionContext expressionContext,
                              ColumnRefFactory columnRefFactory,
                              OptimizerContext optimizerContext) {
        super(expressionContext, columnRefFactory, optimizerContext);
        this.hboPlanStatisticsProvider = Objects.requireNonNull(GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager()
                        .getHboPlanStatisticsProvider(), "HboPlanStatisticsProvider is null");
    }

    @Override
    public Void computeHMSTableScanNode(Operator node, ExpressionContext context, Table table,
                                        Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        super.computeHMSTableScanNode(node, context, table, colRefToColumnMetaMap);
        Statistics cboStats = context.getStatistics();
        OptExpression expression = context.getOptExpression();
        GroupExpression groupExpression = context.getGroupExpression();
        if (expression != null) {
            Statistics hboStats = getStatsFromHboPlanStats(expression, cboStats);
            context.setStatistics(hboStats);
        } else if (groupExpression != null) {
            Statistics hboStats = getStatsFromHboPlanStats(groupExpression, cboStats);
            context.setStatistics(hboStats);
        } else {
            Statistics hboStats = getStatsFromHboPlanStats(node, cboStats);
            context.setStatistics(hboStats);
        }
        return null;
    }

    @Override
    public Void computeOlapScanNode(Operator node, ExpressionContext context, Table table,
                                    Collection<Long> selectedPartitionIds,
                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        super.computeOlapScanNode(node, context, table, selectedPartitionIds, colRefToColumnMetaMap);
        Statistics cboStats = context.getStatistics();
        OptExpression expression = context.getOptExpression();
        GroupExpression groupExpression = context.getGroupExpression();
        if (expression != null) {
            Statistics hboStats = getStatsFromHboPlanStats(expression, cboStats);
            context.setStatistics(hboStats);
        } else if (groupExpression != null) {
            Statistics hboStats = getStatsFromHboPlanStats(groupExpression, cboStats);
            context.setStatistics(hboStats);
        } else {
            Statistics hboStats = getStatsFromHboPlanStats(node, cboStats);
            context.setStatistics(hboStats);
        }
        return null;
    }

    @Override
    public Void computeJoinNode(ExpressionContext context, JoinOperator joinType, ScalarOperator joinOnPredicate) {
        super.computeJoinNode(context, joinType, joinOnPredicate);
        Statistics cboStats = context.getStatistics();
        OptExpression expression = context.getOptExpression();
        GroupExpression groupExpression = context.getGroupExpression();
        boolean hasCte = !optimizerContext.getCteContext().getAllCTEProduce().isEmpty();
        if (expression != null) {
            Statistics hboStats = getStatsFromHboPlanStats(expression, cboStats);
            context.setStatistics(hboStats);
        } else if (groupExpression != null && !hasCte) {
            Statistics hboStats = getStatsFromHboPlanStats(context.getGroupExpression(), cboStats);
            context.setStatistics(hboStats);
        }
        return null;
    }

    @Override
    public Void computeAggregateNode(Operator node, ExpressionContext context, List<ColumnRefOperator> groupBys,
                                      Map<ColumnRefOperator, CallOperator> aggregations) {
        // NOTE: aggr has two times matching, one is the global but logical aggr,
        // another is local but physical aggr.
        // the physical one can be matched but the logical one is hard to be matched.
        // e.g, logical one likes "count(*) AS `count(*)`#4"
        //      local physical one likes "partial_count(*) AS `partial_count(*)`#5"
        //      global physical one likes "count(partial_count(*)#5) AS `count(*)`#4"
        super.computeAggregateNode(node, context, groupBys, aggregations);
        Statistics cboStats = context.getStatistics();
        OptExpression expression = context.getOptExpression();
        GroupExpression groupExpression = context.getGroupExpression();
        boolean hasCte = !optimizerContext.getCteContext().getAllCTEProduce().isEmpty();
        if (expression != null) {
            Statistics hboStats = getStatsFromHboPlanStats(expression, cboStats);
            context.setStatistics(hboStats);
        } else if (groupExpression != null && !hasCte) {
            Statistics hboStats = getStatsFromHboPlanStats(context.getGroupExpression(), cboStats);
            context.setStatistics(hboStats);
        }
        return null;
    }

    private Statistics getStatsFromHboPlanStats(OptExpression expression, Statistics delegateStats) {
        PlanNodeAndHash planNodeAndHash = HboUtils.getOperatorHash(expression);
        RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboPlanStats(planNodeAndHash);
        PlanStatistics matchedPlanStatistics = HboUtils.getMatchedPlanStatistics(planStatistics, optimizerContext);
        if (matchedPlanStatistics != null) {
            delegateStats = delegateStats.withRowCountAndHboFlag(matchedPlanStatistics.getPullRows());
        }
        return delegateStats;
    }

    private Statistics getStatsFromHboPlanStats(GroupExpression expression, Statistics delegateStats) {
        PlanNodeAndHash planNodeAndHash = HboUtils.getOperatorHash(expression);
        RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboPlanStats(planNodeAndHash);
        PlanStatistics matchedPlanStatistics = HboUtils.getMatchedPlanStatistics(planStatistics, optimizerContext);
        if (matchedPlanStatistics != null) {
            delegateStats = delegateStats.withRowCountAndHboFlag(matchedPlanStatistics.getPullRows());
        }
        return delegateStats;
    }

    private Statistics getStatsFromHboPlanStats(Operator node, Statistics delegateStats) {
        PlanNodeAndHash planNodeAndHash = HboUtils.getOperatorHash(node);
        RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboPlanStats(planNodeAndHash);
        PlanStatistics matchedPlanStatistics = HboUtils.getMatchedPlanStatistics(planStatistics, optimizerContext);
        if (matchedPlanStatistics != null) {
            delegateStats = delegateStats.withRowCountAndHboFlag(matchedPlanStatistics.getPullRows());
        }
        return delegateStats;
    }

    private static String getFingerprintForTable(Table table) {
        String partitions = "";
        String tableType = "";
        switch (table.getType()) {
            case HIVE:
                tableType = "HiveScan";
                break;
            case ICEBERG:
                tableType = "IcebergScan";
                break;
            case OLAP:
                tableType = "OlapScan";
                break;
            default:
                break;
        }
        return Utils.toSqlString(tableType + "[" +
                Utils.getQualifiedTableName(table) + partitions + "]" + "#" + Utils.getQualifiedTableKey(table));
    }

    public static Statistics getStatsFromHboPlanStats(OptimizerContext optimizerContext,
                                                      Table table,
                                                      Statistics delegateStats,
                                                      List<ColumnRefOperator> columns) {
        HboPlanStatisticsProvider hboPlanStatisticsProvider = GlobalStateMgr.getCurrentState()
                .getHboPlanStatisticsManager().getHboPlanStatisticsProvider();
        if (hboPlanStatisticsProvider == null) {
            return delegateStats;
        }
        String tableFingerprint = getFingerprintForTable(table);
        String planHash = HboUtils.getPlanFingerprintHash(tableFingerprint);
        MockOperator dummyOp = new MockOperator(OperatorType.LOGICAL_OLAP_SCAN);
        PlanNodeAndHash planNodeAndHash = new PlanNodeAndHash(dummyOp, Optional.of(planHash));
        RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboPlanStats(planNodeAndHash);
        PlanStatistics matchedPlanStatistics = HboUtils.getMatchedPlanStatistics(planStatistics, optimizerContext);
        if (matchedPlanStatistics != null) {
            delegateStats = delegateStats.withRowCountAndHboFlag(matchedPlanStatistics.getPullRows(), columns);
        }
        return delegateStats;
    }
}

