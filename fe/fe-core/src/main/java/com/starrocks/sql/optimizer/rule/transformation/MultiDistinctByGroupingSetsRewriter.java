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
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Function.CompareMode.IS_SUPERTYPE_OF;
import static com.starrocks.sql.optimizer.transformer.QueryTransformer.GROUPING_ID;

/*
 * Optimize multi count distinct aggregate node using grouping sets.
 * e.g: SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender) FROM emps group by deptno
 *
 * Before:
 *                  Agg[cd(sal), cd(gender)]
 *                              |
 *                          Child Plan
 *
 * After:
 *      Agg[count(if(GROUPING_ID = 1: sal, null), count(if(GROUPING_ID = 2: gender, null)]
 *                               |
 *                  Agg(group by (deptno, sal, gender, GROUPING_ID))
 *                               |
 *        Repeat((deptno, sal, GROUPING_ID = 1), (deptno, gender, GROUPING_ID = 2))
 *                               |
 *                          Child Plan
 *
 * Unit test: {@code RewriteMultiDistinctPlanTest}
 */
public class MultiDistinctByGroupingSetsRewriter {
    private static final Logger LOG = LogManager.getLogger(MultiDistinctByGroupingSetsRewriter.class);

    public List<OptExpression> transformImpl(OptExpression input, OptimizerContext context) {
        try {
            if (!check(input, context)) {
                return Lists.newArrayList();
            }
            OptExpression optExpression = buildGroupingSetsExpression(input, context);
            LOG.info("Rewrite multi distinct to grouping sets expression: {}", optExpression.debugString());
            return Lists.newArrayList(optExpression);
        } catch (Throwable e) {
            LOG.warn("Failed to rewrite multi distinct by grouping sets:", e);
            return Lists.newArrayList();
        }
    }

    public boolean check(OptExpression input, OptimizerContext context) {
        boolean sessionEnable = context.getSessionVariable().isEnableGroupingSets() && enableTableScans(input, context);
        if (!sessionEnable) {
            return false;
        }
        // disable distinct multi columns, like count(DISTINCT sal, gender)
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggrEntry : aggregate.getAggregations().entrySet()) {
            CallOperator call = aggrEntry.getValue();
            if (call.isDistinct() && call.getArguments().size() > 1) {
                return false;
            }
        }
        return true;
    }

    public OptExpression buildGroupingSetsExpression(OptExpression input, OptimizerContext context) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        ColumnRefOperator groupingIdRef = factory.create(GROUPING_ID, Type.BIGINT, false);
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();

        // step1: build repeat operator
        // used to record the group by columns
        List<ColumnRefOperator> groupByColumnRefs = new ArrayList<>();
        // used to record the complete grouping_id, which contains all the group by columns and grouping_id value.
        Map<ColumnRefSet, Long> groupingIdMap = new LinkedHashMap<>();
        OptExpression repeatOpt = buildRepeatOpt(input, groupingIdRef, groupByColumnRefs, groupingIdMap, context);

        // step2: build all grouping aggregate operator
        // used to record the column ref with new project ref
        Map<ColumnRefOperator, ColumnRefOperator> columnRefAggrMap = new LinkedHashMap<>();
        OptExpression groupOpt = buildGroupingAggregateOpt(aggregate, repeatOpt, groupByColumnRefs, columnRefAggrMap, context);

        // step3: build project with grouping_id filter
        Map<ColumnRefOperator, ColumnRefOperator> columnRefProjectMap = new LinkedHashMap<>();
        OptExpression projectOpt = buildProjectOpt(aggregate, groupOpt, groupingIdRef, groupByColumnRefs, groupingIdMap,
                columnRefAggrMap, columnRefProjectMap, context);

        // step4: build final aggregate operator & project mapping with parents
        return buildFinalAggregateOpt(aggregate, projectOpt, columnRefProjectMap, context);
    }

    //
    private OptExpression buildRepeatOpt(OptExpression input,
                                         ColumnRefOperator groupingIdRef,
                                         List<ColumnRefOperator> groupByColumnRefs,
                                         Map<ColumnRefSet, Long> groupingIdMap,
                                         OptimizerContext context) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();

        // used to record the column reference that needs to be repeatedly calculated.
        Map<ColumnRefSet, List<ColumnRefOperator>> repeatColumnRefMap = new LinkedHashMap<>();
        // used to record the output column of repeatOperator, this column only represents the generated grouping_id column
        List<ColumnRefOperator> repeatOutput = new ArrayList<>();
        // used to record the complete grouping_id, which contains all the group by columns.
        List<List<Long>> groupingIds = new ArrayList<>();

        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        ColumnRefSet groupByKeys = new ColumnRefSet();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggrEntry : aggregate.getAggregations().entrySet()) {
            ColumnRefOperator column = aggrEntry.getKey();
            CallOperator call = aggrEntry.getValue();

            // ColumnRefSet can automatically deduplicate
            ColumnRefSet usedColumns = call.isDistinct() ? call.getUsedColumns() : new ColumnRefSet();
            usedColumns.union(groupingKeys);
            if (repeatColumnRefMap.containsKey(usedColumns)) {
                continue;
            }
            groupByKeys.union(usedColumns);
            // Get grouping sets repeat groups
            List<ColumnRefOperator> requiredColumns = Arrays.stream(usedColumns.getColumnIds())
                    .mapToObj(factory::getColumnRef)
                    .collect(Collectors.toList());
            repeatColumnRefMap.put(usedColumns, requiredColumns);
        }
        // update groupByColumnRefs order by column index
        Arrays.stream(groupByKeys.getColumnIds()).mapToObj(factory::getColumnRef)
                .forEach(groupByColumnRefs::add);

        int groupingSize = groupByColumnRefs.size();
        for (Map.Entry<ColumnRefSet, List<ColumnRefOperator>> entry : repeatColumnRefMap.entrySet()) {
            ColumnRefSet usedColumns = entry.getKey();
            List<ColumnRefOperator> groupings = entry.getValue();
            BitSet groupingIdBitSet = new BitSet(groupingSize);
            // if groupByColumnRefs size = 3, (colum1, column2, column3), then init with binary value (111)
            groupingIdBitSet.set(0, groupingSize, true);
            // if group by (colum1, column2), then update binary value (0,0,1)
            groupings.stream()
                    .filter(groupByColumnRefs::contains)
                    .forEach(key -> groupingIdBitSet.set(groupByColumnRefs.indexOf(key), false));
            long gid = Utils.convertBitSetToLong(groupingIdBitSet, groupingSize);
            assert !groupingIdMap.containsKey(usedColumns);
            groupingIdMap.put(usedColumns, gid);
        }

        // add grouping_id to groupByColumnRefs
        groupByColumnRefs.add(groupingIdRef);
        repeatOutput.add(groupingIdRef);
        groupingIds.add(new ArrayList<>(groupingIdMap.values()));
        LogicalRepeatOperator repeatOperator =
                new LogicalRepeatOperator(repeatOutput, new ArrayList<>(repeatColumnRefMap.values()), groupingIds);
        return OptExpression.create(repeatOperator, input.getInputs());
    }

    private OptExpression buildGroupingAggregateOpt(LogicalAggregationOperator aggregate,
                                                    OptExpression input,
                                                    List<ColumnRefOperator> groupByColumnRefs,
                                                    Map<ColumnRefOperator, ColumnRefOperator> columnRefAggrMap,
                                                    OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        Map<ColumnRefOperator, CallOperator> aggregations = new LinkedHashMap<>();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggrEntry : aggregate.getAggregations().entrySet()) {
            ColumnRefOperator column = aggrEntry.getKey();
            CallOperator call = aggrEntry.getValue();
            if (!call.isDistinct()) {
                ColumnRefOperator newColumnRef = columnRefFactory.create(column.getName(), column.getType(), column.isNullable());
                aggregations.put(newColumnRef, call);
                // add non-distinct aggregate project mapping
                columnRefAggrMap.put(column, newColumnRef);
            }
        }
        LogicalAggregationOperator groupingAggr =
                new LogicalAggregationOperator(AggType.GLOBAL, groupByColumnRefs, aggregations);
        return OptExpression.create(groupingAggr, Lists.newArrayList(input));
    }

    private OptExpression buildProjectOpt(LogicalAggregationOperator aggregate,
                                          OptExpression input,
                                          ColumnRefOperator groupingIdRef,
                                          List<ColumnRefOperator> groupByColumnRefs,
                                          Map<ColumnRefSet, Long> groupingIdMap,
                                          Map<ColumnRefOperator, ColumnRefOperator> columnRefAggrMap,
                                          Map<ColumnRefOperator, ColumnRefOperator> columnRefProjectMap,
                                          OptimizerContext context) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        Map<ColumnRefOperator, ScalarOperator> baseColumnRefMap = new LinkedHashMap<>();
        // add group by columns
        groupByColumnRefs.forEach(column -> baseColumnRefMap.put(column, column));
        // add non-distinct aggregate columns
        columnRefAggrMap.values().forEach(column -> baseColumnRefMap.put(column, column));

        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggrEntry : aggregate.getAggregations().entrySet()) {
            ColumnRefOperator column = aggrEntry.getKey();
            CallOperator call = aggrEntry.getValue();
            // if non-distinct aggregate, use groupingKeys set
            ColumnRefSet usedColumns = call.isDistinct() ? call.getUsedColumns() : new ColumnRefSet();
            usedColumns.union(groupingKeys);
            Long groupingId = groupingIdMap.get(usedColumns);
            assert groupingId != null;
            BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                    BinaryType.EQ,
                    groupingIdRef,
                    ConstantOperator.createBigint(groupingId));

            assert call.getArguments().size() == 1;
            ScalarOperator aggrColumn = call.isDistinct() ? call.getArguments().get(0) : columnRefAggrMap.get(column);
            final Type columnType = aggrColumn.getType();
            List<ScalarOperator> filterArgs = Lists.newArrayList(predicate, aggrColumn, ConstantOperator.NULL);
            // ifFunction filter grouping_id
            Function ifFunc = Expr.getBuiltinFunction(FunctionSet.IF, new Type[] {Type.BOOLEAN, columnType, columnType},
                    IS_SUPERTYPE_OF).copy();
            ifFunc.setRetType(columnType);
            CallOperator ifOperator = new CallOperator(FunctionSet.IF, columnType, filterArgs, ifFunc);
            ColumnRefOperator newColumnRef = factory.create(ifOperator, columnType, true);
            baseColumnRefMap.put(newColumnRef, ifOperator);
            columnRefProjectMap.put(column, newColumnRef);
        }
        return OptExpression.create(new LogicalProjectOperator(baseColumnRefMap), Lists.newArrayList(input));
    }

    private OptExpression buildFinalAggregateOpt(LogicalAggregationOperator aggregate,
                                                 OptExpression input,
                                                 Map<ColumnRefOperator, ColumnRefOperator> columnRefProjectMap,
                                                 OptimizerContext context) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> finalColumnRefMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggrEntry : aggregate.getAggregations().entrySet()) {
            ColumnRefOperator column = aggrEntry.getKey();
            CallOperator call = aggrEntry.getValue();

            ColumnRefOperator newColumnRef = columnRefProjectMap.get(column);
            assert newColumnRef != null;

            Function func;
            if (!call.isDistinct()) {
                func = Expr.getBuiltinFunction(FunctionSet.MIN, new Type[] {newColumnRef.getType()}, IS_SUPERTYPE_OF).copy();
            } else {
                func = call.getFunction().copy();
            }
            // update function args type and return type, ensure TypeChecker can pass through
            func.setArgsType(new Type[] {newColumnRef.getType()});
            func.setRetType(call.getType());

            // final aggregate with grouping datas
            CallOperator newCall =
                    new CallOperator(func.functionName(), call.getType(), Lists.newArrayList(newColumnRef), func, false);
            ColumnRefOperator newColumn = factory.create(column.getName(), column.getType(), column.isNullable());
            aggregations.put(newColumn, newCall);
            finalColumnRefMap.put(column, newColumn);
        }
        LogicalAggregationOperator finalAggr =
                new LogicalAggregationOperator(AggType.GLOBAL, aggregate.getGroupingKeys(), aggregations);
        OptExpression aggregateOpt = OptExpression.create(finalAggr, Lists.newArrayList(input));
        // return project mapping with parent node
        return OptExpression.create(new LogicalProjectOperator(finalColumnRefMap), Lists.newArrayList(aggregateOpt));
    }

    private boolean enableTableScans(OptExpression tree, OptimizerContext context) {
        if (context.getSessionVariable().isEnableGroupingSetsOlap()) {
            return true;
        }
        List<LogicalOperator> scans = Lists.newArrayList();
        Utils.extractOperator(tree, scans, op -> op instanceof LogicalOlapScanOperator);
        return scans.isEmpty();
    }
}
