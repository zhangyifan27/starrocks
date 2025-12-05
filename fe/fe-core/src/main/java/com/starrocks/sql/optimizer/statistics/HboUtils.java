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

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.planner.PlanNodeAndHash;
import com.starrocks.proto.NodeExecStatsItemPB;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.hbo.InputTableStatisticsInfo;
import com.starrocks.sql.optimizer.statistics.hbo.PlanStatistics;
import com.starrocks.sql.optimizer.statistics.hbo.PlanStatisticsMatchStrategy;
import com.starrocks.sql.optimizer.statistics.hbo.PlanStatisticsWithInputInfo;
import com.starrocks.sql.optimizer.statistics.hbo.RecentRunsPlanStatistics;
import com.starrocks.sql.optimizer.statistics.hbo.RecentRunsPlanStatisticsEntry;
import com.starrocks.sql.optimizer.statistics.hbo.ScanPlanStatistics;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Hbo utils.
 */
public class HboUtils {
    private static final Logger LOG = LogManager.getLogger(HboUtils.class);

    /**
     * Get accurate stats index
     * @param recentRunsPlanStatistics recentRunsPlanStatistics
     * @param inputTableStatistics inputTableStatistics
     * @param rfSafeThreshold rfSafeThreshold
     * @param isEnableHboNonStrictMatchingMode isEnableHboNonStrictMatchingMode
     * @param strategy match strategy
     * @return accurate stats index
     */
    public static Optional<Integer> getAccurateStatsIndex(
            RecentRunsPlanStatistics recentRunsPlanStatistics,
            List<PlanStatistics> inputTableStatistics, double rfSafeThreshold,
            boolean isEnableHboNonStrictMatchingMode, PlanStatisticsMatchStrategy strategy) {
        List<RecentRunsPlanStatisticsEntry> recentRunsStatistics = recentRunsPlanStatistics.getRecentRunsStatistics();
        if (recentRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int recentRunsIndex = 0; recentRunsIndex < recentRunsStatistics.size(); ++recentRunsIndex) {
            if (inputTableStatistics.size() != recentRunsStatistics.get(recentRunsIndex)
                    .getInputTableStatistics().size()) {
                continue;
            }
            boolean accurateMatch = true;
            for (int inputTableIndex = 0; accurateMatch
                    && inputTableIndex < inputTableStatistics.size(); ++inputTableIndex) {
                ScanPlanStatistics curInputStatistics = (ScanPlanStatistics) inputTableStatistics.get(inputTableIndex);
                ScanPlanStatistics historicalInputStatistics =
                        (ScanPlanStatistics) recentRunsStatistics.get(recentRunsIndex)
                                .getInputTableStatistics().get(inputTableIndex);
                boolean isRFSafe = historicalInputStatistics.isRuntimeFilterSafeNode(rfSafeThreshold);
                if (!isRFSafe) {
                    accurateMatch = false;
                } else if (!curInputStatistics.isPartitionedTable()
                        && !historicalInputStatistics.isPartitionedTable()) {
                    accurateMatch = canAccurateMatchForNonPartitionTable(curInputStatistics,
                            historicalInputStatistics, strategy);
                } else if (curInputStatistics.isPartitionedTable()
                        && historicalInputStatistics.isPartitionedTable()) {
                    // find the first full matching entry in recentRunEntries
                    accurateMatch = canAccurateMatchForPartitionTable(curInputStatistics,
                            historicalInputStatistics, isEnableHboNonStrictMatchingMode, strategy);
                } else {
                    throw new RuntimeException("unexpected state during hbo input table stats matching");
                }
            }
            if (accurateMatch) {
                return Optional.of(recentRunsIndex);
            }
        }
        return Optional.empty();
    }

    /**
     * Can accurate match for Non-Partition table
     * @param currentInputStatistics currentInputStatistics
     * @param recentRunsInputStatistics recentRunsInputStatistics
     * @return can accurate match for Non-Partition table.
     */
    public static boolean canAccurateMatchForNonPartitionTable(ScanPlanStatistics currentInputStatistics,
                                                               ScanPlanStatistics recentRunsInputStatistics,
                                                               PlanStatisticsMatchStrategy strategy) {
        boolean hasSameOtherPredicate = currentInputStatistics.hasSameOtherPredicates(recentRunsInputStatistics);
        if (strategy.equals(PlanStatisticsMatchStrategy.OTHER_ONLY_MATCH)
                || strategy.equals(PlanStatisticsMatchStrategy.FULL_MATCH)
                || strategy.equals(PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH)) {
            return hasSameOtherPredicate;
        } else {
            return false;
        }
    }

    /**
     * Can accurate match for partition table
     * @param currentInputStatistics currentInputStatistics
     * @param recentRunsInputStatistics historicalInputStatistics
     * @param strategy strategy
     * @return can accurate match for partition table
     */
    public static boolean canAccurateMatchForPartitionTable(ScanPlanStatistics currentInputStatistics,
            ScanPlanStatistics recentRunsInputStatistics, boolean isEnableHboNonStrictMatchingMode,
            PlanStatisticsMatchStrategy strategy) {
        // For partition table, must ensure
        // 1. the pruned partition is the same
        // 2. partition column predicate is the exactly same(for single value partition it is not special,
        // but for range partition, although the select partition id is the same, but the partition column
        // filter may not be the same, which may have impact on the hbo cache matching)
        // 3. the other predicate with the constant is the same
        boolean hasSamePartition = currentInputStatistics.hasSamePartitionId(recentRunsInputStatistics);
        boolean hasSamePartitionColumnPredicate = currentInputStatistics
                .hasSamePartitionColumnPredicates(recentRunsInputStatistics);
        boolean hasSameOtherPredicate = currentInputStatistics.hasSameOtherPredicates(recentRunsInputStatistics);
        //boolean hasSimilarStats = true;
        //hasSimilarStats = similarStats(currentInputStatistics.getOutputRows(),
        // recentRunsInputStatistics.getOutputRows(), rowThreshold);
        if (strategy.equals(PlanStatisticsMatchStrategy.FULL_MATCH)) {
            return hasSamePartition && hasSamePartitionColumnPredicate && hasSameOtherPredicate;
        } else if (strategy.equals(PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH)) {
            return hasSamePartition && hasSameOtherPredicate/* && hasSimilarStats*/;
        } else if (isEnableHboNonStrictMatchingMode
                && strategy.equals(PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH)) {
            return hasSamePartition;
        //} else if (needMatchSelectedPartition && needMatchPartitionColumnPredicate && !needMatchOtherPredicate) {
        //    return hasSamePartition && hasSamePartitionColumnPredicate && hasSimilarStats;
        //} else if (needMatchSelectedPartition && !needMatchPartitionColumnPredicate && !needMatchOtherPredicate) {
        //    return hasSamePartition && hasSimilarStats;
        } else {
            return false;
        }
    }

    /**
     * Get similar stats index
     * @param recentRunsPlanStatistics recentRunsPlanStatistics
     * @param inputTableStatistics inputTableStatistics
     * @param rowThreshold rowThreshold
     * @param hboRfSafeThreshold hboRfSafeThreshold
     * @return similar StatsIndex
     */
    public static Optional<Integer> getSimilarStatsIndex(
            RecentRunsPlanStatistics recentRunsPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double rowThreshold, double hboRfSafeThreshold) {
        List<RecentRunsPlanStatisticsEntry> recentRunsStatistics = recentRunsPlanStatistics.getRecentRunsStatistics();
        if (recentRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int recentRunsIndex = 0; recentRunsIndex < recentRunsStatistics.size(); ++recentRunsIndex) {
            if (inputTableStatistics.size() != recentRunsStatistics.get(recentRunsIndex)
                    .getInputTableStatistics().size()) {
                continue;
            }
            boolean rowSimilarity = true;
            for (int inputTablesIndex = 0; rowSimilarity
                    && inputTablesIndex < inputTableStatistics.size(); ++inputTablesIndex) {
                PlanStatistics currentInputStatistics = inputTableStatistics.get(inputTablesIndex);
                PlanStatistics historicalInputStatistics = recentRunsStatistics.get(recentRunsIndex)
                        .getInputTableStatistics().get(inputTablesIndex);
                // check if rf safe
                boolean isRFSafe = historicalInputStatistics.isRuntimeFilterSafeNode(hboRfSafeThreshold);
                if (!isRFSafe) {
                    rowSimilarity = false;
                } else {
                    rowSimilarity = similarStats(currentInputStatistics.getPullRows(),
                            historicalInputStatistics.getPullRows(), rowThreshold);
                }
            }
            if (rowSimilarity) {
                return Optional.of(recentRunsIndex);
            }
        }
        return Optional.empty();
    }

    /**
     * similarStats
     * @param stats1 stats1
     * @param stats2 stats2
     * @param threshold threshold
     * @return similar Stats
     */
    public static boolean similarStats(double stats1, double stats2, double threshold) {
        if (Double.isNaN(stats1) && Double.isNaN(stats2)) {
            return true;
        }
        return stats1 >= (1 - threshold) * stats2 && stats1 <= (1 + threshold) * stats2;
    }

    /**
     * Get plan fingerprint hash value
     * @param planFingerprint plan fingerprint
     * @return plan fingerprint hash value
     */
    public static String getPlanFingerprintHash(String planFingerprint) {
        return Hashing.sha256().hashString(planFingerprint, StandardCharsets.UTF_8).toString();
    }

    /**
     * Get matched hbo plan stats. entry.
     * @param recentHboPlanStatistics recentHboPlanStatistics
     * @param inputTableStatistics inputTableStatistics
     * @param hboRfSafeThreshold hboRfSafeThreshold
     * @param isEnableHboNonStrictMatchingMode isEnableHboNonStrictMatchingMode
     * @return matched hbo plan stats. entry.
     */
    public static Optional<RecentRunsPlanStatisticsEntry> getMatchedHboPlanStatisticsEntry(
            RecentRunsPlanStatistics recentHboPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double hboRfSafeThreshold,
            boolean isEnableHboNonStrictMatchingMode) {
        List<RecentRunsPlanStatisticsEntry> recentRunsStatistics = recentHboPlanStatistics.getRecentRunsStatistics();
        if (recentRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        // TODO: if only non-partition table exists, the following steps may be redundant.
        // MATCH 1: PlanStatisticsMatchStrategy.FULL_MATCH
        Optional<Integer> accurateStatsIndex = HboUtils.getAccurateStatsIndex(
                recentHboPlanStatistics, inputTableStatistics, hboRfSafeThreshold,
                isEnableHboNonStrictMatchingMode, PlanStatisticsMatchStrategy.FULL_MATCH);
        if (accurateStatsIndex.isPresent()) {
            return Optional.of(recentRunsStatistics.get(accurateStatsIndex.get()));
        }

        // MATCH 2: PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH
        Optional<Integer> accurateStatsMatchPartitionIdAndOtherPredicateIndex = HboUtils.getAccurateStatsIndex(
                recentHboPlanStatistics, inputTableStatistics, hboRfSafeThreshold,
                isEnableHboNonStrictMatchingMode, PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH);
        if (accurateStatsMatchPartitionIdAndOtherPredicateIndex.isPresent()) {
            return Optional.of(recentRunsStatistics.get(accurateStatsMatchPartitionIdAndOtherPredicateIndex.get()));
        }

        // MATCH 3: PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH
        if (isEnableHboNonStrictMatchingMode) {
            Optional<Integer> accurateStatsOnlyMatchPartitionIdIndex
                    = HboUtils.getAccurateStatsIndex(
                    recentHboPlanStatistics, inputTableStatistics, hboRfSafeThreshold,
                    true, PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH);
            if (accurateStatsOnlyMatchPartitionIdIndex.isPresent()) {
                return Optional.of(recentRunsStatistics.get(accurateStatsOnlyMatchPartitionIdIndex.get()));
            }

            // MATCH 4: TODO: this option is actually useless since the inputTableStatistics is not exactly
            // the current input, but actually a mocked one with the non-current row count.
            //Optional<Integer> similarStatsIndex = HboUtils.getSimilarStatsIndex(
            //        recentHboPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold);
            //if (similarStatsIndex.isPresent()) {
            //    return Optional.of(recentRunsStatistics.get(similarStatsIndex.get()));
            //}
        }
        return Optional.empty();
    }

    /**
     * getOperatorHash
     * @param planNode planNode
     * @return planNode And Hash
     */
    public static PlanNodeAndHash getOperatorHash(OptExpression planNode) {
        String planFingerprint;
        String planHash;
        if (planNode.getOp() instanceof PhysicalOperator) {
            planFingerprint = planNode.getPlanTreeFingerprint();
            planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        } else if (planNode.getOp() instanceof LogicalOperator) {
            planFingerprint = planNode.getPlanTreeFingerprint();
            planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        } else {
            throw new IllegalStateException("hbo get neither physical plan nor logical plan");
        }
        return new PlanNodeAndHash(planNode.getOp(), Optional.of(planHash));
    }

    public static PlanNodeAndHash getOperatorHash(GroupExpression groupExpression) {
        String planFingerprint = groupExpression.getPlanTreeFingerprint();
        String planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        return new PlanNodeAndHash(groupExpression.getOp(), Optional.of(planHash));
    }

    public static PlanNodeAndHash getOperatorHash(Operator planNode) {
        String planFingerprint;
        String planHash;
        if (planNode instanceof PhysicalOperator) {
            planFingerprint = planNode.getFingerprint();
            planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        } else if (planNode instanceof LogicalOperator) {
            planFingerprint = planNode.getFingerprint();
            planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        } else {
            throw new IllegalStateException("hbo get neither physical plan nor logical plan");
        }
        return new PlanNodeAndHash(planNode, Optional.of(planHash));
    }

    private static Optional<List<PlanStatistics>> getFilterAdjustedInputTableStatistics(
            List<PlanStatistics> currentInputTableStatistics, OptimizerContext optimizerContext) {
        ImmutableList.Builder<PlanStatistics> outputTableStatisticsBuilder = ImmutableList.builder();
        HboPlanStatisticsManager hboManager = GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager();
        HboPlanInfoProvider planInfoProvider = hboManager.getHboPlanInfoProvider();

        try {
            if (planInfoProvider != null && optimizerContext.getQueryId() != null) {
                String queryId = DebugUtil.printId(optimizerContext.getQueryId());
                ConcurrentHashMap<String, ScalarOperator> scanToFilterMap = planInfoProvider.getScanToFilterMap(queryId);
                // here allows scanToFilterMap is empty when no filter on the scan and upper plan node
                // can also reuse the recent run's plan stats. info.
                for (PlanStatistics inputTableStatistics : currentInputTableStatistics) {
                    PhysicalScanOperator tableScan = ((ScanPlanStatistics) inputTableStatistics).getScan();
                    String key = Utils.getQualifiedTableKey(tableScan.getTable());
                    ScalarOperator tableFilterSet = scanToFilterMap.get(key);
                    List<Long> selectedPartitionIds;
                    if (tableScan instanceof PhysicalOlapScanOperator) {
                        selectedPartitionIds = ((PhysicalOlapScanOperator) tableScan).getSelectedPartitionId();
                    } else {
                        selectedPartitionIds = tableScan.getScanOperatorPredicates().getSelectedPartitionIds()
                                .stream().collect(Collectors.toList());
                    }
                    ScanPlanStatistics newInputPlanStatistics = new ScanPlanStatistics(inputTableStatistics, tableScan,
                            tableFilterSet, tableScan.getTable().isPartitioned(),
                            tableScan.getTable().getPartitionColumns(), selectedPartitionIds);
                    outputTableStatisticsBuilder.add(newInputPlanStatistics);
                }
            }
        } catch (AnalysisException e) {
            LOG.info("failed to get selected partition id {}", e.toString());
        }
        return Optional.of(outputTableStatisticsBuilder.build());
    }

    /**
     * getMatchedPlanStatistics
     * @param planStatistics planStatistics
     * @param connectContext connectContext
     * @return planStatistics
     */
    public static PlanStatistics getMatchedPlanStatistics(RecentRunsPlanStatistics planStatistics,
            OptimizerContext optimizerContext) {
        PlanStatistics matchedPlanStatistics = null;
        // NOTE: get current inputTableStatistics is difficult, consider the case:
        // select ... from t where c1 = 1 followed by select ... from t where c1 = 1 and c2 = 2
        // since the input table t will have two entries in recentRunEntries list,
        // if the getOperatorInputTableStatistics only find the table entry by the table name,
        // it may find the wrong entry for the different filter pattern. By contract, use current planStatistics
        // is relative safe because the plan hash has ensured the plan pattern, such as partition number,
        // filter pattern are matched, although the constant in filter may not be same.
        // After considering the above situation, current solution is as following:
        // firstly find an initial entry, e.g, the latest entry, of currentInputTablesStatistics.
        // then adjust the initial info based on the filter info.
        if (!planStatistics.getRecentRunsStatistics().isEmpty()) {
            // use the latest entry as initial entry, which will be updated with the filter info.
            int initialIndex = planStatistics.getRecentRunsStatistics().size() - 1;
            List<PlanStatistics> initialInputTableStatistics = planStatistics.getRecentRunsStatistics()
                    .get(initialIndex).getInputTableStatistics();
            if (!initialInputTableStatistics.isEmpty()) {
                Optional<List<PlanStatistics>> inputTableStatistics = getFilterAdjustedInputTableStatistics(
                        initialInputTableStatistics, optimizerContext);
                if (inputTableStatistics.isPresent()) {
                    double rfsafeThreshold = -1.0;
                    //double rowCountMatchingThreshold = 0.1;
                    boolean isEnableHboNonStrictMatchingMode = false;
                    if (optimizerContext != null && optimizerContext.getSessionVariable() != null) {
                        rfsafeThreshold = optimizerContext.getSessionVariable().getHboRfSafeThreshold();
                        //rowCountMatchingThreshold = connectContext.getSessionVariable().getHboRowMatchingThreshold();
                        isEnableHboNonStrictMatchingMode = optimizerContext.getSessionVariable()
                                .isEnableHboNonStrictMatchingMode();
                    }
                    Optional<RecentRunsPlanStatisticsEntry> recentRunsPlanStatisticsEntry
                            = HboUtils.getMatchedHboPlanStatisticsEntry(
                                    planStatistics, inputTableStatistics.get(),
                                    rfsafeThreshold, isEnableHboNonStrictMatchingMode);
                    if (recentRunsPlanStatisticsEntry.isPresent()) {
                        matchedPlanStatistics = recentRunsPlanStatisticsEntry.get().getPlanStatistics();
                    }
                }
            }
        }
        return matchedPlanStatistics;
    }

    private static PlanStatistics generateScanPlanStatistics(int nodeId,
            List<NodeExecStatsItemPB> planNodeRuntimeStats,
            PhysicalScanOperator scan, Map<String, ScalarOperator> scanToFilterMap) {
        for (NodeExecStatsItemPB item : planNodeRuntimeStats) {
            if (item.nodeId == nodeId) {
                return PlanStatistics.buildFromStatsItem(item, scan, scanToFilterMap);
            }
        }
        return PlanStatistics.EMPTY;
    }

    public static void collectScanList(OptExpression root, List<PhysicalScanOperator> scans) {
        if (root.getOp() instanceof PhysicalScanOperator) {
            scans.add((PhysicalScanOperator) root.getOp());
        } else {
            for (Object child : root.getInputs()) {
                collectScanList((OptExpression) child, scans);
            }
        }
    }

    public static void collectScanQualifierList(OptExpression root, List<String> scans) {
        if (root.getOp() instanceof PhysicalScanOperator) {
            scans.add(Utils.getQualifiedTableKey(((PhysicalScanOperator) root.getOp()).getTable()));
        } else if (root.getOp() instanceof LogicalScanOperator) {
            scans.add(Utils.getQualifiedTableKey(((LogicalScanOperator) root.getOp()).getTable()));
        } else {
            for (Object child : root.getInputs()) {
                collectScanQualifierList((OptExpression) child, scans);
            }
        }
    }

    public static void collectScanQualifierList(GroupExpression groupExpression, List<String> scans) {
        if (groupExpression.getOp() instanceof LogicalScanOperator) {
            String tableName = ((LogicalScanOperator) groupExpression.getOp()).getTable().getName();
            scans.add(tableName);
        } else if (groupExpression.getOp() instanceof PhysicalScanOperator) {
            String tableName = ((PhysicalScanOperator) groupExpression.getOp()).getTable().getName();
            scans.add(tableName);
        } else {
            for (Object child : groupExpression.getInputs()) {
                if (((Group) child).getLogicalExpressions() != null &&
                        !((Group) child).getLogicalExpressions().isEmpty()) {
                    GroupExpression childGroupExpression = ((Group) child).getLogicalExpressions().get(0);
                    collectScanQualifierList(childGroupExpression, scans);
                } else if (((Group) child).getPhysicalExpressions() != null &&
                        !((Group) child).getPhysicalExpressions().isEmpty()) {
                    GroupExpression childGroupExpression = ((Group) child).getPhysicalExpressions().get(0);
                    collectScanQualifierList(childGroupExpression, scans);
                }
            }
        }
    }

    private static InputTableStatisticsInfo buildHboInputTableStatisticsInfo(OptExpression root,
            Map<PhysicalOperator, Integer> planToIdMap, Map<String, ScalarOperator> scanToFilterMap,
            List<NodeExecStatsItemPB> statsItem) {
        String planFingerprint = root.getPlanTreeFingerprint();
        String planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        ImmutableList.Builder<PlanStatistics> inputTableStatisticsBuilder = ImmutableList.builder();
        List<PhysicalScanOperator> scans = new ArrayList<>();
        collectScanList(root, scans);
        for (PhysicalScanOperator scan : scans) {
            Integer nodeId = planToIdMap.get(scan);
            if (nodeId != null) {
                PlanStatistics planStatistics = generateScanPlanStatistics(
                        nodeId, statsItem, scan, scanToFilterMap);
                if (planStatistics != null && !planStatistics.equals(PlanStatistics.EMPTY)) {
                    inputTableStatisticsBuilder.add(planStatistics);
                }
            }
        }
        return new InputTableStatisticsInfo(Optional.of(planHash), Optional.of(inputTableStatisticsBuilder.build()));
    }

    /**
     * Generate plan statistics map.
     * @param idToPlanMap idToPlanMap
     * @param planToIdMap planToIdMap
     * @param scanToFilterMap scanToFilterMap
     * @param curOperatorRuntimeStats curOperatorRuntimeStats
     * @return plan statistics map
     */
    public static Map<PlanNodeAndHash, PlanStatisticsWithInputInfo> genPlanStatisticsMap(
            ExecPlan execPlan, Map<Integer, PhysicalOperator> idToPlanMap, Map<PhysicalOperator, Integer> planToIdMap,
            Map<String, ScalarOperator> scanToFilterMap,
            List<NodeExecStatsItemPB> curOperatorRuntimeStats) {
        Map<PlanNodeAndHash, PlanStatisticsWithInputInfo> outputPlanStatisticsMap = new HashMap<>();
        for (NodeExecStatsItemPB nodeStats : curOperatorRuntimeStats) {
            int nodeId = nodeStats.nodeId;
            PhysicalOperator planNode = idToPlanMap.get(nodeId);
            OptExpression optExpression = execPlan.getOptExpression(nodeId);
            if (planNode != null && optExpression != null) {
                PlanStatistics curPlanStatistics = PlanStatistics.buildFromStatsItem(
                        nodeStats, planNode, scanToFilterMap);
                InputTableStatisticsInfo inputTableStatisticsInfo = buildHboInputTableStatisticsInfo(
                        optExpression, planToIdMap, scanToFilterMap, curOperatorRuntimeStats);
                Optional<String> hash = inputTableStatisticsInfo.getHash();
                PlanNodeAndHash planNodeAndHash = new PlanNodeAndHash((Operator) planNode, hash);
                PlanStatisticsWithInputInfo planHashWithInputInfo = new PlanStatisticsWithInputInfo(
                        nodeId, curPlanStatistics, inputTableStatisticsInfo);
                outputPlanStatisticsMap.put(planNodeAndHash, planHashWithInputInfo);
            }
        }
        return outputPlanStatisticsMap;
    }

    public static void collectPredicateOnScan(OptExpression optExression, OptimizerContext optimizerContext) {
        if (!GlobalVariable.isEnableHboInfoCollection()) {
            return;
        }
        for (OptExpression child : optExression.getInputs()) {
            collectPredicateOnScan(child, optimizerContext);
        }
        if (optExression.getOp() instanceof LogicalHiveScanOperator
                || optExression.getOp() instanceof LogicalIcebergScanOperator
                || optExression.getOp() instanceof LogicalOlapScanOperator) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) optExression.getOp();
            if (scanOperator != null && scanOperator.getPredicate() != null) {
                ScalarOperator filterOp = scanOperator.getPredicate();
                String queryId = DebugUtil.printId(optimizerContext.getQueryId());
                ConcurrentHashMap<String, ScalarOperator> scanToFilterMap = GlobalStateMgr.getCurrentState()
                        .getHboPlanStatisticsManager().getHboPlanInfoProvider().getScanToFilterMap(queryId);
                if (scanToFilterMap.isEmpty()) {
                    GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager()
                            .getHboPlanInfoProvider().putScanToFilterMap(queryId, scanToFilterMap);
                }
                String tableKey = Utils.getQualifiedTableKey(scanOperator.getTable());
                scanToFilterMap.put(tableKey, filterOp);
            }
        }
    }
}
