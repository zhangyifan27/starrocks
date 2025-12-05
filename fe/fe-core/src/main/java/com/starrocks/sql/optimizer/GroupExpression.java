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


package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.sql.common.DebugOperatorTracer;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.OutputPropertyGroup;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.HboUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A group-expression is the same as an expression except
 * it takes groups as inputs while expressions take other expressions as inputs.
 * <p>
 * With group-expressions, a group can be re-defined as
 * a set of logically equivalent group-expressions.
 * <p>
 * With group-expressions, we could reduces
 * the number of transformations, storage overhead, and repeated cost estimations.
 */
public class GroupExpression {
    // The group this group expression belong to,
    // will set by setGroup method
    private Group group;
    private final List<Group> inputs;
    private final Operator op;
    private final BitSet ruleMasks = new BitSet(RuleType.NUM_RULES.ordinal() + 1);
    private final BitSet appliedRuleMasks = new BitSet(RuleType.NUM_RULES.ordinal() + 1);
    private boolean statsDerived = false;
    private final Map<PhysicalPropertySet, Pair<Double, List<PhysicalPropertySet>>> lowestCostTable;
    // required property by parent -> output property
    private final Map<PhysicalPropertySet, PhysicalPropertySet> outputPropertyMap;

    // valid output property groups, only used in enum plan
    private final Set<OutputPropertyGroup> validOutputPropertyGroups;
    // property group -> plan count, only used in enum plan
    private final Map<OutputPropertyGroup, Integer> propertiesPlanCountMap;

    private boolean isUnused = false;

    private Optional<Boolean> isAppliedMVRules = Optional.empty();
    // all mv rewrite rules
    private static final List<Rule> ALL_MV_REWRITE_RULES = RuleSet.getRewriteRulesByType(RuleSetType.ALL_MV_REWRITE);

    public GroupExpression(Operator op, List<Group> inputs) {
        this.op = op;
        this.inputs = inputs;
        this.lowestCostTable = Maps.newHashMap();
        this.validOutputPropertyGroups = Sets.newLinkedHashSet();
        this.propertiesPlanCountMap = Maps.newLinkedHashMap();
        this.outputPropertyMap = Maps.newHashMap();
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public List<Group> getInputs() {
        return inputs;
    }

    public int arity() {
        return inputs.size();
    }

    public Group inputAt(int i) {
        return inputs.get(i);
    }

    public Operator getOp() {
        return op;
    }

    public boolean isStatsDerived() {
        return statsDerived;
    }

    public boolean isUnused() {
        return hasEmptyRootGroup() || hasEmptyChildGroup() || isUnused;
    }

    private boolean hasEmptyChildGroup() {
        return inputs.stream().anyMatch(Group::isEmpty);
    }

    public boolean hasEmptyRootGroup() {
        return group.isEmpty();
    }

    public void setStatsDerived() {
        statsDerived = true;
    }

    public void setRuleExplored(Rule rule) {
        ruleMasks.set(rule.type().ordinal());
    }

    public void setUnused(boolean isUnused) {
        this.isUnused = isUnused;
    }

    public boolean hasRuleExplored(Rule rule) {
        return ruleMasks.get(rule.type().ordinal());
    }

    public BitSet getAppliedRuleMasks() {
        return this.appliedRuleMasks;
    }

    public void mergeAppliedRules(BitSet bitSet) {
        appliedRuleMasks.or(bitSet);
    }

    public void addNewAppliedRule(Rule rule) {
        appliedRuleMasks.set(rule.type().ordinal());
    }

    public boolean hasRuleApplied(Rule rule) {
        return appliedRuleMasks.get(rule.type().ordinal());
    }

    public PhysicalPropertySet getOutputProperty(PhysicalPropertySet requiredPropertySet) {
        PhysicalPropertySet outputProperty = outputPropertyMap.get(requiredPropertySet);
        Preconditions.checkState(outputProperty != null);
        return outputProperty;
    }

    public void setOutputPropertySatisfyRequiredProperty(PhysicalPropertySet outputPropertySet,
                                                         PhysicalPropertySet requiredPropertySet) {
        this.outputPropertyMap.put(requiredPropertySet, outputPropertySet);
    }

    public void addValidOutputPropertyGroup(PhysicalPropertySet outputProperty,
                                            List<PhysicalPropertySet> childrenOutputProperties) {
        validOutputPropertyGroups.add(OutputPropertyGroup.of(outputProperty, childrenOutputProperties));
    }

    public List<OutputPropertyGroup> getChildrenOutputProperties(PhysicalPropertySet outputProperty) {
        List<OutputPropertyGroup> outputPropertyGroups = Lists.newArrayList();
        for (OutputPropertyGroup outputPropertyGroup : validOutputPropertyGroups) {
            if (outputPropertyGroup.getOutputProperty().equals(outputProperty)) {
                outputPropertyGroups.add(outputPropertyGroup);
            }
        }
        return outputPropertyGroups;
    }

    public boolean hasValidSubPlan() {
        return !validOutputPropertyGroups.isEmpty();
    }

    public void addPlanCountOfProperties(OutputPropertyGroup properties, int count) {
        propertiesPlanCountMap.put(properties, count);
    }

    public Map<OutputPropertyGroup, Integer> getPropertiesPlanCountMap(
            PhysicalPropertySet requiredProperty) {
        Map<OutputPropertyGroup, Integer> result = Maps.newLinkedHashMap();
        propertiesPlanCountMap.entrySet().stream()
                .filter(entry -> entry.getKey().getOutputProperty().equals(requiredProperty))
                .forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }

    public int getOutputPropertyPlanCount(PhysicalPropertySet requiredProperty) {
        return propertiesPlanCountMap.entrySet().stream()
                .filter(entry -> entry.getKey().getOutputProperty().equals(requiredProperty))
                .mapToInt(Map.Entry::getValue).sum();
    }

    /**
     * Retrieves the lowest cost satisfying a given set of properties
     *
     * @param require PropertySet that needs to be satisfied
     * @return Lowest cost to satisfy that PropertySet
     */
    public double getCost(PhysicalPropertySet require) {
        Preconditions.checkState(lowestCostTable.containsKey(require));
        return lowestCostTable.get(require).first;
    }

    /**
     * Gets the input properties needed for a given required properties
     *
     * @param require PhysicalPropertySet that needs to be satisfied
     * @return List of children input physical properties required
     */
    public List<PhysicalPropertySet> getInputProperties(PhysicalPropertySet require) {
        Pair<Double, List<PhysicalPropertySet>> lowestInput = lowestCostTable.get(require);
        if (lowestInput == null) {
            String msg = "no best plan with this required property %s for this groupExpression %s";
            throw new IllegalArgumentException(String.format(msg, require, this));
        }
        return lowestInput.second;
    }

    /**
     * Add a mapping from output_properties to
     * a pair of lowest cost and vector of children input properties.
     *
     * @param outputProperties PropertySet that is satisfied
     * @param inputProperties  List of children input properties required
     * @param cost             Cost
     */
    public boolean updatePropertyWithCost(PhysicalPropertySet outputProperties,
                                          List<PhysicalPropertySet> inputProperties,
                                          double cost) {
        if (lowestCostTable.containsKey(outputProperties)) {
            if (lowestCostTable.get(outputProperties).first > cost) {
                lowestCostTable.put(outputProperties, new Pair<>(cost, inputProperties));
                return true;
            }
        } else {
            lowestCostTable.put(outputProperties, new Pair<>(cost, inputProperties));
            return true;
        }
        return false;
    }

    // use other Group Expression to update the lowestCostTable
    public void updatePropertyWithCost(GroupExpression other) {
        for (Map.Entry<PhysicalPropertySet, Pair<Double, List<PhysicalPropertySet>>> entry : other.lowestCostTable
                .entrySet()) {
            updatePropertyWithCost(entry.getKey(), entry.getValue().second, entry.getValue().first);
        }
    }

    // merge other group expression state to this group expression
    public void mergeGroupExpression(GroupExpression other) {
        // 1. low Cost Table
        for (Map.Entry<PhysicalPropertySet, Pair<Double, List<PhysicalPropertySet>>> entry : other.lowestCostTable
                .entrySet()) {
            updatePropertyWithCost(entry.getKey(), entry.getValue().second, entry.getValue().first);
        }
        // 2. outputPropertyMap
        outputPropertyMap.putAll(other.outputPropertyMap);
    }

    // This function will drive input group logical property first,
    // then derive itself's logical property
    public void deriveLogicalPropertyRecursively() {
        for (Group group : inputs) {
            group.getFirstLogicalExpression().deriveLogicalPropertyRecursively();
        }

        deriveLogicalPropertyItself();
    }

    // This function assume the child group logical property has been derived
    public void deriveLogicalPropertyItself() {
        ExpressionContext context = new ExpressionContext(this);
        context.deriveLogicalProperty();
        getGroup().setLogicalProperty(context.getRootProperty());
    }

    public ColumnRefSet getChildOutputColumns(int index) {
        return inputAt(index).getLogicalProperty().getOutputColumns();
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, inputs);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GroupExpression)) {
            return false;
        }
        GroupExpression rhs = (GroupExpression) obj;
        if (this == rhs) {
            return true;
        }
        if (!op.equals(rhs.getOp())) {
            return false;
        }
        if (arity() != rhs.arity()) {
            return false;
        }
        for (int i = 0; i < arity(); ++i) {
            if (inputs.get(i).getId() != (rhs.inputAt(i).getId())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{root group ");
        if (group != null) {
            sb.append(group.getId());
        } else {
            sb.append(-1);
        }
        sb.append(" root operator: ")
                .append(op)
                .append(" child: ");
        sb.append(inputs.stream().map(s -> String.valueOf(s.getId())).collect(Collectors.joining(", ")));
        sb.append("}");
        return sb.toString();
    }

    public String debugString(String headlineIndent, String detailIndent) {
        StringBuilder sb = new StringBuilder();
        sb.append(detailIndent)
                .append(op.accept(new DebugOperatorTracer(), null)).append("\n");
        String childHeadlineIndent = detailIndent + "->  ";
        String childDetailIndent = detailIndent + "    ";
        for (Group input : inputs) {
            sb.append(input.debugString(childHeadlineIndent, childDetailIndent));
        }
        return sb.toString();
    }

    public boolean hasAppliedMVRules() {
        if (isAppliedMVRules.isEmpty()) {
            isAppliedMVRules = Optional.of(ALL_MV_REWRITE_RULES.stream().anyMatch(rule -> hasRuleApplied(rule)));
        }
        return isAppliedMVRules.get();
    }

    public String getPlanTreeFingerprint() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getOp().getFingerprint());
        if (this.getInputs().isEmpty()) {
            return builder.toString();
        } else {
            List<Group> mutableGroupChildren = new ArrayList<>(this.getInputs());
            // the following sorting by scans depended on will increase the plan matching possibility
            // during hbo plan matching stage, e.g, the final physical plan is encoded as 'a join b',
            // but the group plan is 'b join a' which can be matched also.
            // NOTE: it will increase the risk brought from rf, as above, the physical plan 'a join b'
            // is with rf's potential influence which will not exactly the same as 'b join a',
            // but when we ignore the join sides as above and want to increase the hbo plan stats.'s adaptability,
            // it may bring the unsuitable matching and increase the dependence for the rf-safe checking.
            Collections.sort(mutableGroupChildren, new Comparator<Group>() {
                @Override
                public int compare(Group group1, Group group2) {
                    List<String> scanQualifierList1 = new ArrayList<>();
                    List<String> scanQualifierList2 = new ArrayList<>();
                    GroupExpression groupExpression1 = group1.getLogicalExpressions() != null
                            ? group1.getFirstLogicalExpression() : group1.getPhysicalExpressions().get(0);
                    GroupExpression groupExpression2 = group2.getLogicalExpressions() != null
                            ? group2.getFirstLogicalExpression() : group2.getPhysicalExpressions().get(0);
                    HboUtils.collectScanQualifierList(groupExpression1, scanQualifierList1);
                    HboUtils.collectScanQualifierList(groupExpression2, scanQualifierList2);
                    Collections.sort(scanQualifierList1);
                    Collections.sort(scanQualifierList2);
                    String qualifiedName1 = StringUtils.join(scanQualifierList1, "");
                    String qualifiedName2 = StringUtils.join(scanQualifierList2, "");
                    return qualifiedName1.compareTo(qualifiedName2);
                }
            });
            for (Group group : mutableGroupChildren) {
                if (group.getLogicalExpressions() != null) {
                    builder.append(group.getLogicalExpressions().get(0).getPlanTreeFingerprint());
                } else if (group.getPhysicalExpressions() != null) {
                    builder.append(group.getPhysicalExpressions().get(0).getPlanTreeFingerprint());
                }
            }
            return builder.toString();
        }
    }
}
