/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableIntList;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Planner utils for Dynamic partition Pruning. */
public class DynamicPartitionPruningUtils {

    /**
     * For the input join node, judge whether the join left side and join right side meet the
     * requirements of dynamic partition pruning. If meets the requirements,return true.
     */
    public static boolean supportDynamicPartitionPruning(Join join) {
        if (!ShortcutUtils.unwrapContext(join)
                .getTableConfig()
                .get(OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED)) {
            return false;
        }
        // Now dynamic partition pruning supports left/right join, inner and semi join. but now semi
        // join can not join reorder.
        if (join.getJoinType() != JoinRelType.INNER
                && join.getJoinType() != JoinRelType.LEFT
                && join.getJoinType() != JoinRelType.RIGHT) {
            return false;
        }

        JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            return false;
        }
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        // TODO Now fact side and dim side don't support many complex patterns, like join inside
        // fact/dim side, agg inside fact/dim side etc. which will support next.
        ImmutableIntList leftPartitionKeys =
                extractPartitionKeysFromFactSide(left, joinInfo.leftKeys);
        if (!leftPartitionKeys.isEmpty()) {
            boolean rightIsDim =
                    isDimSide(
                            right,
                            getDimSidePartitionKeys(
                                    joinInfo.leftKeys, joinInfo.rightKeys, leftPartitionKeys));
            if (rightIsDim) {
                return true;
            }
        }

        ImmutableIntList rightPartitionKeys =
                extractPartitionKeysFromFactSide(right, joinInfo.rightKeys);
        if (!rightPartitionKeys.isEmpty()) {
            return isDimSide(
                    left,
                    getDimSidePartitionKeys(
                            joinInfo.rightKeys, joinInfo.leftKeys, rightPartitionKeys));
        }
        return false;
    }

    /**
     * Judge whether input RelNode meets the conditions of dimSide. If joinKeys is null means we
     * need not consider the join keys in dim side, which already deal by dynamic partition pruning
     * rule. If joinKeys not null means we need to judge whether joinKeys changed in dim side, if
     * changed, this RelNode is not dim side.
     */
    public static boolean isDimSide(RelNode rel, @Nullable ImmutableIntList joinKeys) {
        DppDimSideFactors dimSideFactors = new DppDimSideFactors();
        visitDimSide(rel, dimSideFactors, joinKeys);
        return dimSideFactors.isDimSide();
    }

    /**
     * Visit dim side to judge whether dim side has filter condition and whether dim side's source
     * table scan is non partitioned scan.
     */
    public static void visitDimSide(
            RelNode rel, DppDimSideFactors dimSideFactors, @Nullable ImmutableIntList joinKeys) {
        // TODO Let visitDimSide more efficient and more accurate. Like a filter on dim table or a
        // filter for the partition field on fact table.
        if (rel instanceof TableScan) {
            TableScan scan = (TableScan) rel;
            TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
            if (table == null) {
                return;
            }
            if (!dimSideFactors.hasFilter
                    && table.abilitySpecs() != null
                    && table.abilitySpecs().length != 0) {
                for (SourceAbilitySpec spec : table.abilitySpecs()) {
                    if (spec instanceof FilterPushDownSpec) {
                        List<RexNode> predicates = ((FilterPushDownSpec) spec).getPredicates();
                        for (RexNode predicate : predicates) {
                            if (isSuitableFilter(predicate)) {
                                dimSideFactors.hasFilter = true;
                            }
                        }
                    }
                }
            }
            CatalogTable catalogTable = table.contextResolvedTable().getTable();
            dimSideFactors.hasNonPartitionedScan = !catalogTable.isPartitioned();
        } else if (rel instanceof HepRelVertex) {
            visitDimSide(((HepRelVertex) rel).getCurrentRel(), dimSideFactors, joinKeys);
        } else if (rel instanceof Exchange) {
            visitDimSide(rel.getInput(0), dimSideFactors, joinKeys);
        } else if (rel instanceof Project) {
            // joinKeys is null means need not consider join keys.
            if (joinKeys != null) {
                List<RexNode> projects = ((Project) rel).getProjects();
                ImmutableIntList inputJoinKeys = getInputIndices(projects, joinKeys);
                if (inputJoinKeys.isEmpty()) {
                    dimSideFactors.isSuitableJoinKey = false;
                }
            }

            visitDimSide(rel.getInput(0), dimSideFactors, joinKeys);
        } else if (rel instanceof Calc) {
            Calc calc = (Calc) rel;
            // joinKeys is null means need not consider join keys.
            if (joinKeys != null) {
                List<RexNode> projects =
                        calc.getProgram().getProjectList().stream()
                                .map(p -> calc.getProgram().expandLocalRef(p))
                                .collect(Collectors.toList());
                ImmutableIntList inputJoinKeys = getInputIndices(projects, joinKeys);
                if (inputJoinKeys.isEmpty()) {
                    dimSideFactors.isSuitableJoinKey = false;
                }
            }

            RexProgram origProgram = ((Calc) rel).getProgram();
            if (origProgram.getCondition() != null
                    && isSuitableFilter(origProgram.expandLocalRef(origProgram.getCondition()))) {
                dimSideFactors.hasFilter = true;
            }
            visitDimSide(rel.getInput(0), dimSideFactors, joinKeys);
        } else if (rel instanceof Filter) {
            if (isSuitableFilter(((Filter) rel).getCondition())) {
                dimSideFactors.hasFilter = true;
            }
            visitDimSide(rel.getInput(0), dimSideFactors, joinKeys);
        }
    }

    /**
     * Not all filter condition suitable for using to filter partitions by dynamic partition pruning
     * rules. For example, NOT NULL can only filter one default partition which have a small impact
     * on filtering data.
     */
    public static boolean isSuitableFilter(RexNode filterCondition) {
        switch (filterCondition.getKind()) {
            case AND:
                List<RexNode> conjunctions = RelOptUtil.conjunctions(filterCondition);
                return isSuitableFilter(conjunctions.get(0))
                        || isSuitableFilter(conjunctions.get(1));
            case OR:
                List<RexNode> disjunctions = RelOptUtil.disjunctions(filterCondition);
                return isSuitableFilter(disjunctions.get(0))
                        && isSuitableFilter(disjunctions.get(1));
            case NOT:
                return isSuitableFilter(((RexCall) filterCondition).operands.get(0));
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case NOT_EQUALS:
            case IN:
            case LIKE:
            case CONTAINS:
            case SEARCH:
            case IS_FALSE:
            case IS_NOT_FALSE:
            case IS_NOT_TRUE:
            case IS_TRUE:
                // TODO adding more suitable filters which can filter enough partitions after using
                // this filter in dynamic partition pruning.
                return true;
            default:
                return false;
        }
    }

    private static ImmutableIntList getDimSidePartitionKeys(
            ImmutableIntList factKeys,
            ImmutableIntList dimKeys,
            ImmutableIntList factPartitionKeys) {
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < factKeys.size(); ++i) {
            int k = factKeys.get(i);
            if (factPartitionKeys.contains(k)) {
                keys.add(dimKeys.get(i));
            }
        }
        return ImmutableIntList.copyOf(keys);
    }

    private static ImmutableIntList getInputIndices(
            List<RexNode> projects, ImmutableIntList joinKeys) {
        List<Integer> indices = new ArrayList<>();
        for (int k : joinKeys) {
            RexNode rexNode = projects.get(k);
            if (rexNode instanceof RexInputRef) {
                indices.add(((RexInputRef) rexNode).getIndex());
            } else {
                return ImmutableIntList.of();
            }
        }
        return ImmutableIntList.copyOf(indices);
    }

    /**
     * For the input RelNode, recursively extract probable partition keys from this node, and return
     * the keys lists, which both in partition keys and join keys.
     */
    private static ImmutableIntList extractPartitionKeysFromFactSide(
            RelNode rel, ImmutableIntList joinKeys) {
        ImmutableIntList partitionKeys = inferPartitionKeysInFactSide(rel);
        if (partitionKeys.isEmpty()) {
            return ImmutableIntList.of();
        }
        List<Integer> keys = new ArrayList<>(joinKeys);
        keys.retainAll(partitionKeys);
        return ImmutableIntList.copyOf(keys);
    }

    /** Recursively extract probable partition keys from the input RelNode. */
    private static ImmutableIntList inferPartitionKeysInFactSide(RelNode rel) {
        if (rel instanceof HepRelVertex) {
            return inferPartitionKeysInFactSide(((HepRelVertex) rel).getCurrentRel());
        } else if (rel instanceof Exchange || rel instanceof Filter) {
            return inferPartitionKeysInFactSide(rel.getInput(0));
        } else if (rel instanceof TableScan) {
            TableScan scan = (TableScan) rel;
            TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
            if (table == null) {
                return ImmutableIntList.of();
            }
            if (!(table.tableSource() instanceof SupportsDynamicFiltering)) {
                return ImmutableIntList.of();
            }
            CatalogTable catalogTable = table.contextResolvedTable().getTable();
            List<String> partitionKeys = catalogTable.getPartitionKeys();
            return ImmutableIntList.of(
                    partitionKeys.stream()
                            .map(i -> scan.getRowType().getFieldNames().indexOf(i))
                            .mapToInt(i -> i)
                            .toArray());
        } else if (rel instanceof Project) {
            ImmutableIntList partitionKeys = inferPartitionKeysInFactSide(rel.getInput(0));
            if (partitionKeys.isEmpty()) {
                return partitionKeys;
            }
            List<RexNode> projects = ((Project) rel).getProjects();
            return getPartitionKeysAfterProject(projects, partitionKeys);
        } else if (rel instanceof Calc) {
            ImmutableIntList partitionKeys = inferPartitionKeysInFactSide(rel.getInput(0));
            if (partitionKeys.isEmpty()) {
                return partitionKeys;
            }
            Calc calc = (Calc) rel;
            List<RexNode> projects =
                    calc.getProgram().getProjectList().stream()
                            .map(p -> calc.getProgram().expandLocalRef(p))
                            .collect(Collectors.toList());
            return getPartitionKeysAfterProject(projects, partitionKeys);
        }
        return ImmutableIntList.of();
    }

    private static ImmutableIntList getPartitionKeysAfterProject(
            List<RexNode> projects, ImmutableIntList partitionKeys) {
        List<Integer> newPartitionKeys = new ArrayList<>();
        for (int i = 0; i < projects.size(); ++i) {
            RexNode rexNode = projects.get(i);
            if (rexNode instanceof RexInputRef) {
                int index = ((RexInputRef) rexNode).getIndex();
                if (partitionKeys.contains(index)) {
                    newPartitionKeys.add(i);
                }
            }
        }
        return ImmutableIntList.copyOf(newPartitionKeys);
    }

    /** This class is used to remember dim side messages while recurring in dim side. */
    public static class DppDimSideFactors {
        private boolean hasFilter;
        private boolean hasNonPartitionedScan;
        // If join key is not changed in dim side, this value is always true.
        private boolean isSuitableJoinKey = true;

        public boolean isDimSide() {
            return hasFilter && hasNonPartitionedScan && isSuitableJoinKey;
        }
    }
}
