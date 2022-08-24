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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.planner.connectors.TransformationScanProvider;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Planner utils for Dynamic partition Pruning. */
public class DynamicPartitionPruningUtils {

    /**
     * For the input join node, judge whether the join left side and join right side meet the
     * requirements of dynamic partition pruning. Fact side in left or right join is not clear.
     */
    public static boolean supportDynamicPartitionPruning(Join join) {
        return supportDynamicPartitionPruning(join, true)
                || supportDynamicPartitionPruning(join, false);
    }

    /**
     * For the input join node, judge whether the join left side and join right side meet the
     * requirements of dynamic partition pruning. Fact side in left or right is clear. If meets the
     * requirements, return true.
     */
    public static boolean supportDynamicPartitionPruning(Join join, boolean factInLeft) {
        if (!ShortcutUtils.unwrapContext(join)
                .getTableConfig()
                .get(OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED)) {
            return false;
        }
        // Now dynamic partition pruning supports left/right join, inner and semi join. but now semi
        // join can not join reorder.
        if (join.getJoinType() == JoinRelType.LEFT) {
            if (factInLeft) {
                return false;
            }
        } else if (join.getJoinType() == JoinRelType.RIGHT) {
            if (!factInLeft) {
                return false;
            }
        } else if (join.getJoinType() != JoinRelType.INNER
                && join.getJoinType() != JoinRelType.SEMI) {
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
        return factInLeft
                ? isDynamicPartitionPruningPattern(left, right, joinInfo.leftKeys)
                : isDynamicPartitionPruningPattern(right, left, joinInfo.rightKeys);
    }

    private static boolean isDynamicPartitionPruningPattern(
            RelNode factSide, RelNode dimSide, ImmutableIntList factSideJoinKey) {
        return isDimSide(dimSide) && isFactSide(factSide, factSideJoinKey);
    }

    /** make a dpp fact side factor to recurrence in fact side. */
    private static boolean isFactSide(RelNode rel, ImmutableIntList joinKeys) {
        DppFactSideFactors factSideFactors = new DppFactSideFactors();
        visitFactSide(rel, factSideFactors, joinKeys);
        return factSideFactors.isFactSide();
    }

    /**
     * Judge whether input RelNode meets the conditions of dimSide. If joinKeys is null means we
     * need not consider the join keys in dim side, which already deal by dynamic partition pruning
     * rule. If joinKeys not null means we need to judge whether joinKeys changed in dim side, if
     * changed, this RelNode is not dim side.
     */
    private static boolean isDimSide(RelNode rel) {
        DppDimSideFactors dimSideFactors = new DppDimSideFactors();
        visitDimSide(rel, dimSideFactors);
        return dimSideFactors.isDimSide();
    }

    /**
     * Visit fact side to judge whether fact side has partition table, partition table source meets
     * the condition of dpp table source and dynamic filtering keys changed in fact side.
     */
    private static void visitFactSide(
            RelNode rel, DppFactSideFactors factSideFactors, ImmutableIntList joinKeys) {
        if (rel instanceof TableScan) {
            TableScan scan = (TableScan) rel;
            if (scan instanceof BatchPhysicalDynamicFilteringTableSourceScan) {
                // rule applied
                factSideFactors.isSuitableFactScanSource = false;
                return;
            }
            TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
            if (tableSourceTable == null) {
                factSideFactors.isSuitableFactScanSource = false;
                return;
            }
            CatalogTable catalogTable = tableSourceTable.contextResolvedTable().getTable();
            List<String> partitionKeys = catalogTable.getPartitionKeys();
            if (partitionKeys.isEmpty()) {
                factSideFactors.isSuitableFactScanSource = false;
                return;
            }
            DynamicTableSource tableSource = tableSourceTable.tableSource();
            if (!(tableSource instanceof SupportsDynamicFiltering)
                    || !(tableSource instanceof ScanTableSource)) {
                factSideFactors.isSuitableFactScanSource = false;
                return;
            }
            if (!isNewSource((ScanTableSource) tableSource)) {
                factSideFactors.isSuitableFactScanSource = false;
                return;
            }

            List<String> candidateFields =
                    joinKeys.stream()
                            .map(i -> scan.getRowType().getFieldNames().get(i))
                            .collect(Collectors.toList());
            if (candidateFields.isEmpty()) {
                factSideFactors.isSuitableFactScanSource = false;
                return;
            }

            factSideFactors.isSuitableFactScanSource =
                    !getSuitableDynamicFilteringFieldsInFactSide(tableSource, candidateFields)
                            .isEmpty();
        } else if (rel instanceof HepRelVertex) {
            visitFactSide(((HepRelVertex) rel).getCurrentRel(), factSideFactors, joinKeys);
        } else if (rel instanceof Exchange || rel instanceof Filter) {
            visitFactSide(rel.getInput(0), factSideFactors, joinKeys);
        } else if (rel instanceof Project) {
            List<RexNode> projects = ((Project) rel).getProjects();
            ImmutableIntList inputJoinKeys = getInputIndices(projects, joinKeys);
            if (inputJoinKeys.isEmpty()) {
                factSideFactors.isSuitableJoinKey = false;
                return;
            }

            visitFactSide(rel.getInput(0), factSideFactors, inputJoinKeys);
        } else if (rel instanceof Calc) {
            Calc calc = (Calc) rel;
            RexProgram program = calc.getProgram();
            List<RexNode> projects =
                    program.getProjectList().stream()
                            .map(program::expandLocalRef)
                            .collect(Collectors.toList());
            ImmutableIntList inputJoinKeys = getInputIndices(projects, joinKeys);
            if (inputJoinKeys.isEmpty()) {
                factSideFactors.isSuitableJoinKey = false;
                return;
            }

            visitFactSide(rel.getInput(0), factSideFactors, inputJoinKeys);
        }
    }

    public static List<String> getSuitableDynamicFilteringFieldsInFactSide(
            DynamicTableSource tableSource, List<String> candidateFields) {
        List<String> acceptedFilterFields =
                ((SupportsDynamicFiltering) tableSource).listAcceptedFilterFields();
        if (acceptedFilterFields == null || acceptedFilterFields.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> suitableFields = new ArrayList<>();
        // If candidateField not in acceptedFilterFields means dpp rule will not be matched,
        // because we can not prune any partitions according to non-accepted filter fields
        // provided by partition table source.
        for (String candidateField : candidateFields) {
            if (acceptedFilterFields.contains(candidateField)) {
                suitableFields.add(candidateField);
            }
        }

        return suitableFields;
    }

    /**
     * Visit dim side to judge whether dim side has filter condition and whether dim side's source
     * table scan is non partitioned scan.
     */
    private static void visitDimSide(RelNode rel, DppDimSideFactors dimSideFactors) {
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
            visitDimSide(((HepRelVertex) rel).getCurrentRel(), dimSideFactors);
        } else if (rel instanceof Exchange || rel instanceof Project) {
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else if (rel instanceof Calc) {
            RexProgram origProgram = ((Calc) rel).getProgram();
            if (origProgram.getCondition() != null
                    && isSuitableFilter(origProgram.expandLocalRef(origProgram.getCondition()))) {
                dimSideFactors.hasFilter = true;
            }
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else if (rel instanceof Filter) {
            if (isSuitableFilter(((Filter) rel).getCondition())) {
                dimSideFactors.hasFilter = true;
            }
            visitDimSide(rel.getInput(0), dimSideFactors);
        }
    }

    /**
     * Not all filter condition suitable for using to filter partitions by dynamic partition pruning
     * rules. For example, NOT NULL can only filter one default partition which have a small impact
     * on filtering data.
     */
    private static boolean isSuitableFilter(RexNode filterCondition) {
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

    /** Returns true if the source is FLIP-27 source, else false. */
    private static boolean isNewSource(ScanTableSource scanTableSource) {
        ScanTableSource.ScanRuntimeProvider provider =
                scanTableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        if (provider instanceof SourceProvider) {
            return true;
        } else if (provider instanceof TransformationScanProvider) {
            Transformation<?> transformation =
                    ((TransformationScanProvider) provider)
                            .createTransformation(name -> Optional.empty());
            return transformation instanceof SourceTransformation;
        } else if (provider instanceof DataStreamScanProvider) {
            // Suppose DataStreamScanProvider of sources that support dynamic filtering will use new
            // Source. It's not reliable and should be checked.
            // TODO FLINK-28864 check if the source used by the DataStreamScanProvider is actually a
            //  new source.
            // This situation will not generate wrong result because it's handled when translating
            // BatchTableSourceScan. The only effect is the physical plan and the exec node plan
            // have DPP nodes, but they do not work in runtime.
            return true;
        }
        // TODO supports more
        return false;
    }

    private static ImmutableIntList getInputIndices(
            List<RexNode> projects, ImmutableIntList joinKeys) {
        List<Integer> indices = new ArrayList<>();
        for (int k : joinKeys) {
            RexNode rexNode = projects.get(k);
            if (rexNode instanceof RexInputRef) {
                indices.add(((RexInputRef) rexNode).getIndex());
            }
        }
        return ImmutableIntList.copyOf(indices);
    }

    /** This class is used to remember dim side messages while recurring in dim side. */
    private static class DppDimSideFactors {
        private boolean hasFilter;
        private boolean hasNonPartitionedScan;

        public boolean isDimSide() {
            return hasFilter && hasNonPartitionedScan;
        }
    }

    /** This class is used to remember fact side messages while recurring in fact side. */
    private static class DppFactSideFactors {
        private boolean isSuitableFactScanSource;
        // If join key is not changed in fact side, this value is always true.
        private boolean isSuitableJoinKey = true;

        public boolean isFactSide() {
            return isSuitableFactScanSource && isSuitableJoinKey;
        }
    }
}
