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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.connectors.TransformationScanProvider;
import org.apache.flink.table.planner.plan.abilities.source.AggregatePushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringDataCollector;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
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
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Planner utils for Dynamic partition Pruning. */
public class DynamicPartitionPruningUtils {

    /**
     * Judge whether the input RelNode meets the conditions of dimSide. If joinKeys is null means we
     * need not consider the join keys in dim side, which already deal by dynamic partition pruning
     * rule. If joinKeys not null means we need to judge whether joinKeys changed in dim side, if
     * changed, this RelNode is not dim side.
     */
    public static boolean isDppDimSide(RelNode rel) {
        DppDimSideChecker dimSideChecker = new DppDimSideChecker(rel);
        return dimSideChecker.isDppDimSide();
    }

    /**
     * Judge whether the input RelNode can be converted to the dpp fact side. If the input RelNode
     * can be converted, this method will return the converted fact side whose partitioned table
     * source will be converted to {@link BatchPhysicalDynamicFilteringTableSourceScan}, If not,
     * this method will return the origin RelNode.
     */
    public static Tuple2<Boolean, RelNode> canConvertAndConvertDppFactSide(
            RelNode rel,
            ImmutableIntList joinKeys,
            RelNode dimSide,
            ImmutableIntList dimSideJoinKey) {
        DppFactSideChecker dppFactSideChecker =
                new DppFactSideChecker(rel, joinKeys, dimSide, dimSideJoinKey);
        return dppFactSideChecker.canConvertAndConvertDppFactSide();
    }

    /** Judge whether the join node is suitable one for dpp pattern. */
    public static boolean isSuitableJoin(Join join) {
        // Now dynamic partition pruning supports left/right join, inner and semi
        // join. but now semi join can not join reorder.
        if (join.getJoinType() != JoinRelType.INNER
                && join.getJoinType() != JoinRelType.SEMI
                && join.getJoinType() != JoinRelType.LEFT
                && join.getJoinType() != JoinRelType.RIGHT) {
            return false;
        }

        JoinInfo joinInfo = join.analyzeCondition();
        return !joinInfo.leftKeys.isEmpty();
    }

    /** This class is used to check whether the relNode is dpp dim side. */
    private static class DppDimSideChecker {
        private final RelNode relNode;
        private boolean hasFilter;
        private boolean hasPartitionedScan;
        private final List<ContextResolvedTable> tables = new ArrayList<>();

        public DppDimSideChecker(RelNode relNode) {
            this.relNode = relNode;
        }

        public boolean isDppDimSide() {
            visitDimSide(this.relNode);
            return hasFilter && !hasPartitionedScan && tables.size() == 1;
        }

        /**
         * Visit dim side to judge whether dim side has filter condition and whether dim side's
         * source table scan is non partitioned scan.
         */
        private void visitDimSide(RelNode rel) {
            // TODO Let visitDimSide more efficient and more accurate. Like a filter on dim table or
            // a filter for the partition field on fact table.
            if (rel instanceof TableScan) {
                TableScan scan = (TableScan) rel;
                TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
                if (table == null) {
                    return;
                }
                if (!hasFilter
                        && table.abilitySpecs() != null
                        && table.abilitySpecs().length != 0) {
                    for (SourceAbilitySpec spec : table.abilitySpecs()) {
                        if (spec instanceof FilterPushDownSpec) {
                            List<RexNode> predicates = ((FilterPushDownSpec) spec).getPredicates();
                            for (RexNode predicate : predicates) {
                                if (isSuitableFilter(predicate)) {
                                    hasFilter = true;
                                }
                            }
                        }
                    }
                }
                CatalogTable catalogTable = table.contextResolvedTable().getResolvedTable();
                if (catalogTable.isPartitioned()) {
                    hasPartitionedScan = true;
                    return;
                }

                // To ensure there is only one source on the dim side.
                setTables(table.contextResolvedTable());
            } else if (rel instanceof HepRelVertex) {
                visitDimSide(((HepRelVertex) rel).getCurrentRel());
            } else if (rel instanceof Exchange || rel instanceof Project) {
                visitDimSide(rel.getInput(0));
            } else if (rel instanceof Calc) {
                RexProgram origProgram = ((Calc) rel).getProgram();
                if (origProgram.getCondition() != null
                        && isSuitableFilter(
                                origProgram.expandLocalRef(origProgram.getCondition()))) {
                    hasFilter = true;
                }
                visitDimSide(rel.getInput(0));
            } else if (rel instanceof Filter) {
                if (isSuitableFilter(((Filter) rel).getCondition())) {
                    hasFilter = true;
                }
                visitDimSide(rel.getInput(0));
            } else if (rel instanceof Join) {
                Join join = (Join) rel;
                visitDimSide(join.getLeft());
                visitDimSide(join.getRight());
            } else if (rel instanceof BatchPhysicalGroupAggregateBase) {
                visitDimSide(((BatchPhysicalGroupAggregateBase) rel).getInput());
            } else if (rel instanceof Union) {
                Union union = (Union) rel;
                for (RelNode input : union.getInputs()) {
                    visitDimSide(input);
                }
            }
        }

        /**
         * Not all filter condition suitable for using to filter partitions by dynamic partition
         * pruning rules. For example, NOT NULL can only filter one default partition which have a
         * small impact on filtering data.
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
                    // TODO adding more suitable filters which can filter enough partitions after
                    // using this filter in dynamic partition pruning.
                    return true;
                default:
                    return false;
            }
        }

        private void setTables(ContextResolvedTable catalogTable) {
            if (tables.size() == 0) {
                tables.add(catalogTable);
            } else {
                for (ContextResolvedTable thisTable : new ArrayList<>(tables)) {
                    if (!thisTable.getIdentifier().equals(catalogTable.getIdentifier())) {
                        tables.add(catalogTable);
                    }
                }
            }
        }
    }

    /** This class is used to check whether the relNode can be as a fact side in dpp. */
    private static class DppFactSideChecker {
        private final RelNode relNode;
        private final ImmutableIntList joinKeys;
        private final RelNode dimSide;
        private final ImmutableIntList dimSideJoinKey;

        // If join key is not changed in fact side, this value is always true.
        private boolean isChanged;

        public DppFactSideChecker(
                RelNode relNode,
                ImmutableIntList joinKeys,
                RelNode dimSide,
                ImmutableIntList dimSideJoinKey) {
            this.relNode = relNode;
            this.joinKeys = joinKeys;
            this.dimSide = dimSide;
            this.dimSideJoinKey = dimSideJoinKey;
        }

        public Tuple2<Boolean, RelNode> canConvertAndConvertDppFactSide() {
            return Tuple2.of(
                    isChanged, convertDppFactSide(relNode, joinKeys, dimSide, dimSideJoinKey));
        }

        private RelNode convertDppFactSide(
                RelNode rel,
                ImmutableIntList joinKeys,
                RelNode dimSide,
                ImmutableIntList dimSideJoinKey) {
            if (rel instanceof TableScan) {
                TableScan scan = (TableScan) rel;
                if (scan instanceof BatchPhysicalDynamicFilteringTableSourceScan) {
                    // rule applied
                    return rel;
                }
                TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
                if (tableSourceTable == null) {
                    return rel;
                }
                CatalogTable catalogTable =
                        tableSourceTable.contextResolvedTable().getResolvedTable();
                List<String> partitionKeys = catalogTable.getPartitionKeys();
                if (partitionKeys.isEmpty()) {
                    return rel;
                }
                DynamicTableSource tableSource = tableSourceTable.tableSource();
                if (!(tableSource instanceof SupportsDynamicFiltering)
                        || !(tableSource instanceof ScanTableSource)) {
                    return rel;
                }

                // Dpp cannot success if source have aggregate push down spec.
                if (Arrays.stream(tableSourceTable.abilitySpecs())
                        .anyMatch(spec -> spec instanceof AggregatePushDownSpec)) {
                    return rel;
                }

                if (!isNewSource((ScanTableSource) tableSource)) {
                    return rel;
                }

                List<String> candidateFields =
                        joinKeys.stream()
                                .map(i -> scan.getRowType().getFieldNames().get(i))
                                .collect(Collectors.toList());
                if (candidateFields.isEmpty()) {
                    return rel;
                }

                List<String> acceptedFilterFields =
                        getSuitableDynamicFilteringFieldsInFactSide(tableSource, candidateFields);

                if (acceptedFilterFields.size() == 0) {
                    return rel;
                }

                // Apply suitable accepted filter fields to source.
                ((SupportsDynamicFiltering) tableSource)
                        .applyDynamicFiltering(acceptedFilterFields);

                List<Integer> acceptedFieldIndices =
                        acceptedFilterFields.stream()
                                .map(f -> scan.getRowType().getFieldNames().indexOf(f))
                                .collect(Collectors.toList());
                List<Integer> dynamicFilteringFieldIndices = new ArrayList<>();
                for (int i = 0; i < joinKeys.size(); ++i) {
                    if (acceptedFieldIndices.contains(joinKeys.get(i))) {
                        dynamicFilteringFieldIndices.add(dimSideJoinKey.get(i));
                    }
                }

                BatchPhysicalDynamicFilteringDataCollector dynamicFilteringDataCollector =
                        createDynamicFilteringConnector(dimSide, dynamicFilteringFieldIndices);

                isChanged = true;
                return new BatchPhysicalDynamicFilteringTableSourceScan(
                        scan.getCluster(),
                        scan.getTraitSet(),
                        scan.getHints(),
                        tableSourceTable,
                        dynamicFilteringDataCollector,
                        acceptedFieldIndices);
            } else if (rel instanceof Exchange || rel instanceof Filter) {
                return rel.copy(
                        rel.getTraitSet(),
                        Collections.singletonList(
                                convertDppFactSide(
                                        rel.getInput(0), joinKeys, dimSide, dimSideJoinKey)));
            } else if (rel instanceof Project) {
                List<RexNode> projects = ((Project) rel).getProjects();
                ImmutableIntList inputJoinKeys = getInputIndices(projects, joinKeys);
                if (inputJoinKeys.isEmpty()) {
                    return rel;
                }

                return rel.copy(
                        rel.getTraitSet(),
                        Collections.singletonList(
                                convertDppFactSide(
                                        rel.getInput(0), inputJoinKeys, dimSide, dimSideJoinKey)));
            } else if (rel instanceof Calc) {
                Calc calc = (Calc) rel;
                RexProgram program = calc.getProgram();
                List<RexNode> projects =
                        program.getProjectList().stream()
                                .map(program::expandLocalRef)
                                .collect(Collectors.toList());
                ImmutableIntList inputJoinKeys = getInputIndices(projects, joinKeys);
                if (inputJoinKeys.isEmpty()) {
                    return rel;
                }

                return rel.copy(
                        rel.getTraitSet(),
                        Collections.singletonList(
                                convertDppFactSide(
                                        rel.getInput(0), inputJoinKeys, dimSide, dimSideJoinKey)));
            } else if (rel instanceof Join) {
                Join currentJoin = (Join) rel;
                return currentJoin.copy(
                        currentJoin.getTraitSet(),
                        Arrays.asList(
                                convertDppFactSide(
                                        currentJoin.getLeft(),
                                        getInputIndices(currentJoin, joinKeys, true),
                                        dimSide,
                                        dimSideJoinKey),
                                convertDppFactSide(
                                        currentJoin.getRight(),
                                        getInputIndices(currentJoin, joinKeys, false),
                                        dimSide,
                                        dimSideJoinKey)));
            } else if (rel instanceof Union) {
                Union union = (Union) rel;
                List<RelNode> newInputs = new ArrayList<>();
                for (RelNode input : union.getInputs()) {
                    newInputs.add(convertDppFactSide(input, joinKeys, dimSide, dimSideJoinKey));
                }
                return union.copy(union.getTraitSet(), newInputs, union.all);
            } else if (rel instanceof BatchPhysicalGroupAggregateBase) {
                BatchPhysicalGroupAggregateBase agg = (BatchPhysicalGroupAggregateBase) rel;
                RelNode input = agg.getInput();
                int[] grouping = agg.grouping();

                // If one of joinKey in joinKeys are aggregate function field, dpp will not success.
                for (int k : joinKeys) {
                    if (k >= grouping.length) {
                        return rel;
                    }
                }

                RelNode convertedRel =
                        convertDppFactSide(
                                input,
                                ImmutableIntList.copyOf(
                                        joinKeys.stream()
                                                .map(joinKey -> agg.grouping()[joinKey])
                                                .collect(Collectors.toList())),
                                dimSide,
                                dimSideJoinKey);
                return agg.copy(agg.getTraitSet(), Collections.singletonList(convertedRel));
            } else {
                // TODO In the future, we need to support more operators to enrich matchable dpp
                // pattern.
            }

            return rel;
        }

        private static List<String> getSuitableDynamicFilteringFieldsInFactSide(
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

        private static BatchPhysicalDynamicFilteringDataCollector createDynamicFilteringConnector(
                RelNode dimSide, List<Integer> dynamicFilteringFieldIndices) {
            final RelDataType outputType =
                    ((FlinkTypeFactory) dimSide.getCluster().getTypeFactory())
                            .projectStructType(
                                    dimSide.getRowType(),
                                    dynamicFilteringFieldIndices.stream()
                                            .mapToInt(i -> i)
                                            .toArray());
            return new BatchPhysicalDynamicFilteringDataCollector(
                    dimSide.getCluster(),
                    dimSide.getTraitSet(),
                    ignoreExchange(dimSide),
                    outputType,
                    dynamicFilteringFieldIndices.stream().mapToInt(i -> i).toArray());
        }

        private static RelNode ignoreExchange(RelNode dimSide) {
            if (dimSide instanceof Exchange) {
                return dimSide.getInput(0);
            } else {
                return dimSide;
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
                // Suppose DataStreamScanProvider of sources that support dynamic filtering will use
                // new Source. It's not reliable and should be checked.
                // TODO FLINK-28864 check if the source used by the DataStreamScanProvider is
                // actually a new source. This situation will not generate wrong result because it's
                // handled when translating BatchTableSourceScan. The only effect is the physical
                // plan and the exec node plan have DPP nodes, but they do not work in runtime.
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

        private static ImmutableIntList getInputIndices(
                Join join, ImmutableIntList joinKeys, boolean isLeft) {
            List<Integer> indices = new ArrayList<>();
            RelNode left = join.getLeft();
            int leftSize = left.getRowType().getFieldCount();
            for (int k : joinKeys) {
                if (isLeft) {
                    if (k < leftSize) {
                        indices.add(k);
                    }
                } else {
                    if (k >= leftSize) {
                        indices.add(k - leftSize);
                    }
                }
            }
            return ImmutableIntList.copyOf(indices);
        }
    }
}
