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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.catalog.Index;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.trait.DuplicateChanges;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.mapping.IntPair;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils for delta joins. */
public class DeltaJoinUtil {

    /**
     * All supported delta join upstream nodes. Only the following nodes are allowed to exist
     * between the delta join and the source. Otherwise, the regular join will not be optimized into
     * the delta join.
     *
     * <p>More physical nodes can be added to support more patterns for delta join.
     */
    private static final Set<Class<?>> ALL_SUPPORTED_DELTA_JOIN_UPSTREAM_NODES =
            Sets.newHashSet(StreamPhysicalTableSourceScan.class, StreamPhysicalExchange.class);

    private DeltaJoinUtil() {}

    /** Check whether the {@link StreamPhysicalJoin} can be optimized into a delta join. */
    public static boolean canConvertToDeltaJoin(StreamPhysicalJoin join) {
        if (!isJoinTypeSupported(join)) {
            return false;
        }

        if (!areJoinConditionsSupported(join)) {
            return false;
        }

        // delta join with eventual consistency will send duplicate changes to downstream nodes
        if (!canJoinOutputDuplicateChanges(join)) {
            return false;
        }

        // currently, only join with append-only inputs is supported
        if (!areAllInputsInsertOnly(join)) {
            return false;
        }

        if (!areAllJoinInputsInWhiteList(join)) {
            return false;
        }

        return areAllJoinTableScansSupported(join);
    }

    /**
     * Get the {@link RelOptTable} from the {@link TableScan} recursively on the input of this node.
     */
    public static RelOptTable getTableScanRelOptTable(RelNode node) {
        return getTableScan(node).getTable();
    }

    /**
     * Extract the delta join spec used for {@link StreamPhysicalDeltaJoin} from {@link
     * StreamPhysicalJoin}.
     */
    public static DeltaJoinSpec getDeltaJoinSpec(
            StreamPhysicalJoin join, boolean treatRightAsLookupSide) {
        JoinInfo joinInfo = join.analyzeCondition();
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        RexNode condition = RexUtil.composeConjunction(rexBuilder, joinInfo.nonEquiConditions);
        Optional<RexNode> remainingCondition =
                condition.isAlwaysTrue() ? Optional.empty() : Optional.of(condition);

        final RelOptTable lookupRelOptTable;
        List<IntPair> streamToLookupJoinKeys = joinInfo.pairs();
        if (treatRightAsLookupSide) {
            lookupRelOptTable = DeltaJoinUtil.getTableScanRelOptTable(join.getRight());
        } else {
            streamToLookupJoinKeys = reverseIntPairs(streamToLookupJoinKeys);
            lookupRelOptTable = DeltaJoinUtil.getTableScanRelOptTable(join.getLeft());
        }
        Preconditions.checkState(lookupRelOptTable instanceof TableSourceTable);
        final TableSourceTable lookupTable = (TableSourceTable) lookupRelOptTable;

        Map<Integer, FunctionCallUtils.FunctionParam> allLookupKeys =
                analyzerDeltaJoinLookupKeys(streamToLookupJoinKeys);

        return new DeltaJoinSpec(
                new TemporalTableSourceSpec(lookupTable),
                allLookupKeys,
                remainingCondition.orElse(null));
    }

    /**
     * get the lookup key from the join keys.
     *
     * <p>Different with {@see CommonPhysicalLookupJoin#analyzeLookupKeys}, we have not supported
     * calc between delta join and source yet.
     *
     * @param streamToLookupJoinKeys the join keys from stream side to lookup side
     */
    private static Map<Integer, FunctionCallUtils.FunctionParam> analyzerDeltaJoinLookupKeys(
            List<IntPair> streamToLookupJoinKeys) {
        Map<Integer, FunctionCallUtils.FunctionParam> allFieldRefLookupKeys = new LinkedHashMap<>();
        for (IntPair intPair : streamToLookupJoinKeys) {
            allFieldRefLookupKeys.put(
                    intPair.target, new FunctionCallUtils.FieldRef(intPair.source));
        }
        return allFieldRefLookupKeys;
    }

    private static List<IntPair> reverseIntPairs(List<IntPair> intPairs) {
        return intPairs.stream()
                .map(pair -> new IntPair(pair.target, pair.source))
                .collect(Collectors.toList());
    }

    private static int[][] getColumnIndicesOfAllTableIndexes(TableSourceTable tableSourceTable) {
        List<List<String>> columnsOfIndexes = getAllIndexesColumnsOfTable(tableSourceTable);
        int[][] results = new int[columnsOfIndexes.size()][];
        for (int i = 0; i < columnsOfIndexes.size(); i++) {
            List<String> fieldNames = tableSourceTable.getRowType().getFieldNames();
            results[i] = columnsOfIndexes.get(i).stream().mapToInt(fieldNames::indexOf).toArray();
        }

        return results;
    }

    private static List<List<String>> getAllIndexesColumnsOfTable(
            TableSourceTable tableSourceTable) {
        ResolvedSchema schema = tableSourceTable.contextResolvedTable().getResolvedSchema();
        List<Index> indexes = schema.getIndexes();
        return indexes.stream().map(Index::getColumns).collect(Collectors.toList());
    }

    private static boolean areJoinConditionsSupported(StreamPhysicalJoin join) {
        JoinInfo joinInfo = join.analyzeCondition();
        // there must be one pair of join key
        return !joinInfo.pairs().isEmpty();
    }

    private static boolean areAllJoinTableScansSupported(StreamPhysicalJoin join) {
        return isTableScanSupported(getTableScan(join.getLeft()), join.joinSpec().getLeftKeys())
                && isTableScanSupported(
                        getTableScan(join.getRight()), join.joinSpec().getRightKeys());
    }

    private static boolean isTableScanSupported(TableScan tableScan, int[] lookupKeys) {
        // legacy source and data stream source are not supported yet
        if (!(tableScan instanceof StreamPhysicalTableSourceScan)) {
            return false;
        }

        TableSourceTable tableSourceTable =
                ((StreamPhysicalTableSourceScan) tableScan).tableSourceTable();

        // source with ability specs are not supported yet
        if (tableSourceTable.abilitySpecs().length != 0) {
            return false;
        }

        DynamicTableSource source = tableSourceTable.tableSource();
        // the source must also be a lookup source
        if (!(source instanceof LookupTableSource)) {
            return false;
        }

        int[][] idxsOfAllIndexes = getColumnIndicesOfAllTableIndexes(tableSourceTable);
        if (idxsOfAllIndexes.length == 0) {
            return false;
        }
        // the source must have at least one index, and the join key contains one index
        Set<Integer> lookupKeysSet = Arrays.stream(lookupKeys).boxed().collect(Collectors.toSet());

        for (int[] idxsOfIndex : idxsOfAllIndexes) {
            Preconditions.checkState(idxsOfIndex.length > 0);

            // ignore the field order of the index
            boolean containsIndex = Arrays.stream(idxsOfIndex).allMatch(lookupKeysSet::contains);
            if (!containsIndex) {
                return false;
            }
        }

        // the lookup source must support async lookup
        return LookupJoinUtil.isAsyncLookup(
                tableSourceTable,
                lookupKeysSet,
                null, // hint
                false, // upsertMaterialize
                false // preferCustomShuffle
                );
    }

    private static TableScan getTableScan(RelNode node) {
        node = unwrapNode(node, true);
        // support to get table across more nodes if we support more nodes in
        // `ALL_SUPPORTED_DELTA_JOIN_UPSTREAM_NODES`
        if (node instanceof StreamPhysicalExchange) {
            return getTableScan(((StreamPhysicalExchange) node).getInput());
        }

        Preconditions.checkState(node instanceof TableScan);
        return (TableScan) node;
    }

    private static boolean isJoinTypeSupported(StreamPhysicalJoin join) {
        // currently, only inner join is supported
        return JoinRelType.INNER == join.getJoinType();
    }

    private static boolean areAllJoinInputsInWhiteList(RelNode node) {
        for (RelNode input : node.getInputs()) {
            input = unwrapNode(input, true);
            if (!isTheNodeInWhiteList(input)) {
                return false;
            }
            if (!areAllJoinInputsInWhiteList(input)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTheNodeInWhiteList(RelNode node) {
        Class<?> nodeClazz = node.getClass();
        return ALL_SUPPORTED_DELTA_JOIN_UPSTREAM_NODES.contains(nodeClazz);
    }

    private static boolean canJoinOutputDuplicateChanges(StreamPhysicalJoin join) {
        DuplicateChanges duplicateChanges =
                DuplicateChangesUtils.getDuplicateChanges(join)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                String.format(
                                                        "Unable to derive changelog mode from node %s. This is a bug.",
                                                        join)));

        return DuplicateChanges.ALLOW.equals(duplicateChanges);
    }

    private static boolean areAllInputsInsertOnly(StreamPhysicalJoin join) {
        for (RelNode input : join.getInputs()) {
            if (!isInsertOnly(unwrapNode(input, false))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isInsertOnly(StreamPhysicalRel node) {
        ChangelogMode changelogMode =
                JavaScalaConversionUtil.toJava(ChangelogPlanUtils.getChangelogMode(node))
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                String.format(
                                                        "Unable to derive changelog mode from node %s. This is a bug.",
                                                        node)));
        return changelogMode.containsOnly(RowKind.INSERT);
    }

    private static StreamPhysicalRel unwrapNode(RelNode node, boolean transposeToChildBlock) {
        if (node instanceof HepRelVertex) {
            node = ((HepRelVertex) node).getCurrentRel();
        }
        if (node instanceof StreamPhysicalIntermediateTableScan && transposeToChildBlock) {
            IntermediateRelTable inputBlockOptimizedTree = (IntermediateRelTable) node.getTable();
            Preconditions.checkState(inputBlockOptimizedTree != null);
            node = inputBlockOptimizedTree.relNode();
        }
        return (StreamPhysicalRel) node;
    }
}
