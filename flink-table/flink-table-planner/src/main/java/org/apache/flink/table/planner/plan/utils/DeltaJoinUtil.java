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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.Index;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.PartitionPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.trait.DuplicateChanges;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.functions.table.lookup.CachingAsyncLookupFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableAsyncLookupFunctionDelegator;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Option;

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
            Sets.newHashSet(
                    StreamPhysicalTableSourceScan.class,
                    StreamPhysicalExchange.class,
                    StreamPhysicalDropUpdateBefore.class,
                    StreamPhysicalCalc.class);

    /**
     * All supported {@link SourceAbilitySpec}s in sources. Only the sources with the following
     * {@link SourceAbilitySpec} can be used as delta join sources. Otherwise, the regular join will
     * not be optimized into the delta join.
     */
    private static final Set<Class<?>> ALL_SUPPORTED_ABILITY_SPEC_IN_SOURCE =
            Sets.newHashSet(
                    FilterPushDownSpec.class,
                    ProjectPushDownSpec.class,
                    PartitionPushDownSpec.class,
                    // TODO FLINK-38569 ReadingMetadataSpec should not be generated when there are
                    //  no metadata keys to be read
                    ReadingMetadataSpec.class);

    private DeltaJoinUtil() {}

    /** Check whether the {@link StreamPhysicalJoin} can be optimized into a delta join. */
    public static boolean canConvertToDeltaJoin(StreamPhysicalJoin join) {
        FlinkJoinType flinkJoinType = JoinTypeUtil.getFlinkJoinType(join.getJoinType());
        if (!isJoinTypeSupported(flinkJoinType)) {
            return false;
        }

        if (!areJoinConditionsSupported(join)) {
            return false;
        }

        // delta join with eventual consistency will send duplicate changes to downstream nodes
        if (!canJoinOutputDuplicateChanges(join)) {
            return false;
        }

        // currently, only join that consumes +I and +U is supported
        if (!areAllInputsInsertOrUpdateAfter(join)) {
            return false;
        }

        if (!areAllJoinInputsInWhiteList(join)) {
            return false;
        }

        if (!areAllUpstreamCalcSupported(join)) {
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

        List<IntPair> joinPairs = joinInfo.pairs();
        final RelOptTable lookupRelOptTable;
        final IntPair[] streamToLookupJoinKeys;
        final Optional<RexProgram> calcOnLookupTable;

        if (treatRightAsLookupSide) {
            calcOnLookupTable = getRexProgramBetweenJoinAndTableScan(join.getRight());

            lookupRelOptTable = DeltaJoinUtil.getTableScanRelOptTable(join.getRight());
            streamToLookupJoinKeys =
                    TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                            joinPairs.toArray(new IntPair[0]),
                            JavaScalaConversionUtil.toScala(calcOnLookupTable));

        } else {
            calcOnLookupTable = getRexProgramBetweenJoinAndTableScan(join.getLeft());

            joinPairs = reverseIntPairs(joinInfo.pairs());
            lookupRelOptTable = DeltaJoinUtil.getTableScanRelOptTable(join.getLeft());
            streamToLookupJoinKeys =
                    TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                            joinPairs.toArray(new IntPair[0]),
                            JavaScalaConversionUtil.toScala(calcOnLookupTable));
        }
        Preconditions.checkState(lookupRelOptTable instanceof TableSourceTable);
        final TableSourceTable lookupTable = (TableSourceTable) lookupRelOptTable;

        Map<Integer, FunctionCallUtil.FunctionParam> allLookupKeys =
                analyzerDeltaJoinLookupKeys(streamToLookupJoinKeys);

        List<RexNode> projectionOnTemporalTable = null;
        RexNode filterOnTemporalTable = null;

        if (calcOnLookupTable.isPresent()) {
            Tuple2<List<RexNode>, Option<RexNode>> projectionsAndFilter =
                    JavaScalaConversionUtil.toJava(
                            FlinkRexUtil.expandRexProgram(calcOnLookupTable.get()));
            projectionOnTemporalTable = projectionsAndFilter.f0;
            filterOnTemporalTable =
                    JavaScalaConversionUtil.toJava(projectionsAndFilter.f1).orElse(null);
        }

        return new DeltaJoinSpec(
                new TemporalTableSourceSpec(lookupTable),
                allLookupKeys,
                remainingCondition.orElse(null),
                projectionOnTemporalTable,
                filterOnTemporalTable);
    }

    /**
     * Get the async lookup function to lookup join this temporal table. Furthermore, this method
     * also unwraps the cache and retryable lookup function to access the inner {@link
     * AsyncTableFunction}.
     */
    public static AsyncTableFunction<?> getUnwrappedAsyncLookupFunction(
            RelOptTable temporalTable, Collection<Integer> lookupKeys, ClassLoader classLoader) {
        UserDefinedFunction lookupFunction =
                LookupJoinUtil.getLookupFunction(
                        temporalTable,
                        lookupKeys,
                        classLoader,
                        true, // async
                        null, // retryStrategy
                        false); // applyCustomShuffle

        boolean changed = true;
        while (changed) {
            // unwrap cache delegator
            if (lookupFunction instanceof CachingAsyncLookupFunction) {
                lookupFunction = ((CachingAsyncLookupFunction) lookupFunction).getDelegate();
                continue;
            }
            // unwrap retryable delegator
            if (lookupFunction instanceof RetryableAsyncLookupFunctionDelegator) {
                lookupFunction =
                        ((RetryableAsyncLookupFunctionDelegator) lookupFunction)
                                .getUserLookupFunction();
                continue;
            }
            changed = false;
        }

        if (!(lookupFunction instanceof AsyncTableFunction)) {
            throw new IllegalStateException(
                    String.format(
                            "Table [%s] does not support async lookup. If the table supports the option of "
                                    + "async lookup joins, add it to the with parameters of the DDL.",
                            String.join(".", temporalTable.getQualifiedName())));
        }
        return (AsyncTableFunction<?>) lookupFunction;
    }

    public static boolean isJoinTypeSupported(FlinkJoinType flinkJoinType) {
        // currently, only inner join is supported
        return FlinkJoinType.INNER == flinkJoinType;
    }

    /**
     * get the lookup key from the join keys.
     *
     * <p>Different with {@see CommonPhysicalLookupJoin#analyzeLookupKeys}, we have not supported
     * calc between delta join and source yet.
     *
     * @param streamToLookupJoinKeys the join keys from stream side to lookup side
     */
    private static Map<Integer, FunctionCallUtil.FunctionParam> analyzerDeltaJoinLookupKeys(
            IntPair[] streamToLookupJoinKeys) {
        Map<Integer, FunctionCallUtil.FunctionParam> allFieldRefLookupKeys = new LinkedHashMap<>();
        for (IntPair intPair : streamToLookupJoinKeys) {
            allFieldRefLookupKeys.put(
                    intPair.target, new FunctionCallUtil.FieldRef(intPair.source));
        }
        return allFieldRefLookupKeys;
    }

    private static List<IntPair> reverseIntPairs(List<IntPair> intPairs) {
        return intPairs.stream()
                .map(pair -> new IntPair(pair.target, pair.source))
                .collect(Collectors.toList());
    }

    private static int[][] getAllIndexesColumnsFromTableSchema(ResolvedSchema schema) {
        List<Index> indexes = schema.getIndexes();
        List<List<String>> columnsOfIndexes =
                indexes.stream().map(Index::getColumns).collect(Collectors.toList());
        int[][] results = new int[columnsOfIndexes.size()][];
        for (int i = 0; i < columnsOfIndexes.size(); i++) {
            List<String> fieldNames = schema.getColumnNames();
            results[i] = columnsOfIndexes.get(i).stream().mapToInt(fieldNames::indexOf).toArray();
        }

        return results;
    }

    private static boolean areJoinConditionsSupported(StreamPhysicalJoin join) {
        JoinInfo joinInfo = join.analyzeCondition();
        // there must be one pair of join key
        if (joinInfo.pairs().isEmpty()) {
            return false;
        }
        JoinSpec joinSpec = join.joinSpec();
        Optional<RexNode> nonEquiCond = joinSpec.getNonEquiCondition();
        if (nonEquiCond.isPresent()
                && !areAllRexNodeDeterministic(Collections.singletonList(nonEquiCond.get()))) {
            return false;
        }

        // if this join outputs cdc records and has non-equiv condition, the reference columns in
        // the non-equiv condition must come from the same set of upsert keys
        ChangelogMode changelogMode = getChangelogMode(join);
        if (changelogMode.containsOnly(RowKind.INSERT)) {
            return true;
        }

        if (nonEquiCond.isEmpty()) {
            return true;
        }

        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(join.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> upsertKeys = fmq.getUpsertKeys(join);
        return isFilterOnOneSetOfUpsertKeys(nonEquiCond.get(), upsertKeys);
    }

    @VisibleForTesting
    protected static boolean isFilterOnOneSetOfUpsertKeys(
            RexNode filter, @Nullable Set<ImmutableBitSet> upsertKeys) {
        ImmutableBitSet fieldRefIndices =
                ImmutableBitSet.of(
                        RexNodeExtractor.extractRefInputFields(Collections.singletonList(filter)));
        return upsertKeys != null
                && upsertKeys.stream().anyMatch(uk -> uk.contains(fieldRefIndices));
    }

    private static boolean areAllJoinTableScansSupported(StreamPhysicalJoin join) {
        List<IntPair> left2RightJoinPairs =
                JoinUtil.createJoinInfo(
                                join.getLeft(),
                                join.getRight(),
                                join.getCondition(),
                                new ArrayList<>())
                        .pairs();

        Optional<RexProgram> calcOnLeftLookupTable =
                getRexProgramBetweenJoinAndTableScan(join.getLeft());
        Optional<RexProgram> calcOnRightLookupTable =
                getRexProgramBetweenJoinAndTableScan(join.getRight());

        List<IntPair> right2LeftJoinPair = reverseIntPairs(left2RightJoinPairs);

        int[] leftJoinKeyOnLeftLookupTable =
                Arrays.stream(
                                TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                                        right2LeftJoinPair.toArray(new IntPair[0]),
                                        JavaScalaConversionUtil.toScala(calcOnLeftLookupTable)))
                        .mapToInt(pair -> pair.target)
                        .toArray();
        int[] rightJoinKeyOnRightLookupTable =
                Arrays.stream(
                                TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                                        left2RightJoinPairs.toArray(new IntPair[0]),
                                        JavaScalaConversionUtil.toScala(calcOnRightLookupTable)))
                        .mapToInt(pair -> pair.target)
                        .toArray();

        return isTableScanSupported(getTableScan(join.getLeft()), leftJoinKeyOnLeftLookupTable)
                && isTableScanSupported(
                        getTableScan(join.getRight()), rightJoinKeyOnRightLookupTable);
    }

    private static boolean isTableScanSupported(TableScan tableScan, int[] lookupKeys) {
        // legacy source and data stream source are not supported yet
        if (!(tableScan instanceof StreamPhysicalTableSourceScan)) {
            return false;
        }

        TableSourceTable tableSourceTable =
                ((StreamPhysicalTableSourceScan) tableScan).tableSourceTable();

        if (tableSourceTable.abilitySpecs().length != 0
                && !areAllSourceAbilitySpecsSupported(tableScan, tableSourceTable.abilitySpecs())) {
            return false;
        }

        DynamicTableSource source = tableSourceTable.tableSource();
        // the source must also be a lookup source
        if (!(source instanceof LookupTableSource)) {
            return false;
        }

        Set<Integer> lookupKeySet = Arrays.stream(lookupKeys).boxed().collect(Collectors.toSet());

        if (!isLookupKeysContainsIndex(tableSourceTable, lookupKeySet)) {
            return false;
        }

        // the lookup source must support async lookup
        return LookupJoinUtil.isAsyncLookup(
                tableSourceTable,
                lookupKeySet,
                null, // hint
                false, // upsertMaterialize
                false // preferCustomShuffle
                );
    }

    private static boolean isLookupKeysContainsIndex(
            TableSourceTable tableSourceTable, Set<Integer> lookupKeySet) {
        // the source must have at least one index, and the join key contains one index
        int[][] idxsOfAllIndexes =
                getAllIndexesColumnsFromTableSchema(
                        tableSourceTable.contextResolvedTable().getResolvedSchema());
        if (idxsOfAllIndexes.length == 0) {
            return false;
        }

        final Set<Integer> lookupKeySetPassThroughProjectPushDownSpec;
        Optional<ProjectPushDownSpec> projectPushDownSpec =
                Arrays.stream(tableSourceTable.abilitySpecs())
                        .filter(spec -> spec instanceof ProjectPushDownSpec)
                        .map(spec -> (ProjectPushDownSpec) spec)
                        .findFirst();

        if (projectPushDownSpec.isEmpty()) {
            lookupKeySetPassThroughProjectPushDownSpec = lookupKeySet;
        } else {
            Map<Integer, Integer> mapOut2InPos = new HashMap<>();
            int[][] projectedFields = projectPushDownSpec.get().getProjectedFields();
            for (int i = 0; i < projectedFields.length; i++) {
                int[] projectedField = projectedFields[i];
                // skip nested projection push-down spec
                if (projectedField.length > 1) {
                    continue;
                }
                int input = projectedField[0];
                mapOut2InPos.put(i, input);
            }

            lookupKeySetPassThroughProjectPushDownSpec =
                    lookupKeySet.stream()
                            .flatMap(out -> Stream.ofNullable(mapOut2InPos.get(out)))
                            .collect(Collectors.toSet());
        }

        return Arrays.stream(idxsOfAllIndexes)
                .peek(idxsOfIndex -> Preconditions.checkState(idxsOfIndex.length > 0))
                .anyMatch(
                        idxsOfIndex ->
                                Arrays.stream(idxsOfIndex)
                                        .allMatch(
                                                lookupKeySetPassThroughProjectPushDownSpec
                                                        ::contains));
    }

    private static boolean areAllSourceAbilitySpecsSupported(
            TableScan tableScan, SourceAbilitySpec[] sourceAbilitySpecs) {
        if (!Arrays.stream(sourceAbilitySpecs)
                .allMatch(spec -> ALL_SUPPORTED_ABILITY_SPEC_IN_SOURCE.contains(spec.getClass()))) {
            return false;
        }

        Optional<ReadingMetadataSpec> metadataSpec =
                Arrays.stream(sourceAbilitySpecs)
                        .filter(spec -> spec instanceof ReadingMetadataSpec)
                        .map(spec -> (ReadingMetadataSpec) spec)
                        .findFirst();
        if (metadataSpec.isPresent() && !metadataSpec.get().getMetadataKeys().isEmpty()) {
            return false;
        }

        // source with non-deterministic filter pushed down is not supported
        Optional<FilterPushDownSpec> filterPushDownSpec =
                Arrays.stream(sourceAbilitySpecs)
                        .filter(spec -> spec instanceof FilterPushDownSpec)
                        .map(spec -> (FilterPushDownSpec) spec)
                        .findFirst();
        if (filterPushDownSpec.isEmpty()) {
            return true;
        }

        List<RexNode> filtersOnSource = filterPushDownSpec.get().getPredicates();
        if (!areAllRexNodeDeterministic(filtersOnSource)) {
            return false;
        }

        ChangelogMode changelogMode = getChangelogMode((StreamPhysicalRel) tableScan);
        if (changelogMode.containsOnly(RowKind.INSERT)) {
            return true;
        }

        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(tableScan.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> upsertKeys = fmq.getUpsertKeys(tableScan);
        return filtersOnSource.stream()
                .allMatch(filter -> isFilterOnOneSetOfUpsertKeys(filter, upsertKeys));
    }

    private static TableScan getTableScan(RelNode node) {
        node = unwrapNode(node, true);
        // support to get table across more nodes if we support more nodes in
        // `ALL_SUPPORTED_DELTA_JOIN_UPSTREAM_NODES`
        if (node instanceof StreamPhysicalExchange
                || node instanceof StreamPhysicalDropUpdateBefore
                || node instanceof StreamPhysicalCalc) {
            return getTableScan(node.getInput(0));
        }

        Preconditions.checkState(node instanceof TableScan);
        return (TableScan) node;
    }

    private static boolean areAllUpstreamCalcSupported(StreamPhysicalJoin join) {
        return areAllUpstreamCalcFromOneJoinInputSupported(join.getLeft())
                && areAllUpstreamCalcFromOneJoinInputSupported(join.getRight());
    }

    private static boolean areAllUpstreamCalcFromOneJoinInputSupported(RelNode joinInput) {
        List<Calc> calcListFromThisInput = collectCalcBetweenJoinAndTableScan(joinInput);

        // currently, at most one calc is allowed to appear between Join and TableScan
        if (calcListFromThisInput.size() > 1) {
            return false;
        }

        if (calcListFromThisInput.isEmpty()) {
            return true;
        }

        Calc calc = calcListFromThisInput.get(0);
        return isCalcSupported(calc);
    }

    private static Optional<Calc> getCalcBetweenJoinAndTableScan(RelNode joinInput) {
        List<Calc> calcListFromLeftInput = collectCalcBetweenJoinAndTableScan(joinInput);
        Preconditions.checkState(
                calcListFromLeftInput.size() <= 1,
                "Should be validated before calling this function");
        if (calcListFromLeftInput.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(calcListFromLeftInput.get(0));
        }
    }

    private static Optional<RexProgram> getRexProgramBetweenJoinAndTableScan(RelNode joinInput) {
        return getCalcBetweenJoinAndTableScan(joinInput).map(Calc::getProgram);
    }

    private static List<Calc> collectCalcBetweenJoinAndTableScan(RelNode joinInput) {
        CalcCollector calcCollector = new CalcCollector();
        calcCollector.go(joinInput);
        return calcCollector.collectResult;
    }

    private static boolean isCalcSupported(Calc calc) {
        RexProgram calcProgram = calc.getProgram();
        // calc with non-deterministic fields or filters is not supported
        return calcProgram == null || areAllRexNodeDeterministic(calcProgram.getExprList());
    }

    private static boolean areAllRexNodeDeterministic(List<RexNode> rexNodes) {
        // Delta joins may produce duplicate data, and when this data is sent downstream, we want it
        // to be processed in an idempotent manner. However, the presence of non-deterministic
        // functions can lead to unpredictable results, such as random filtering or the addition of
        // non-deterministic columns. Therefore, we strictly prohibit the use of non-deterministic
        // functions in this context to ensure consistent and reliable processing.
        return rexNodes.stream().allMatch(RexUtil::isDeterministic);
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

    private static boolean areAllInputsInsertOrUpdateAfter(StreamPhysicalJoin join) {
        for (RelNode input : join.getInputs()) {
            if (!onlyProduceInsertOrUpdateAfter(unwrapNode(input, false))) {
                return false;
            }
        }
        return true;
    }

    private static boolean onlyProduceInsertOrUpdateAfter(StreamPhysicalRel node) {
        ChangelogMode changelogMode = getChangelogMode(node);
        Set<RowKind> allKinds = changelogMode.getContainedKinds();
        return !allKinds.contains(RowKind.UPDATE_BEFORE) && !allKinds.contains(RowKind.DELETE);
    }

    private static ChangelogMode getChangelogMode(StreamPhysicalRel node) {
        return JavaScalaConversionUtil.toJava(ChangelogPlanUtils.getChangelogMode(node))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        String.format(
                                                "Unable to derive changelog mode from node %s. This is a bug.",
                                                node)));
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

    private static class CalcCollector extends RelVisitor {

        private final List<Calc> collectResult = new ArrayList<>();

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            node = unwrapNode(node, true);

            super.visit(node, ordinal, parent);

            if (node instanceof Calc) {
                collectResult.add((Calc) node);
            }
        }
    }
}
