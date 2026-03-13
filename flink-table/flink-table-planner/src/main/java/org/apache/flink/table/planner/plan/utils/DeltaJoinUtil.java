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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Index;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.PartitionPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinAssociation;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinLookupChain;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinTree;
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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
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
    private static final Set<Class<?>> ALL_SUPPORTED_NODES_BEFORE_DELTA_JOIN =
            Sets.newHashSet(
                    StreamPhysicalTableSourceScan.class,
                    StreamPhysicalExchange.class,
                    StreamPhysicalDropUpdateBefore.class,
                    StreamPhysicalCalc.class);

    /**
     * All allowed nodes between cascaded delta joins. Only the following nodes are allowed to exist
     * between cascaded delta joins. Otherwise, the join downstream will not be converted to delta
     * join.
     *
     * <p>More physical nodes can be added to support more patterns for delta join.
     */
    private static final Set<Class<?>> ALL_ALLOWED_NODES_BETWEEN_CASCADED_DELTA_JOIN =
            Sets.newHashSet(StreamPhysicalExchange.class, StreamPhysicalCalc.class);

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

        if (!areAllJoinInputsInWhiteList(join.getLeft(), new ArrayDeque<>())
                || !areAllJoinInputsInWhiteList(join.getRight(), new ArrayDeque<>())) {
            return false;
        }

        if (!areAllUpstreamCalcSupported(join)) {
            return false;
        }

        return areInputTableScansSupported(join);
    }

    /** Extract the delta join spec used for {@link StreamPhysicalDeltaJoin}. */
    public static DeltaJoinSpec getDeltaJoinSpec(
            JoinInfo joinInfo,
            TableSourceTable lookupTable,
            @Nullable RexProgram calcOnLookupTable,
            RelOptCluster cluster,
            boolean treatRightAsLookupSide) {
        RexBuilder rexBuilder = cluster.getRexBuilder();

        RexNode condition = RexUtil.composeConjunction(rexBuilder, joinInfo.nonEquiConditions);
        Optional<RexNode> remainingCondition =
                condition.isAlwaysTrue() ? Optional.empty() : Optional.of(condition);

        IntPair[] streamToLookupJoinPair =
                treatRightAsLookupSide
                        ? joinInfo.pairs().toArray(new IntPair[0])
                        : reverseIntPairs(joinInfo.pairs().toArray(new IntPair[0]));
        final IntPair[] streamToLookupJoinKeys =
                TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                        streamToLookupJoinPair,
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(calcOnLookupTable)));

        Map<Integer, FunctionCallUtil.FunctionParam> allLookupKeys =
                analyzerDeltaJoinLookupKeys(streamToLookupJoinKeys);

        Tuple2<Optional<List<RexNode>>, Optional<RexNode>> projectionAndFilter =
                splitProjectionAndFilter(calcOnLookupTable);

        return new DeltaJoinSpec(
                new TemporalTableSourceSpec(lookupTable),
                allLookupKeys,
                remainingCondition.orElse(null),
                projectionAndFilter.f0.orElse(null),
                projectionAndFilter.f1.orElse(null));
    }

    /**
     * Split the projection and filter from the {@link RexProgram}. If the {@link RexProgram} is
     * null, return empty projection and filter.
     */
    public static Tuple2<Optional<List<RexNode>>, Optional<RexNode>> splitProjectionAndFilter(
            @Nullable RexProgram rexProgram) {
        if (rexProgram == null) {
            return Tuple2.of(Optional.empty(), Optional.empty());
        }
        Tuple2<List<RexNode>, Option<RexNode>> projectionsAndFilter =
                JavaScalaConversionUtil.toJava(FlinkRexUtil.expandRexProgram(rexProgram));
        return Tuple2.of(
                Optional.of(projectionsAndFilter.f0),
                JavaScalaConversionUtil.toJava(projectionsAndFilter.f1));
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
     * Try to build lookup chain for delta join to do lookup.
     *
     * <p>Take the following join tree as example. Each leaf table has columns named with its
     * lowercase letter and a number, e.g., A(a0, a1), B(b0, b1, b2), C(c0, c1), D(d0, d1, d2).
     *
     * <pre>{@code
     *                     Top
     *            (a1 = c1 and b2 = d2)
     *            /                       \
     *       Bottom1                      Bottom2
     *      (a0 = b0)                    (c0 = d0)
     *    /         \                    /       \
     * A(a0,a1)  B(b0,b1,b2)        C(c0,c1)  D(d0,d1,d2)
     *
     * }</pre>
     *
     * <p>If Bottom1 is treated as stream side and Bottom2 is treated as lookup side, the lookup
     * chain will be like this:
     *
     * <p>use A + B to lookup C with (a1 = c1) -> use C to lookup D with (c0 = d0).
     */
    public static DeltaJoinLookupChain buildLookupChainAndUpdateTopJoinAssociation(
            JoinSpec topJoinSpec,
            List<IntPair> joinKeysForLeftToRight,
            DeltaJoinAssociation joinAssociationOnLeft,
            DeltaJoinAssociation joinAssociationOnRight,
            RelNode topLeftSide,
            RelNode topRightSide,
            // if true, left is treated as stream side with bottom1;
            // otherwise right is treated as stream side with bottom1
            boolean leftIsStreamSide,
            DeltaJoinAssociation topJoinAssociation,
            @Nullable RexProgram calcOnLookupSide,
            FlinkTypeFactory typeFactory) {

        Preconditions.checkArgument(
                !joinKeysForLeftToRight.isEmpty(),
                "There must be at least one equality condition on the join condition.");

        DeltaJoinAssociation joinAssociationInBottom2 =
                leftIsStreamSide ? joinAssociationOnRight : joinAssociationOnLeft;
        IntPair[] joinKeysForBottom1To2 =
                leftIsStreamSide
                        ? joinKeysForLeftToRight.toArray(new IntPair[0])
                        : reverseIntPairs(joinKeysForLeftToRight.toArray(new IntPair[0]));

        IntPair[] joinKeysFromBottom1To2TransposedFromCalc =
                getJoinKeyPassThroughCalc(joinKeysForBottom1To2, calcOnLookupSide);

        List<Tuple2<DeltaJoinAssociation.BinaryInputInfo, IntPair[]>>
                joinKeyOfDifferentBinaryTablesOnBottom2 =
                        splitJoinKeysOfDifferentBinaryTablesOnLookupSide(
                                joinKeysFromBottom1To2TransposedFromCalc,
                                joinAssociationInBottom2,
                                typeFactory);

        String joinKeyErrorMessage =
                joinKeysToString(
                        joinKeysForLeftToRight,
                        topLeftSide.getRowType().getFieldNames(),
                        topRightSide.getRowType().getFieldNames());

        Tuple2<LookupBinaryInputInfo, DeltaJoinSpec> pickedBinaryInput =
                pickAnyBinaryTableOnLookupSideToLookup(
                        topJoinSpec,
                        joinAssociationInBottom2,
                        joinKeyOfDifferentBinaryTablesOnBottom2,
                        joinKeyErrorMessage);
        DeltaJoinSpec pickedBinaryInputDeltaJoinSpec = pickedBinaryInput.f1;

        int[] streamSideBinaryInputOrdinalsWithOffset =
                leftIsStreamSide
                        ? joinAssociationOnLeft.getAllBinaryInputOrdinals().stream()
                                .mapToInt(i -> i)
                                .toArray()
                        : joinAssociationOnRight
                                .getAllBinaryInputOrdinalsWithOffset(
                                        joinAssociationOnLeft.getBinaryInputCount())
                                .stream()
                                .mapToInt(i -> i)
                                .toArray();

        final FlinkJoinType stream2LookupSideJoinType =
                leftIsStreamSide
                        ? topJoinSpec.getJoinType()
                        : swapJoinType(topJoinSpec.getJoinType());
        int lookupSideBinaryOrdinalShift =
                leftIsStreamSide ? joinAssociationOnLeft.getBinaryInputCount() : 0;
        int pickedBinaryInputOrdinal = pickedBinaryInput.f0.binaryInputOrdinal;
        int pickedBinaryInputOrdinalOnTopJoin =
                pickedBinaryInputOrdinal + lookupSideBinaryOrdinalShift;
        int totalLookupCount = joinAssociationInBottom2.getBinaryInputCount();

        topJoinAssociation.addJoinAssociation(
                Arrays.stream(streamSideBinaryInputOrdinalsWithOffset)
                        .boxed()
                        .collect(Collectors.toSet()),
                pickedBinaryInputOrdinalOnTopJoin,
                DeltaJoinAssociation.Association.of(
                        stream2LookupSideJoinType, pickedBinaryInputDeltaJoinSpec));

        return buildLookupChain(
                streamSideBinaryInputOrdinalsWithOffset,
                pickedBinaryInput.f0,
                pickedBinaryInputDeltaJoinSpec,
                joinAssociationInBottom2,
                lookupSideBinaryOrdinalShift,
                stream2LookupSideJoinType,
                totalLookupCount);
    }

    /**
     * Split the join keys on the lookup side into different binary inputs.
     *
     * <p>If the lookup side has multi calc between top join and scan, the returned join keys will
     * transpose all these calc.
     *
     * <p>Take the following join tree as example. Each leaf table has columns named with its
     * lowercase letter and a number, e.g., A(a0, a1), B(b0, b1, b2), C(c0, c1), D(d0, d1, d2).
     *
     * <pre>{@code
     *                    Top
     *            (a1 = c1 and b2 = d2)
     *         /                        \
     *       Bottom1                   Bottom2
     *      (a0 = b0)                  (c0 = d0)
     *     /       \                    /      \
     * A(a0,a1)  B(b0,b1,b2)        C(c0,c1)  D(d0,d1,d2)
     *
     * }</pre>
     *
     * <p>If Bottom1 is stream side, the result will be {@code [(C, <a1, c1>), (D, <b2, d2>)]}.
     *
     * <p>If there are no join keys on one binary table {@code i}, the result will contain {@code i}
     * with empty list.
     */
    private static List<Tuple2<DeltaJoinAssociation.BinaryInputInfo, IntPair[]>>
            splitJoinKeysOfDifferentBinaryTablesOnLookupSide(
                    IntPair[] joinKeysForStreamSide2LookupSide,
                    DeltaJoinAssociation joinAssociationInLookupSide,
                    FlinkTypeFactory typeFactory) {
        SplitJoinKeyVisitor visitor =
                new SplitJoinKeyVisitor(typeFactory, joinKeysForStreamSide2LookupSide);

        visitor.visit(joinAssociationInLookupSide.getJoinTree());
        LinkedHashMap<Integer, IntPair[]> splitResult = visitor.result;
        Preconditions.checkState(
                splitResult.size() == joinAssociationInLookupSide.getBinaryInputCount());
        List<Tuple2<DeltaJoinAssociation.BinaryInputInfo, IntPair[]>> result = new ArrayList<>();
        for (Map.Entry<Integer, IntPair[]> inputOrdWithJoinKey : splitResult.entrySet()) {
            int inputOrd = inputOrdWithJoinKey.getKey();
            IntPair[] joinKey = inputOrdWithJoinKey.getValue();
            result.add(
                    Tuple2.of(joinAssociationInLookupSide.getBinaryInputInfo(inputOrd), joinKey));
        }
        return result;
    }

    private static Tuple2<LookupBinaryInputInfo, DeltaJoinSpec>
            pickAnyBinaryTableOnLookupSideToLookup(
                    JoinSpec topJoinSpec,
                    DeltaJoinAssociation joinAssociationInLookupSide,
                    List<Tuple2<DeltaJoinAssociation.BinaryInputInfo, IntPair[]>>
                            joinKeyOfDifferentBinaryTablesOnLookupSide,
                    String joinKeyErrorMessage) {
        LookupBinaryInputInfo pickedBinaryInputInfo =
                pickAnyBinaryTableOnLookupSideToLookup(
                        joinKeyOfDifferentBinaryTablesOnLookupSide, joinKeyErrorMessage);

        Map<Integer, LookupJoinUtil.FunctionParam> lookupKeysOnThisLookupBinaryInput =
                pickedBinaryInputInfo.lookupKeysOnThisBinaryInput;
        DeltaJoinAssociation.BinaryInputInfo pickedBinaryInput =
                pickedBinaryInputInfo.binaryInputInfo;

        // begin to build lookup chain for bottom1 to lookup C or D in bottom2
        int lookupCount = joinAssociationInLookupSide.getBinaryInputCount();
        DeltaJoinSpec deltaJoinSpec =
                buildDeltaJoinSpecForStreamSide2PickedLookupBinaryInput(
                        topJoinSpec,
                        pickedBinaryInput,
                        lookupKeysOnThisLookupBinaryInput,
                        lookupCount);

        return Tuple2.of(pickedBinaryInputInfo, deltaJoinSpec);
    }

    /** Pick any binary input as lookup input to do first lookup. */
    private static LookupBinaryInputInfo pickAnyBinaryTableOnLookupSideToLookup(
            List<Tuple2<DeltaJoinAssociation.BinaryInputInfo, IntPair[]>>
                    joinKeyOfDifferentLookupBinaryTables,
            String joinKeyErrorMessage) {
        // select all binary tables on bottom2 that can be looked up
        // for example if [c1] is an index on C and [d2] is an index on D, the result will be:
        // [<C, <c1, a1>>, <D, <d2, b2>>]
        List<Tuple2<Integer, IntPair[]>> pickedBinaryTablesToLookup =
                new ArrayList<>(
                        pickBinaryTablesThatCanLookup(
                                joinKeyOfDifferentLookupBinaryTables, joinKeyErrorMessage));
        Preconditions.checkState(!pickedBinaryTablesToLookup.isEmpty());

        // pick the first (leftest) binary input to lookup
        // TODO consider query hint specified by user
        Tuple2<Integer, IntPair[]> pickedBinaryTable = pickedBinaryTablesToLookup.get(0);

        DeltaJoinAssociation.BinaryInputInfo pickedBinaryInputInfo =
                joinKeyOfDifferentLookupBinaryTables.get(pickedBinaryTable.f0).f0;
        Map<Integer, LookupJoinUtil.FunctionParam> lookupKeysOnThisLookupBinaryInput =
                analyzerDeltaJoinLookupKeys(pickedBinaryTable.f1);

        return LookupBinaryInputInfo.of(
                pickedBinaryTable.f0, pickedBinaryInputInfo, lookupKeysOnThisLookupBinaryInput);
    }

    /**
     * Pick the tables that can be looked up by the given join keys.
     *
     * <p>If the table can be picked, that means the join keys contain one of its indexes.
     *
     * @return the f0 of the list element is the picked table's idx, the f1 of the list element is
     *     its lookup keys.
     */
    private static List<Tuple2<Integer, IntPair[]>> pickBinaryTablesThatCanLookup(
            List<Tuple2<DeltaJoinAssociation.BinaryInputInfo, IntPair[]>>
                    joinKeyOnDifferentBinaryInputs,
            String joinKeyErrorMsg) {
        List<Tuple2<Integer, IntPair[]>> result = new ArrayList<>();

        for (int i = 0; i < joinKeyOnDifferentBinaryInputs.size(); i++) {
            DeltaJoinAssociation.BinaryInputInfo binaryInput =
                    joinKeyOnDifferentBinaryInputs.get(i).f0;
            if (joinKeyOnDifferentBinaryInputs.get(i).f1.length == 0) {
                continue;
            }
            IntPair[] joinKeys = joinKeyOnDifferentBinaryInputs.get(i).f1;

            if (isTableScanSupported(binaryInput.tableScan, getTargetOrdinals(joinKeys))) {
                result.add(Tuple2.of(i, joinKeys));
            }
        }

        if (!result.isEmpty()) {
            return result;
        }

        // should not happen because we have validated before
        List<TableSourceTable> allTables =
                joinKeyOnDifferentBinaryInputs.stream()
                        .map(t -> t.f0.tableScan.tableSourceTable())
                        .collect(Collectors.toList());

        String errorMsg =
                String.format(
                        "The join key [%s] does not include all primary keys nor "
                                + "all fields from an index of any table on the other side.\n"
                                + "All indexes about tables on the other side are:\n\n%s",
                        joinKeyErrorMsg, allTableIndexDetailMessageToString(allTables));
        throw new TableException(
                "This is a bug and should not happen. Please file an issue. The detail message is:\n"
                        + errorMsg);
    }

    /**
     * Build the delta join spec for stream side to picked lookup binary input.
     *
     * <p>Take the following join tree as example. Each leaf table has columns named with its
     * lowercase letter and a number, e.g., A(a0, a1), B(b0, b1, b2), C(c0, c1), D(d0, d1, d2).
     *
     * <pre>{@code
     *                       Top
     *              (a1 = c1 and b2 = d2)
     *              /                      \
     *        Bottom1                     Bottom2
     *      (a0 = b0)                    (c0 = d0)
     *    /         \                    /      \
     * A(a0,a1)  B(b0,b1,b2)        C(c0,c1)  D(d0,d1,d2)
     *
     * }</pre>
     *
     * <p>If Bottom1 is stream side, and choose to lookup C first. Then this function is used to
     * build the delta join spec for Bottom1 to lookup C.
     */
    private static DeltaJoinSpec buildDeltaJoinSpecForStreamSide2PickedLookupBinaryInput(
            JoinSpec topJoinSpec,
            DeltaJoinAssociation.BinaryInputInfo pickedLookupBinaryInput,
            Map<Integer, LookupJoinUtil.FunctionParam> lookupKeysOnPickedLookupBinaryInput,
            int totalLookupCount) {

        // TODO:
        //  1. split remaining join condition into the pre-filter and remaining parts
        //  2. support Constant functionParam
        Optional<RexNode> nonEquivCondition = topJoinSpec.getNonEquiCondition();

        // TODO push down the non-equiv deterministic conditions on each inputs by
        //  RelOptUtil.classifyFilters to get less data for subsequent cascading lookups
        Optional<RexNode> remainingCondition =
                totalLookupCount == 1 && nonEquivCondition.isPresent()
                        ? nonEquivCondition
                        : Optional.empty();

        RexProgram lookupSideCalc = pickedLookupBinaryInput.calcOnTable;
        Tuple2<Optional<List<RexNode>>, Optional<RexNode>> projectionAndFilter =
                splitProjectionAndFilter(lookupSideCalc);

        return new DeltaJoinSpec(
                new TemporalTableSourceSpec(pickedLookupBinaryInput.tableScan.tableSourceTable()),
                lookupKeysOnPickedLookupBinaryInput,
                remainingCondition.orElse(null),
                projectionAndFilter.f0.orElse(null),
                projectionAndFilter.f1.orElse(null));
    }

    private static String allTableIndexDetailMessageToString(List<TableSourceTable> allTables) {
        StringBuilder tableDetailMsgBuilder = new StringBuilder();
        for (TableSourceTable table : allTables) {
            List<List<String>> allFieldsAboutAllIndexes =
                    getAllIndexesColumnNamesFromTableSchema(
                            table.contextResolvedTable().getResolvedSchema());
            tableDetailMsgBuilder.append(
                    getTableIndexDetailErrorMessage(table, allFieldsAboutAllIndexes));
            tableDetailMsgBuilder.append("\n");
        }
        return tableDetailMsgBuilder.toString();
    }

    private static String getTableIndexDetailErrorMessage(
            TableSourceTable table, List<List<String>> indexes) {
        StringBuilder sb = new StringBuilder();
        String tableName = table.contextResolvedTable().getIdentifier().asSummaryString();

        String indexesStr;
        if (indexes.isEmpty()) {
            indexesStr = "N/A";
        } else {
            indexesStr =
                    indexes.stream()
                            .map(
                                    fields ->
                                            fields.stream()
                                                    .collect(Collectors.joining(", ", "(", ")")))
                            .collect(Collectors.joining(", ", "[", "]"));
        }
        sb.append(String.format("All indexes of the table [%s] is %s.", tableName, indexesStr));
        return sb.toString();
    }

    private static DeltaJoinLookupChain buildLookupChain(
            int[] streamSideBinaryInputOrdinalsWithOffset,
            LookupBinaryInputInfo pickedBinaryInputInfo,
            DeltaJoinSpec pickedBinaryInputDeltaJoinSpec,
            DeltaJoinAssociation joinAssociationInLookupSide,
            int lookupSideBinaryOrdinalShift,
            FlinkJoinType stream2LookupSideJoinType,
            int totalLookupCount) {
        DeltaJoinLookupChain lookupChain = DeltaJoinLookupChain.newInstance();
        // add the first lookup table to the lookup chain
        lookupChain.addNode(
                DeltaJoinLookupChain.Node.of(
                        streamSideBinaryInputOrdinalsWithOffset,
                        pickedBinaryInputInfo.binaryInputOrdinal + lookupSideBinaryOrdinalShift,
                        pickedBinaryInputDeltaJoinSpec,
                        stream2LookupSideJoinType));

        // binary input ordinals that have been added to the lookup chain
        Set<Integer> visited = new HashSet<>();
        visited.add(pickedBinaryInputInfo.binaryInputOrdinal);

        collectNextLookupBinaryInputsWithSingleSourceInput(
                joinAssociationInLookupSide,
                visited,
                pickedBinaryInputInfo.binaryInputOrdinal,
                totalLookupCount,
                lookupSideBinaryOrdinalShift,
                lookupChain);

        collectNextLookupBinaryInputsWithCompositeSourceInputs(
                joinAssociationInLookupSide,
                visited,
                totalLookupCount,
                lookupSideBinaryOrdinalShift,
                lookupChain);

        if (visited.size() < totalLookupCount) {
            throw new TableException(
                    String.format(
                            "This is a bug and should not happen. Please file an issue. The detail message is:\n"
                                    + "Visited inputs: %s\n"
                                    + "Total lookup count is: %d\n"
                                    + "All join associations on lookup side are: %s",
                            visited,
                            totalLookupCount,
                            joinAssociationInLookupSide.getAssociationSummary()));
        }

        return lookupChain;
    }

    private static void collectNextLookupBinaryInputsWithSingleSourceInput(
            DeltaJoinAssociation deltaJoinAssociation,
            Set<Integer> visited,
            int inputOrdinal,
            int targetLookupCount,
            int inputBinaryOrdinalShift,
            DeltaJoinLookupChain collector) {

        Preconditions.checkState(visited.size() <= targetLookupCount);
        if (visited.size() == targetLookupCount) {
            return;
        }

        Map<Integer, DeltaJoinAssociation.Association> targets =
                findAvailableLookupTargets(deltaJoinAssociation, visited, inputOrdinal);
        if (targets.isEmpty()) {
            return;
        }

        // pick anyone of them
        // here we pick the smallest (leftest) one to get a stable result
        Optional<Integer> minTargetOrd = targets.keySet().stream().min(Integer::compareTo);
        int targetOrd = minTargetOrd.get();
        DeltaJoinAssociation.Association association = targets.get(targetOrd);
        DeltaJoinLookupChain.Node node =
                DeltaJoinLookupChain.Node.of(
                        inputOrdinal + inputBinaryOrdinalShift,
                        targetOrd + inputBinaryOrdinalShift,
                        association.deltaJoinSpec,
                        association.joinType);
        collector.addNode(node);
        visited.add(targetOrd);

        collectNextLookupBinaryInputsWithSingleSourceInput(
                deltaJoinAssociation,
                visited,
                targetOrd,
                targetLookupCount,
                inputBinaryOrdinalShift,
                collector);
    }

    private static void collectNextLookupBinaryInputsWithCompositeSourceInputs(
            DeltaJoinAssociation deltaJoinAssociation,
            Set<Integer> visited,
            int targetLookupCount,
            int lookupSideBinaryOrdinalShift,
            DeltaJoinLookupChain collector) {

        Preconditions.checkState(visited.size() <= targetLookupCount);
        if (visited.size() == targetLookupCount) {
            return;
        }

        // blacklist to avoid re-visit when retrying
        Set<Set<Integer>> visitedCompositeJoinAssociations = new HashSet<>();

        // sort them to get a stable result
        List<Map.Entry<Set<Integer>, Map<Integer, DeltaJoinAssociation.Association>>>
                orderedEachCompositeJoinAssociations =
                        deltaJoinAssociation
                                .getCompositeBinary2BinaryJoinAssociation()
                                .entrySet()
                                .stream()
                                .sorted(Comparator.comparing(e -> e.getKey().toString()))
                                .collect(Collectors.toList());

        while (true) {
            boolean changed = false;
            for (Map.Entry<Set<Integer>, Map<Integer, DeltaJoinAssociation.Association>>
                    eachComposite : orderedEachCompositeJoinAssociations) {
                if (visitedCompositeJoinAssociations.contains(eachComposite.getKey())) {
                    continue;
                }
                if (!visited.containsAll(eachComposite.getKey())) {
                    continue;
                }

                // pick anyone of them
                // here we pick the smallest one to get a stable result
                OptionalInt targetOrdOp =
                        eachComposite.getValue().keySet().stream()
                                .filter(t -> !visited.contains(t))
                                .mapToInt(t -> t)
                                .min();
                if (targetOrdOp.isEmpty()) {
                    visitedCompositeJoinAssociations.add(eachComposite.getKey());
                    continue;
                }
                int targetOrd = targetOrdOp.getAsInt();
                DeltaJoinAssociation.Association association =
                        eachComposite.getValue().get(targetOrd);
                DeltaJoinLookupChain.Node node =
                        DeltaJoinLookupChain.Node.of(
                                eachComposite.getKey().stream()
                                        .sorted()
                                        .mapToInt(i -> i + lookupSideBinaryOrdinalShift)
                                        .toArray(),
                                targetOrd + lookupSideBinaryOrdinalShift,
                                association.deltaJoinSpec,
                                association.joinType);

                collector.addNode(node);
                visited.add(targetOrd);

                collectNextLookupBinaryInputsWithSingleSourceInput(
                        deltaJoinAssociation,
                        visited,
                        targetOrd,
                        targetLookupCount,
                        lookupSideBinaryOrdinalShift,
                        collector);

                if (visited.size() == targetLookupCount) {
                    return;
                }

                changed = true;
                break;
            }
            // new inputs are added to the `visited` set, try again to collect next lookup
            // binary inputs
            if (changed) {
                continue;
            }
            // all composite join associations are visited, no need to do the loop
            break;
        }
    }

    private static Map<Integer, DeltaJoinAssociation.Association> findAvailableLookupTargets(
            DeltaJoinAssociation deltaJoinAssociation, Set<Integer> visited, int inputOrdinal) {
        Map<Integer, DeltaJoinAssociation.Association> destOrdinalAndAssociations =
                deltaJoinAssociation.getDestOrdinalAndAssociations(inputOrdinal);
        return destOrdinalAndAssociations.entrySet().stream()
                .filter(e -> !visited.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Get the lookup key from the join keys.
     *
     * <p>Different with {@see CommonPhysicalLookupJoin#analyzeLookupKeys}, we have not supported
     * {@link FunctionCallUtil.Constant}.
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

    /** Combine the output row type of the delta join. */
    public static RowType combineOutputRowType(
            RowType leftRowType,
            RowType rightRowType,
            FlinkJoinType flinkJoinType,
            FlinkTypeFactory typeFactory) {
        RelDataType leftRelType = typeFactory.buildRelNodeRowType(leftRowType);
        RelDataType rightRelType = typeFactory.buildRelNodeRowType(rightRowType);
        JoinRelType joinType = toJoinRelType(flinkJoinType);
        RelDataType joinRelType =
                SqlValidatorUtil.deriveJoinRowType(
                        leftRelType,
                        rightRelType,
                        joinType,
                        typeFactory,
                        null,
                        Collections.emptyList());
        return FlinkTypeFactory.toLogicalRowType(joinRelType);
    }

    private static JoinRelType toJoinRelType(FlinkJoinType flinkJoinType) {
        switch (flinkJoinType) {
            case INNER:
                return JoinRelType.INNER;
            case LEFT:
                return JoinRelType.LEFT;
            case RIGHT:
                return JoinRelType.RIGHT;
            case FULL:
                return JoinRelType.FULL;
            default:
                throw new IllegalStateException("Unsupported join type: " + flinkJoinType);
        }
    }

    public static IntPair[] reverseIntPairs(IntPair[] intPairs) {
        return Arrays.stream(intPairs)
                .map(pair -> new IntPair(pair.target, pair.source))
                .toArray(IntPair[]::new);
    }

    public static int[] getTargetOrdinals(IntPair[] intPairs) {
        return Arrays.stream(intPairs).mapToInt(pair -> pair.target).toArray();
    }

    /** Print all join keys as readable string. */
    public static String joinKeysToString(
            List<IntPair> joinKeys, List<String> leftFieldNames, List<String> rightFieldNames) {
        return joinKeys.stream()
                .map(
                        pair -> {
                            String template = "%s=%s";
                            return String.format(
                                    template,
                                    leftFieldNames.get(pair.source),
                                    rightFieldNames.get(pair.target));
                        })
                .collect(java.util.stream.Collectors.joining(", "));
    }

    private static int[][] getAllIndexesColumnsFromTableSchema(ResolvedSchema schema) {
        List<List<String>> columnsOfIndexes = getAllIndexesColumnNamesFromTableSchema(schema);
        int[][] results = new int[columnsOfIndexes.size()][];
        for (int i = 0; i < columnsOfIndexes.size(); i++) {
            List<String> fieldNames = schema.getColumnNames();
            results[i] = columnsOfIndexes.get(i).stream().mapToInt(fieldNames::indexOf).toArray();
        }

        return results;
    }

    public static List<List<String>> getAllIndexesColumnNamesFromTableSchema(
            ResolvedSchema schema) {
        List<Index> indexes = schema.getIndexes();
        return indexes.stream().map(Index::getColumns).collect(Collectors.toList());
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

    private static boolean areInputTableScansSupported(StreamPhysicalJoin join) {
        JoinInfo joinInfo =
                JoinUtil.createJoinInfo(
                        join.getLeft(), join.getRight(), join.getCondition(), new ArrayList<>());
        IntPair[] joinPair = joinInfo.pairs().toArray(new IntPair[0]);

        return areInputTableScansSupported(join.getLeft(), reverseIntPairs(joinPair))
                && areInputTableScansSupported(join.getRight(), joinPair);
    }

    private static boolean areInputTableScansSupported(
            RelNode lookupSideInput, IntPair[] streamToLookupSideJoinKey) {
        if (streamToLookupSideJoinKey.length == 0) {
            return false;
        }

        Optional<StreamPhysicalDeltaJoin> lookupSideBottomDeltaJoinOp =
                getDeltaJoin(lookupSideInput);

        if (lookupSideBottomDeltaJoinOp.isEmpty()) {
            Optional<RexProgram> calcOnLookupTable =
                    getRexProgramBetweenJoinAndTableScan(lookupSideInput);
            IntPair[] joinKeyOnLookupTable =
                    TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                            streamToLookupSideJoinKey,
                            JavaScalaConversionUtil.toScala(calcOnLookupTable));
            return isTableScanSupported(
                    getTableScan(lookupSideInput), getTargetOrdinals(joinKeyOnLookupTable));
        }

        StreamPhysicalDeltaJoin lookupSideBottomDeltaJoin = lookupSideBottomDeltaJoinOp.get();
        Optional<RexProgram> calcOnDeltaJoin =
                getRexProgramBetweenJoinAndDeltaJoin(lookupSideInput);
        IntPair[] joinKeyOnDeltaJoin =
                TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                        streamToLookupSideJoinKey,
                        JavaScalaConversionUtil.toScala(calcOnDeltaJoin));

        Tuple2<IntPair[], IntPair[]> joinKeyOnDeltaJoinLeftRight =
                splitJoinKeyOnDeltaJoinLeftRightSide(lookupSideBottomDeltaJoin, joinKeyOnDeltaJoin);
        // use 'or' to check if the join key contains any index in any source
        return areInputTableScansSupported(
                        lookupSideBottomDeltaJoin.getLeft(), joinKeyOnDeltaJoinLeftRight.f0)
                || areInputTableScansSupported(
                        lookupSideBottomDeltaJoin.getRight(), joinKeyOnDeltaJoinLeftRight.f1);
    }

    private static Tuple2<IntPair[], IntPair[]> splitJoinKeyOnDeltaJoinLeftRightSide(
            StreamPhysicalDeltaJoin lookupSideBottomDeltaJoin, IntPair[] joinKeyOnDeltaJoin) {
        List<IntPair> left = new ArrayList<>();
        List<IntPair> right = new ArrayList<>();
        int leftFieldCnt = lookupSideBottomDeltaJoin.getLeft().getRowType().getFieldCount();
        for (IntPair jk : joinKeyOnDeltaJoin) {
            if (jk.target < leftFieldCnt) {
                left.add(jk);
            } else {
                right.add(IntPair.of(jk.source, jk.target - leftFieldCnt));
            }
        }
        return Tuple2.of(left.toArray(new IntPair[0]), right.toArray(new IntPair[0]));
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

        final Set<Integer> lookupKeySetPassThroughProjectPushDownSpec =
                getLookupKeyPassThroughSourceSpec(lookupKeySet, tableSourceTable);

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

    public static StreamPhysicalTableSourceScan getTableScan(RelNode node) {
        node = unwrapNode(node, true);
        // support to get table across more nodes if we support more nodes in
        // `ALL_SUPPORTED_NODES_BEFORE_DELTA_JOIN`
        if (node instanceof StreamPhysicalExchange
                || node instanceof StreamPhysicalDropUpdateBefore
                || node instanceof StreamPhysicalCalc) {
            return getTableScan(node.getInput(0));
        }

        Preconditions.checkState(node instanceof StreamPhysicalTableSourceScan);
        return (StreamPhysicalTableSourceScan) node;
    }

    public static Optional<StreamPhysicalDeltaJoin> getDeltaJoin(RelNode node) {
        node = unwrapNode(node, true);
        // support to get delta join across more nodes if we support more nodes in
        // `ALL_ALLOWED_NODES_BETWEEN_CASCADED_DELTA_JOIN`
        if (node instanceof StreamPhysicalExchange || node instanceof StreamPhysicalCalc) {
            return getDeltaJoin(node.getInput(0));
        }
        if (node instanceof StreamPhysicalDeltaJoin) {
            return Optional.of((StreamPhysicalDeltaJoin) node);
        }
        return Optional.empty();
    }

    public static FlinkJoinType swapJoinType(FlinkJoinType joinType) {
        return JoinTypeUtil.getFlinkJoinType(swapJoinType(toJoinRelType(joinType)));
    }

    public static JoinRelType swapJoinType(JoinRelType joinType) {
        switch (joinType) {
            case INNER:
                return JoinRelType.INNER;
            case LEFT:
                return JoinRelType.RIGHT;
            case RIGHT:
                return JoinRelType.LEFT;
            case FULL:
                return JoinRelType.FULL;
            default:
                throw new IllegalStateException("Unsupported join type: " + joinType);
        }
    }

    private static boolean areAllUpstreamCalcSupported(StreamPhysicalJoin join) {
        final boolean isLeftSupported;
        final boolean isRightSupported;

        // If the input has a delta join, that means this join is a cascaded delta join, and we only
        // need to check the calc between these two joins.
        // Else, we need to check the calc between this join and the table scan.
        Optional<StreamPhysicalDeltaJoin> leftDeltaJoin = getDeltaJoin(join.getLeft());
        if (leftDeltaJoin.isEmpty()) {
            isLeftSupported = isCalcOnTableScanSupported(join.getLeft());
        } else {
            isLeftSupported = isCalcOnJoinSupported(join.getLeft());
        }

        Optional<StreamPhysicalDeltaJoin> rightDeltaJoin = getDeltaJoin(join.getRight());
        if (rightDeltaJoin.isEmpty()) {
            isRightSupported = isCalcOnTableScanSupported(join.getRight());
        } else {
            isRightSupported = isCalcOnJoinSupported(join.getRight());
        }

        return isLeftSupported && isRightSupported;
    }

    private static boolean isCalcOnTableScanSupported(RelNode joinInput) {
        return isCalcSupported(() -> collectCalcBetweenJoinAndTableScan(joinInput));
    }

    private static boolean isCalcOnJoinSupported(RelNode joinInput) {
        return isCalcSupported(() -> collectCalcBetweenJoinAndDeltaJoin(joinInput));
    }

    private static boolean isCalcSupported(Supplier<List<Calc>> calcListGetter) {
        List<Calc> calcList = calcListGetter.get();

        // currently, at most one calc is allowed to appear between Join and TableScan
        if (calcList.size() > 1) {
            return false;
        }

        if (calcList.isEmpty()) {
            return true;
        }

        Calc calc = calcList.get(0);
        return isCalcSupported(calc);
    }

    private static Optional<Calc> validateAndGetCalc(Supplier<List<Calc>> calcListSupplier) {
        List<Calc> calcList = calcListSupplier.get();
        Preconditions.checkState(calcList.size() <= 1, "More than one calc is not supported");
        if (calcList.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(calcList.get(0));
        }
    }

    public static Optional<RexProgram> getRexProgramBetweenJoinAndTableScan(RelNode joinInput) {
        return validateAndGetCalc(() -> collectCalcBetweenJoinAndTableScan(joinInput))
                .map(Calc::getProgram);
    }

    public static Optional<RexProgram> getRexProgramBetweenJoinAndDeltaJoin(RelNode joinInput) {
        return validateAndGetCalc(() -> collectCalcBetweenJoinAndDeltaJoin(joinInput))
                .map(Calc::getProgram);
    }

    private static List<Calc> collectCalcBetweenJoinAndTableScan(RelNode joinInput) {
        return collectCalcToSpecificUpstreamNode(joinInput, StreamPhysicalTableSourceScan.class);
    }

    private static List<Calc> collectCalcBetweenJoinAndDeltaJoin(RelNode joinInput) {
        return collectCalcToSpecificUpstreamNode(joinInput, StreamPhysicalDeltaJoin.class);
    }

    private static List<Calc> collectCalcToSpecificUpstreamNode(
            RelNode node, Class<? extends RelNode> upstreamEndNode) {
        CalcCollector calcCollector = new CalcCollector(upstreamEndNode);
        calcCollector.go(node);
        return calcCollector.getCollectResult();
    }

    private static boolean isCalcSupported(Calc calc) {
        RexProgram calcProgram = calc.getProgram();
        // calc with non-deterministic fields or filters is not supported
        return calcProgram == null || areAllRexNodeDeterministic(calcProgram.getExprList());
    }

    private static boolean areAllRexNodeDeterministic(List<RexNode> rexNodes) {
        // The presence of non-deterministic functions in projection or filter before join will
        // output non-deterministic fields or rows to delta join. Therefore, we strictly prohibit
        // the use of non-deterministic functions before delta join to ensure consistent and
        // reliable processing.
        return rexNodes.stream().allMatch(RexUtil::isDeterministic);
    }

    private static boolean areAllJoinInputsInWhiteList(RelNode node, Deque<RelNode> collector) {
        node = unwrapNode(node, true);

        // if meeting the source, check all collected nodes whether they are valid by
        // ALL_SUPPORTED_NODES_BEFORE_DELTA_JOIN
        if (node instanceof StreamPhysicalTableSourceScan) {
            collector.addLast(node);
            return collector.stream().allMatch(DeltaJoinUtil::isValidBeforeAllDeltaJoins);
        }
        // if meeting the delta join, check all collected nodes whether they are valid by
        // ALL_ALLOWED_NODES_BETWEEN_CASCADED_DELTA_JOIN
        if (node instanceof StreamPhysicalDeltaJoin) {
            return collector.stream().allMatch(DeltaJoinUtil::isValidBetweenCascadeDeltaJoins);
        }

        if (node.getInputs().isEmpty()) {
            return false;
        }

        collector.addLast(node);
        for (RelNode input : node.getInputs()) {
            if (!areAllJoinInputsInWhiteList(input, collector)) {
                return false;
            }
        }

        collector.removeLast();
        return true;
    }

    private static boolean isValidBeforeAllDeltaJoins(RelNode node) {
        Class<?> nodeClazz = node.getClass();
        return ALL_SUPPORTED_NODES_BEFORE_DELTA_JOIN.contains(nodeClazz);
    }

    private static boolean isValidBetweenCascadeDeltaJoins(RelNode node) {
        Class<?> nodeClazz = node.getClass();
        return ALL_ALLOWED_NODES_BETWEEN_CASCADED_DELTA_JOIN.contains(nodeClazz);
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

    private static IntPair[] getJoinKeyPassThroughCalc(
            IntPair[] joinKey, @Nullable RexProgram calc) {
        return TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                joinKey, JavaScalaConversionUtil.toScala(Optional.ofNullable(calc)));
    }

    private static Set<Integer> getLookupKeyPassThroughSourceSpec(
            Set<Integer> lookupKeySet, TableSourceTable tableSourceTable) {
        Map<Integer, Integer> mapOut2InPos = buildSourceOut2InPosMap(tableSourceTable);
        if (mapOut2InPos == null) {
            return lookupKeySet;
        }
        return lookupKeySet.stream()
                .flatMap(out -> Stream.ofNullable(mapOut2InPos.get(out)))
                .collect(Collectors.toSet());
    }

    @Nullable
    private static Map<Integer, Integer> buildSourceOut2InPosMap(
            TableSourceTable tableSourceTable) {
        Optional<ProjectPushDownSpec> projectPushDownSpec =
                Arrays.stream(tableSourceTable.abilitySpecs())
                        .filter(spec -> spec instanceof ProjectPushDownSpec)
                        .map(spec -> (ProjectPushDownSpec) spec)
                        .findFirst();
        if (projectPushDownSpec.isEmpty()) {
            return null;
        }

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
        return mapOut2InPos;
    }

    private static class CalcCollector extends RelVisitor {

        private final Class<? extends RelNode> upstreamEndNode;
        private final List<Calc> collectResult;

        private boolean foundUpstreamEndNode = false;

        public CalcCollector(Class<? extends RelNode> upstreamEndNode) {
            this.upstreamEndNode = upstreamEndNode;
            this.collectResult = new ArrayList<>();
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            Preconditions.checkArgument(
                    ordinal == 0,
                    "Only RelNode with single input is supported. Unexpected node is %s",
                    node.getClass());

            node = unwrapNode(node, true);

            if (upstreamEndNode.isInstance(node)) {
                foundUpstreamEndNode = true;
                return;
            }

            super.visit(node, ordinal, parent);

            if (node instanceof Calc) {
                collectResult.add((Calc) node);
            }
        }

        public List<Calc> getCollectResult() {
            if (!foundUpstreamEndNode) {
                throw new TableException(
                        String.format(
                                "Could not find the expected upstream end node [%s]. "
                                        + "This is a bug and should not happen. "
                                        + "Please file an issue",
                                upstreamEndNode.getName()));
            }
            return collectResult;
        }
    }

    private static class LookupBinaryInputInfo {

        private final int binaryInputOrdinal;
        private final DeltaJoinAssociation.BinaryInputInfo binaryInputInfo;
        private final Map<Integer, LookupJoinUtil.FunctionParam> lookupKeysOnThisBinaryInput;

        public LookupBinaryInputInfo(
                int binaryInputOrdinal,
                DeltaJoinAssociation.BinaryInputInfo binaryInputInfo,
                Map<Integer, LookupJoinUtil.FunctionParam> lookupKeysOnThisBinaryInput) {
            this.binaryInputOrdinal = binaryInputOrdinal;
            this.binaryInputInfo = binaryInputInfo;
            this.lookupKeysOnThisBinaryInput = lookupKeysOnThisBinaryInput;
        }

        public static LookupBinaryInputInfo of(
                int binaryInputOrdinal,
                DeltaJoinAssociation.BinaryInputInfo binaryInputInfo,
                Map<Integer, LookupJoinUtil.FunctionParam> lookupKeysOnThisBinaryInput) {
            return new LookupBinaryInputInfo(
                    binaryInputOrdinal, binaryInputInfo, lookupKeysOnThisBinaryInput);
        }
    }

    private abstract static class DeltaJoinTreeVisitor {

        public final void visit(DeltaJoinTree tree) {
            visit(tree.root);
        }

        protected void visit(DeltaJoinTree.BinaryInputNode node) {}

        protected void visit(DeltaJoinTree.JoinNode node) {
            visit(node.left);
            visit(node.right);
        }

        protected void visit(DeltaJoinTree.Node node) {
            if (node instanceof DeltaJoinTree.BinaryInputNode) {
                visit((DeltaJoinTree.BinaryInputNode) node);
            } else {
                visit((DeltaJoinTree.JoinNode) node);
            }
        }
    }

    private static class SplitJoinKeyVisitor extends DeltaJoinTreeVisitor {
        public final LinkedHashMap<Integer, IntPair[]> result = new LinkedHashMap<>();

        private final Deque<IntPair[]> stack = new ArrayDeque<>();
        private final FlinkTypeFactory typeFactory;

        public SplitJoinKeyVisitor(FlinkTypeFactory typeFactory, IntPair[] joinKey) {
            this.typeFactory = typeFactory;
            this.stack.addLast(joinKey);
        }

        @Override
        protected void visit(DeltaJoinTree.BinaryInputNode node) {
            int binaryInputOrd = node.inputOrdinal;

            IntPair[] curJoinKey = getJoinKeyPassThroughCalc(stack.getLast(), node.rexProgram);
            result.put(binaryInputOrd, curJoinKey);
        }

        @Override
        protected void visit(DeltaJoinTree.JoinNode node) {
            IntPair[] curJoinKey = getJoinKeyPassThroughCalc(stack.getLast(), node.rexProgram);
            int leftFieldCnt = node.left.getRowTypeAfterCalc(typeFactory).getFieldCount();
            List<IntPair> leftSideJoinKey = new ArrayList<>();
            List<IntPair> rightSideJoinKey = new ArrayList<>();
            for (IntPair jk : curJoinKey) {
                if (jk.target < leftFieldCnt) {
                    leftSideJoinKey.add(jk);
                } else {
                    rightSideJoinKey.add(IntPair.of(jk.source, jk.target - leftFieldCnt));
                }
            }
            stack.addLast(leftSideJoinKey.toArray(new IntPair[0]));
            visit(node.left);
            stack.removeLast();

            stack.addLast(rightSideJoinKey.toArray(new IntPair[0]));
            visit(node.right);
            stack.removeLast();
        }
    }
}
