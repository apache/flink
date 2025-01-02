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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkLogicalRelFactories;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.sql.SqlFirstLastValueAggFunction;
import org.apache.flink.table.planner.plan.PartialFinalType;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate;
import org.apache.flink.table.planner.plan.trait.RelWindowProperties;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ExpandUtil;
import org.apache.flink.table.planner.plan.utils.WindowUtil;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.commons.lang3.ArrayUtils;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.collection.JavaConverters;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AVG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAX;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SINGLE_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM0;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * Planner rule that splits aggregations containing distinct aggregates, e.g, count distinct, into
 * partial aggregations and final aggregations.
 *
 * <p>This rule rewrites an aggregate query with distinct aggregations into an expanded double
 * aggregations. The first aggregation compute the results in sub-partition and the results are
 * combined by the second aggregation.
 *
 * <pre>
 * Examples:
 *
 * MyTable: a: BIGINT, b: INT, c: VARCHAR
 *
 * Original records:
 * | a | b | c  |
 * |:-:|:-:|:--:|
 * | 1 | 1 | c1 |
 * | 1 | 2 | c1 |
 * | 2 | 1 | c2 |
 *
 * SQL: SELECT SUM(DISTINCT b), COUNT(DISTINCT c), AVG(b) FROM MyTable GROUP BY a
 *
 * flink logical plan:
 * {@code
 * FlinkLogicalCalc(select=[$f1 AS EXPR$0, $f2 AS EXPR$1, CAST(IF(=($f4, 0:BIGINT), null:INTEGER,
 *  /($f3, $f4))) AS EXPR$2])
 * +- FlinkLogicalAggregate(group=[{0}], agg#0=[SUM($3)], agg#1=[$SUM0($4)], agg#2=[$SUM0($5)],
 * agg#3=[$SUM0($6)])
 *    +- FlinkLogicalAggregate(group=[{0, 3, 4}], agg#0=[SUM(DISTINCT $1) FILTER $5],
 *    agg#1=[COUNT(DISTINCT $2) FILTER $6], agg#2=[$SUM0($1) FILTER $7],
 *    agg#3=[COUNT($1) FILTER $7])
 *       +- FlinkLogicalCalc(select=[a, b, c, $f3, $f4, =($e, 1) AS $g_1, =($e, 2) AS $g_2,
 *       =($e, 3) AS $g_3])
 *          +- FlinkLogicalExpand(projects=[a, b, c, $f3, $f4, $e])
 *             +- FlinkLogicalCalc(select=[a, b, c, MOD(HASH_CODE(b), 1024) AS $f3,
 *               MOD(HASH_CODE(c), 1024) AS $f4])
 *                +- FlinkLogicalTableSourceScan(table=[[MyTable]], fields=[a, b, c])
 * }
 *
 * '$e = 1' is equivalent to 'group by a, hash(b) % 1024' '$e = 2' is equivalent to 'group by a,
 * hash(c) % 1024' '$e = 3' is equivalent to 'group by a
 *
 * Expanded records: \+-----+-----+-----+------------------+------------------+-----+ \| a | b | c |
 * hash(b) % 1024 | hash(c) % 1024 | $e |
 * \+-----+-----+-----+------------------+------------------+-----+ ---+--- \| 1 | 1 | c1 | hash(b)
 * % 1024 | null | 1 | | \+-----+-----+-----+------------------+------------------+-----+ | \| 1 | 1
 * \| c1 | null | hash(c) % 1024 | 2 | records expanded by record1
 * \+-----+-----+-----+------------------+-----------------+------+ | \| 1 | 1 | c1 | null | null |
 * 3 | | \+-----+-----+-----+------------------+-----------------+------+ ---+--- \| 1 | 2 | c1 |
 * hash(b) % 1024 | null | 1 | | \+-----+-----+-----+------------------+-----------------+------+ |
 * \| 1 | 2 | c1 | null | hash(c) % 1024 | 2 | records expanded by record2
 * \+-----+-----+-----+------------------+-----------------+------+ | \| 1 | 2 | c1 | null | null |
 * 3 | | \+-----+-----+-----+------------------+-----------------+------+ ---+--- \| 2 | 1 | c2 |
 * hash(b) % 1024 | null | 1 | | \+-----+-----+-----+------------------+-----------------+------+ |
 * \| 2 | 1 | c2 | null | hash(c) % 1024 | 2 | records expanded by record3
 * \+-----+-----+-----+------------------+-----------------+------+ | \| 2 | 1 | c2 | null | null |
 * 3 | | \+-----+-----+-----+------------------+-----------------+------+ ---+---
 * </pre>
 *
 * <p>NOTES: this rule is only used for Stream now.
 */
@Value.Enclosing
public class SplitAggregateRule extends RelRule<SplitAggregateRule.SplitAggregateRuleConfig> {
    public static final SplitAggregateRule INSTANCE =
            SplitAggregateRule.SplitAggregateRuleConfig.DEFAULT.toRule();

    protected SplitAggregateRule(SplitAggregateRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig tableConfig = unwrapTableConfig(call);
        FlinkLogicalAggregate agg = call.rel(0);

        boolean splitDistinctAggEnabled =
                tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED);
        boolean isAllAggSplittable = AggregateUtil.doAllAggSupportSplit(agg.getAggCallList());

        // disable distinct split for processing-time window,
        // because the semantic is not clear to materialize processing-time window in two aggregates
        FlinkRelMetadataQuery fmq = (FlinkRelMetadataQuery) call.getMetadataQuery();
        RelWindowProperties windowProps = fmq.getRelWindowProperties(agg.getInput());
        boolean isWindowAgg =
                WindowUtil.groupingContainsWindowStartEnd(agg.getGroupSet(), windowProps);
        boolean isProctimeWindowAgg = isWindowAgg && !windowProps.isRowtime();

        // disable distinct split for session window,
        // otherwise window assigner results may be different
        boolean isSessionWindowAgg =
                isWindowAgg && windowProps.getWindowSpec() instanceof SessionWindowSpec;
        // TableAggregate is not supported. see also FLINK-21923.
        boolean isTableAgg = AggregateUtil.isTableAggregate(agg.getAggCallList());

        return agg.partialFinalType() == PartialFinalType.NONE
                && agg.containsDistinctCall()
                && splitDistinctAggEnabled
                && isAllAggSplittable
                && !isProctimeWindowAgg
                && !isTableAgg
                && !isSessionWindowAgg;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableConfig tableConfig = unwrapTableConfig(call);
        FlinkLogicalAggregate originalAggregate = call.rel(0);
        List<AggregateCall> aggCalls = originalAggregate.getAggCallList();
        FlinkRelNode input = call.rel(1);
        RelOptCluster cluster = originalAggregate.getCluster();
        FlinkRelBuilder relBuilder = (FlinkRelBuilder) call.builder();
        relBuilder.push(input);
        int[] aggGroupSet = originalAggregate.getGroupSet().toArray();

        // STEP 1: add hash fields if necessary
        int[] hashFieldIndexes =
                IntStream.range(0, aggCalls.size())
                        .filter(i -> SplitAggregateRule.needAddHashFields(aggCalls.get(i)))
                        .flatMap(
                                i ->
                                        Arrays.stream(
                                                SplitAggregateRule.getArgIndexes(aggCalls.get(i))))
                        .distinct()
                        .filter(index -> !ArrayUtils.contains(aggGroupSet, index))
                        .sorted()
                        .toArray();

        Map<Integer, Integer> hashFieldsMap = new HashMap<>();
        int buckets =
                tableConfig.get(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_BUCKET_NUM);

        if (hashFieldIndexes.length > 0) {
            List<RexNode> projects = new ArrayList<>(relBuilder.fields());
            int hashFieldsOffset = projects.size();

            IntStream.range(0, hashFieldIndexes.length)
                    .forEach(
                            index -> {
                                int hashFieldIdx = hashFieldIndexes[index];
                                RexNode hashField = relBuilder.field(hashFieldIdx);
                                // hash(f) % buckets
                                RexNode node =
                                        relBuilder.call(
                                                SqlStdOperatorTable.MOD,
                                                relBuilder.call(
                                                        FlinkSqlOperatorTable.HASH_CODE, hashField),
                                                relBuilder.literal(buckets));

                                projects.add(node);
                                hashFieldsMap.put(hashFieldIdx, hashFieldsOffset + index);
                            });
            relBuilder.project(projects);
        }

        // STEP 2: construct partial aggregates
        Set<ImmutableBitSet> groupSetTreeSet = new TreeSet<>(ImmutableBitSet.ORDERING);
        Map<AggregateCall, ImmutableBitSet> aggInfoToGroupSetMap = new HashMap<>();
        int newGroupSetsNum = 0;
        for (AggregateCall aggCall : aggCalls) {
            ImmutableBitSet groupSet;
            if (SplitAggregateRule.needAddHashFields(aggCall)) {
                List<Integer> newIndexes =
                        Arrays.stream(SplitAggregateRule.getArgIndexes(aggCall))
                                .mapToObj(
                                        argIndex -> hashFieldsMap.getOrDefault(argIndex, argIndex))
                                .collect(Collectors.toList());
                groupSet = ImmutableBitSet.of(newIndexes).union(ImmutableBitSet.of(aggGroupSet));
                // Only increment groupSet number if aggregate call needs add new different hash
                // fields
                // e.g SQL1: SELECT COUNT(DISTINCT a), MAX(a) FROM T group by b
                // newGroupSetsNum is 1 because two agg function add same hash field
                // e.g SQL2: SELECT COUNT(DISTINCT a), COUNT(b) FROM T group by c
                // newGroupSetsNum is 1 because only COUNT(DISTINCT a) adds a new hash field
                // e.g SQL3: SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM T group by b
                // newGroupSetsNum is 2 because COUNT(DISTINCT a), COUNT(DISTINCT b) both add hash
                // field
                if (!groupSetTreeSet.contains(groupSet)) {
                    newGroupSetsNum += 1;
                }
            } else {
                groupSet = ImmutableBitSet.of(aggGroupSet);
            }
            groupSetTreeSet.add(groupSet);
            aggInfoToGroupSetMap.put(aggCall, groupSet);
        }
        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.copyOf(groupSetTreeSet);
        ImmutableBitSet fullGroupSet = ImmutableBitSet.union(groupSets);

        // STEP 2.1: expand input fields
        List<AggregateCall> partialAggCalls = new ArrayList<>();
        Map<AggregateCall, ImmutableBitSet> partialAggCallToGroupSetMap = new HashMap<>();
        aggCalls.stream()
                .forEach(
                        aggCall -> {
                            List<AggregateCall> newAggCalls =
                                    SplitAggregateRule.getPartialAggFunction(aggCall).stream()
                                            .map(
                                                    aggFunc ->
                                                            AggregateCall.create(
                                                                    aggFunc,
                                                                    aggCall.isDistinct(),
                                                                    aggCall.isApproximate(),
                                                                    false,
                                                                    aggCall.getArgList(),
                                                                    aggCall.filterArg,
                                                                    null,
                                                                    RelCollations.EMPTY,
                                                                    fullGroupSet.cardinality(),
                                                                    relBuilder.peek(),
                                                                    null,
                                                                    null))
                                            .collect(Collectors.toList());
                            partialAggCalls.addAll(newAggCalls);
                            newAggCalls.stream()
                                    .forEach(
                                            newAggCall -> {
                                                partialAggCallToGroupSetMap.put(
                                                        newAggCall,
                                                        aggInfoToGroupSetMap.get(aggCall));
                                            });
                        });

        boolean needExpand = newGroupSetsNum > 1;
        Map<Integer, Integer> duplicateFieldMap =
                needExpand
                        ? JavaConverters.mapAsJavaMap(
                                ExpandUtil.buildExpandNode(
                                                relBuilder,
                                                JavaConverters.asScalaBufferConverter(
                                                                partialAggCalls)
                                                        .asScala(),
                                                fullGroupSet,
                                                groupSets)
                                        ._1)
                        : Collections.emptyMap();

        // STEP 2.2: add filter columns for partial aggregates
        Map<Tuple2<ImmutableBitSet, Integer>, Integer> filters = new LinkedHashMap<>();
        List<AggregateCall> newPartialAggCalls = new ArrayList<>();
        if (needExpand) {
            // GROUPING returns an integer (0, 1, 2...).
            // Add a project to convert those values to BOOLEAN.
            List<RexNode> nodes = new ArrayList<>(relBuilder.fields());
            RexNode expandIdNode = nodes.remove(nodes.size() - 1);
            int filterColumnsOffset = nodes.size();
            int x = 0;
            for (AggregateCall aggCall : partialAggCalls) {
                ImmutableBitSet groupSet = partialAggCallToGroupSetMap.get(aggCall);
                int oldFilterArg = aggCall.filterArg;
                List<Integer> newArgList =
                        aggCall.getArgList().stream()
                                .map(a -> duplicateFieldMap.getOrDefault(a, a))
                                .collect(Collectors.toList());

                if (!filters.containsKey(Tuple2.of(groupSet, oldFilterArg))) {
                    long expandId = ExpandUtil.genExpandId(fullGroupSet, groupSet);
                    if (oldFilterArg >= 0) {
                        nodes.add(
                                relBuilder.alias(
                                        relBuilder.and(
                                                relBuilder.equals(
                                                        expandIdNode, relBuilder.literal(expandId)),
                                                relBuilder.field(oldFilterArg)),
                                        "$g_" + expandId));
                    } else {
                        nodes.add(
                                relBuilder.alias(
                                        relBuilder.equals(
                                                expandIdNode, relBuilder.literal(expandId)),
                                        "$g_" + expandId));
                    }

                    int newFilterArg = filterColumnsOffset + x;
                    filters.put(Tuple2.of(groupSet, oldFilterArg), newFilterArg);
                    x++;
                }

                int newFilterArg = filters.get(Tuple2.of(groupSet, oldFilterArg));
                AggregateCall newAggCall =
                        aggCall.adaptTo(
                                relBuilder.peek(),
                                newArgList,
                                newFilterArg,
                                fullGroupSet.cardinality(),
                                fullGroupSet.cardinality());
                newPartialAggCalls.add(newAggCall);
            }
            relBuilder.project(nodes);
        } else {
            newPartialAggCalls.addAll(partialAggCalls);
        }

        // STEP 2.3: construct partial aggregates
        // Create aggregate node directly to avoid ClassCastException,
        // Please see FLINK-21923 for more details.
        // TODO reuse aggregate function, see FLINK-22412
        FlinkLogicalAggregate partialAggregate =
                FlinkLogicalAggregate.create(
                        relBuilder.build(),
                        fullGroupSet,
                        ImmutableList.of(fullGroupSet),
                        newPartialAggCalls,
                        originalAggregate.getHints());
        partialAggregate.setPartialFinalType(PartialFinalType.PARTIAL);
        relBuilder.push(partialAggregate);

        // STEP 3: construct final aggregates
        int finalAggInputOffset = fullGroupSet.cardinality();
        int x = 0;
        List<AggregateCall> finalAggCalls = new ArrayList<>();
        boolean needMergeFinalAggOutput = false;
        for (AggregateCall aggCall : aggCalls) {
            for (SqlAggFunction aggFunction : SplitAggregateRule.getFinalAggFunction(aggCall)) {
                ImmutableIntList newArgList = ImmutableIntList.of(finalAggInputOffset + x);
                x++;

                AggregateCall newAggCall =
                        AggregateCall.create(
                                aggFunction,
                                false,
                                aggCall.isApproximate(),
                                false,
                                newArgList,
                                -1,
                                null,
                                RelCollations.EMPTY,
                                originalAggregate.getGroupCount(),
                                relBuilder.peek(),
                                null,
                                null);
                finalAggCalls.add(newAggCall);
            }

            if (SplitAggregateRule.getFinalAggFunction(aggCall).size() > 1) {
                needMergeFinalAggOutput = true;
            }
        }
        // Create aggregate node directly to avoid ClassCastException,
        // Please see FLINK-21923 for more details.
        // TODO reuse aggregate function, see FLINK-22412
        FlinkLogicalAggregate finalAggregate =
                FlinkLogicalAggregate.create(
                        relBuilder.build(),
                        SplitAggregateRule.remap(fullGroupSet, originalAggregate.getGroupSet()),
                        SplitAggregateRule.remap(
                                fullGroupSet, Arrays.asList(originalAggregate.getGroupSet())),
                        finalAggCalls,
                        originalAggregate.getHints());
        finalAggregate.setPartialFinalType(PartialFinalType.FINAL);
        relBuilder.push(finalAggregate);

        // STEP 4: convert final aggregation output to the original aggregation output.
        // For example, aggregate function AVG is transformed to SUM0 and COUNT, so the output of
        // the final aggregation is (sum, count). We should converted it to (sum / count)
        // for the final output.
        int aggGroupCount = finalAggregate.getGroupCount();
        if (needMergeFinalAggOutput) {
            List<RexNode> nodes =
                    IntStream.range(0, aggGroupCount)
                            .mapToObj(index -> RexInputRef.of(index, finalAggregate.getRowType()))
                            .collect(Collectors.toCollection(ArrayList::new));

            int avgAggCount = 0;
            for (int i = 0; i < aggCalls.size(); i++) {
                AggregateCall aggCall = aggCalls.get(i);
                RexNode newNode;
                if (aggCall.getAggregation().getKind() == SqlKind.AVG) {
                    RexNode sumInputRef =
                            RexInputRef.of(
                                    aggGroupCount + i + avgAggCount, finalAggregate.getRowType());
                    RexNode countInputRef =
                            RexInputRef.of(
                                    aggGroupCount + i + avgAggCount + 1,
                                    finalAggregate.getRowType());
                    avgAggCount++;
                    // Make a guarantee that the final aggregation returns NULL if underlying count
                    // is ZERO.
                    // We use SUM0 for underlying sum, which may run into ZERO / ZERO,
                    // and division by zero exception occurs.
                    // @see Glossary#SQL2011 SQL:2011 Part 2 Section 6.27
                    RexNode equals =
                            relBuilder.call(
                                    FlinkSqlOperatorTable.EQUALS,
                                    countInputRef,
                                    relBuilder
                                            .getRexBuilder()
                                            .makeBigintLiteral(BigDecimal.valueOf(0)));
                    RexNode ifTrue = relBuilder.getRexBuilder().makeNullLiteral(aggCall.getType());
                    RexNode ifFalse =
                            relBuilder.call(
                                    FlinkSqlOperatorTable.DIVIDE, sumInputRef, countInputRef);
                    newNode = relBuilder.call(FlinkSqlOperatorTable.IF, equals, ifTrue, ifFalse);
                } else {
                    newNode =
                            RexInputRef.of(
                                    aggGroupCount + i + avgAggCount, finalAggregate.getRowType());
                }
                nodes.add(newNode);
            }
            relBuilder.project(nodes);
        }

        relBuilder.convert(originalAggregate.getRowType(), false);

        RelNode newRel = relBuilder.build();
        call.transformTo(newRel);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface SplitAggregateRuleConfig extends RelRule.Config {
        SplitAggregateRule.SplitAggregateRuleConfig DEFAULT =
                ImmutableSplitAggregateRule.SplitAggregateRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalAggregate.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(FlinkRelNode.class)
                                                                        .anyInputs()))
                        .withRelBuilderFactory(FlinkLogicalRelFactories.FLINK_LOGICAL_REL_BUILDER())
                        .withDescription("SplitAggregateRule");

        @Override
        default SplitAggregateRule toRule() {
            return new SplitAggregateRule(this);
        }
    }

    // mapping aggFun to (partial aggFun, final aggFun)
    private static final Map<SqlAggFunction, Tuple2<List<SqlAggFunction>, List<SqlAggFunction>>>
            PARTIAL_FINAL_MAP = new HashMap<>();

    static {
        PARTIAL_FINAL_MAP.put(
                AVG, Tuple2.of(Arrays.asList(SUM0, COUNT), Arrays.asList(SUM0, SUM0)));
        PARTIAL_FINAL_MAP.put(COUNT, Tuple2.of(Arrays.asList(COUNT), Arrays.asList(SUM0)));
        PARTIAL_FINAL_MAP.put(MIN, Tuple2.of(Arrays.asList(MIN), Arrays.asList(MIN)));
        PARTIAL_FINAL_MAP.put(MAX, Tuple2.of(Arrays.asList(MAX), Arrays.asList(MAX)));
        PARTIAL_FINAL_MAP.put(SUM, Tuple2.of(Arrays.asList(SUM), Arrays.asList(SUM)));
        PARTIAL_FINAL_MAP.put(SUM0, Tuple2.of(Arrays.asList(SUM0), Arrays.asList(SUM0)));
        PARTIAL_FINAL_MAP.put(
                FlinkSqlOperatorTable.FIRST_VALUE,
                Tuple2.of(
                        Arrays.asList(FlinkSqlOperatorTable.FIRST_VALUE),
                        Arrays.asList(FlinkSqlOperatorTable.FIRST_VALUE)));
        PARTIAL_FINAL_MAP.put(
                FlinkSqlOperatorTable.LAST_VALUE,
                Tuple2.of(
                        Arrays.asList(FlinkSqlOperatorTable.LAST_VALUE),
                        Arrays.asList(FlinkSqlOperatorTable.LAST_VALUE)));
        PARTIAL_FINAL_MAP.put(
                FlinkSqlOperatorTable.LISTAGG,
                Tuple2.of(
                        Arrays.asList(FlinkSqlOperatorTable.LISTAGG),
                        Arrays.asList(FlinkSqlOperatorTable.LISTAGG)));
        PARTIAL_FINAL_MAP.put(
                SINGLE_VALUE, Tuple2.of(Arrays.asList(SINGLE_VALUE), Arrays.asList(SINGLE_VALUE)));
    }

    private static boolean needAddHashFields(AggregateCall aggCall) {
        // When min/max/first_value/last_value is in retraction mode, records will aggregate into
        // one single operator instance regardless of localAgg optimization, which leads to hotspot.
        // So we split them into partial/final aggs either.
        boolean needSplit =
                aggCall.getAggregation() instanceof SqlMinMaxAggFunction
                        || aggCall.getAggregation() instanceof SqlFirstLastValueAggFunction;
        return needSplit || aggCall.isDistinct();
    }

    private static int[] getArgIndexes(AggregateCall aggCall) {
        return aggCall.getArgList().stream().mapToInt(Integer::intValue).toArray();
    }

    private static List<SqlAggFunction> getPartialAggFunction(AggregateCall aggCall) {
        Tuple2<List<SqlAggFunction>, List<SqlAggFunction>> partialFinalPair =
                PARTIAL_FINAL_MAP.get(aggCall.getAggregation());
        if (partialFinalPair != null) {
            return partialFinalPair.f0;
        } else {
            throw new TableException(
                    "Aggregation " + aggCall.getAggregation() + " is not supported to split!");
        }
    }

    private static List<SqlAggFunction> getFinalAggFunction(AggregateCall aggCall) {
        Tuple2<List<SqlAggFunction>, List<SqlAggFunction>> partialFinalPair =
                PARTIAL_FINAL_MAP.get(aggCall.getAggregation());
        if (partialFinalPair != null) {
            return partialFinalPair.f1;
        } else {
            throw new TableException(
                    "Aggregation " + aggCall.getAggregation() + " is not supported to split!");
        }
    }

    /**
     * Compute the group sets of the final aggregation.
     *
     * @param groupSet the group set of the previous partial aggregation
     * @param originalGroupSets the group set of the original aggregation
     */
    private static ImmutableList<ImmutableBitSet> remap(
            ImmutableBitSet groupSet, Iterable<ImmutableBitSet> originalGroupSets) {
        ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
        for (ImmutableBitSet originalGroupSet : originalGroupSets) {
            builder.add(remap(groupSet, originalGroupSet));
        }
        return builder.build();
    }

    /**
     * Compute the group set of the final aggregation.
     *
     * @param groupSet the group set of the previous partial aggregation
     * @param originalGroupSet the group set of the original aggregation
     */
    private static ImmutableBitSet remap(
            ImmutableBitSet groupSet, ImmutableBitSet originalGroupSet) {
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int bit : originalGroupSet) {
            builder.set(remap(groupSet, bit));
        }
        return builder.build();
    }

    private static int remap(ImmutableBitSet groupSet, int arg) {
        if (arg < 0) {
            return -1;
        } else {
            return groupSet.indexOf(arg);
        }
    }
}
