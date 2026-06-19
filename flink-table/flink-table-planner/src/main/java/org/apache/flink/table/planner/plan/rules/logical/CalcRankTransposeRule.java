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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRank;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.RankUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Planner rule that transposes {@link FlinkLogicalCalc} past {@link FlinkLogicalRank} to reduce
 * rank input fields.
 */
@Value.Enclosing
public class CalcRankTransposeRule
        extends RelRule<CalcRankTransposeRule.CalcRankTransposeRuleConfig> {

    public static final CalcRankTransposeRule INSTANCE =
            CalcRankTransposeRule.CalcRankTransposeRuleConfig.DEFAULT.toRule();

    private CalcRankTransposeRule(CalcRankTransposeRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        FlinkLogicalRank rank = call.rel(1);

        int totalColumnCount = rank.getInput().getRowType().getFieldCount();
        // apply the rule only when calc could prune some columns
        int[] pushableColumns = getPushableColumns(calc, rank);
        return pushableColumns.length < totalColumnCount;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        FlinkLogicalRank rank = call.rel(1);

        int[] pushableColumns = getPushableColumns(calc, rank);

        RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
        // create a new Calc to project columns of Rank's input
        RexProgram innerProgram =
                createNewInnerCalcProgram(
                        pushableColumns, rank.getInput().getRowType(), rexBuilder);
        FlinkLogicalCalc newInnerCalc =
                (FlinkLogicalCalc) calc.copy(calc.getTraitSet(), rank.getInput(), innerProgram);

        // create a new Rank on top of new Calc
        Map<Integer, Integer> fieldMapping =
                IntStream.range(0, pushableColumns.length)
                        .boxed()
                        .collect(Collectors.toMap(i -> pushableColumns[i], Function.identity()));
        FlinkLogicalRank newRank = createNewRankOnCalc(fieldMapping, newInnerCalc, rank);

        // create a new Calc on top of newRank if needed
        if (rank.outputRankNumber()) {
            // append RankNumber field mapping
            int oldRankFunFieldIdx =
                    RankUtil.getRankNumberColumnIndex(rank)
                            .getOrElse(
                                    () -> {
                                        throw new TableException("This should not happen");
                                    });
            int newRankFunFieldIdx =
                    RankUtil.getRankNumberColumnIndex(newRank)
                            .getOrElse(
                                    () -> {
                                        throw new TableException("This should not happen");
                                    });
            fieldMapping.put(oldRankFunFieldIdx, newRankFunFieldIdx);
        }
        RexProgram topProgram =
                createNewTopCalcProgram(
                        calc.getProgram(), fieldMapping, newRank.getRowType(), rexBuilder);

        RelNode equiv;
        if (topProgram.isTrivial()) {
            // Ignore newTopCac if it's program is trivial
            equiv = newRank;
        } else {
            equiv = calc.copy(calc.getTraitSet(), newRank, topProgram);
        }
        call.transformTo(equiv);
    }

    private int[] getPushableColumns(Calc calc, FlinkLogicalRank rank) {
        int[] usedFields = getUsedFields(calc.getProgram());
        int rankFunFieldIndex =
                (int)
                        JavaScalaConversionUtil.toJava(RankUtil.getRankNumberColumnIndex(rank))
                                .orElse(-1);
        int[] usedFieldsExcludeRankNumber =
                Arrays.stream(usedFields).filter(index -> index != rankFunFieldIndex).toArray();

        int[] requiredFields = getKeyFields(rank);
        return Stream.of(usedFieldsExcludeRankNumber, requiredFields)
                .flatMapToInt(Arrays::stream)
                .distinct()
                .sorted()
                .toArray();
    }

    private int[] getUsedFields(RexProgram program) {
        List<RexNode> projects =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());
        RexNode condition =
                program.getCondition() != null
                        ? program.expandLocalRef(program.getCondition())
                        : null;
        return RelOptUtil.InputFinder.bits(projects, condition).toArray();
    }

    private int[] getKeyFields(FlinkLogicalRank rank) {
        int[] partitionKey = rank.partitionKey().toArray();
        int[] orderKey =
                rank.orderKey().getFieldCollations().stream()
                        .mapToInt(RelFieldCollation::getFieldIndex)
                        .toArray();
        Set<ImmutableBitSet> upsertKeys =
                FlinkRelMetadataQuery.reuseOrCreate(rank.getCluster().getMetadataQuery())
                        .getUpsertKeysInKeyGroupRange(rank.getInput(), partitionKey);
        int[] keysInUniqueKeys =
                upsertKeys == null || upsertKeys.isEmpty()
                        ? new int[0]
                        : upsertKeys.stream()
                                .flatMapToInt(key -> Arrays.stream(key.toArray()))
                                .toArray();
        int[] rankRangeKey =
                rank.rankRange() instanceof VariableRankRange
                        ? new int[] {((VariableRankRange) rank.rankRange()).getRankEndIndex()}
                        : new int[0];

        // All key including partition key, order key, unique keys, VariableRankRange rankEndIndex
        return Stream.of(partitionKey, orderKey, keysInUniqueKeys, rankRangeKey)
                .flatMapToInt(Arrays::stream)
                .toArray();
    }

    private RexProgram createNewInnerCalcProgram(
            int[] projectedFields, RelDataType inputRowType, RexBuilder rexBuilder) {
        List<RexNode> projects =
                Arrays.stream(projectedFields)
                        .mapToObj(i -> RexInputRef.of(i, inputRowType))
                        .collect(Collectors.toList());
        List<String> inputColNames = inputRowType.getFieldNames();
        List<String> colNames =
                Arrays.stream(projectedFields)
                        .mapToObj(inputColNames::get)
                        .collect(Collectors.toList());
        return RexProgram.create(inputRowType, projects, null, colNames, rexBuilder);
    }

    private RexProgram createNewTopCalcProgram(
            RexProgram oldTopProgram,
            Map<Integer, Integer> fieldMapping,
            RelDataType inputRowType,
            RexBuilder rexBuilder) {
        List<RexNode> newProjects =
                oldTopProgram.getProjectList().stream()
                        .map(oldTopProgram::expandLocalRef)
                        .map(p -> FlinkRexUtil.adjustInputRef(p, fieldMapping))
                        .collect(Collectors.toList());
        RexNode newCondition =
                oldTopProgram.getCondition() != null
                        ? FlinkRexUtil.adjustInputRef(
                                oldTopProgram.expandLocalRef(oldTopProgram.getCondition()),
                                fieldMapping)
                        : null;
        List<String> colNames = oldTopProgram.getOutputRowType().getFieldNames();
        return RexProgram.create(inputRowType, newProjects, newCondition, colNames, rexBuilder);
    }

    private FlinkLogicalRank createNewRankOnCalc(
            Map<Integer, Integer> fieldMapping, Calc input, FlinkLogicalRank rank) {
        int[] newPartitionKey =
                Arrays.stream(rank.partitionKey().toArray()).map(fieldMapping::get).toArray();
        RelCollation oldOrderKey = rank.orderKey();
        List<RelFieldCollation> oldFieldCollations = oldOrderKey.getFieldCollations();
        List<RelFieldCollation> newFieldCollations =
                oldFieldCollations.stream()
                        .map(fc -> fc.withFieldIndex(fieldMapping.get(fc.getFieldIndex())))
                        .collect(Collectors.toList());
        RelCollation newOrderKey =
                newFieldCollations.equals(oldFieldCollations)
                        ? oldOrderKey
                        : RelCollations.of(newFieldCollations);

        return new FlinkLogicalRank(
                rank.getCluster(),
                rank.getTraitSet(),
                input,
                ImmutableBitSet.of(newPartitionKey),
                newOrderKey,
                rank.rankType(),
                rank.rankRange(),
                rank.rankNumberType(),
                rank.outputRankNumber());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface CalcRankTransposeRuleConfig extends RelRule.Config {
        CalcRankTransposeRule.CalcRankTransposeRuleConfig DEFAULT =
                ImmutableCalcRankTransposeRule.CalcRankTransposeRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalCalc.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRank.class)
                                                                        .anyInputs()))
                        .withDescription("CalcRankTransposeRule");

        @Override
        default CalcRankTransposeRule toRule() {
            return new CalcRankTransposeRule(this);
        }
    }
}
