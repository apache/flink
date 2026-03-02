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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.WindowUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple4;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Planner rule that tries to pull up {@link StreamPhysicalWindowTableFunction} into a {@link
 * StreamPhysicalWindowAggregate}.
 */
@Value.Enclosing
public class PullUpWindowTableFunctionIntoWindowAggregateRule
        extends RelRule<
                PullUpWindowTableFunctionIntoWindowAggregateRule
                        .PullUpWindowTableFunctionIntoWindowAggregateRuleConfig> {

    public static final PullUpWindowTableFunctionIntoWindowAggregateRule INSTANCE =
            PullUpWindowTableFunctionIntoWindowAggregateRule
                    .PullUpWindowTableFunctionIntoWindowAggregateRuleConfig.DEFAULT
                    .toRule();

    protected PullUpWindowTableFunctionIntoWindowAggregateRule(
            PullUpWindowTableFunctionIntoWindowAggregateRule
                            .PullUpWindowTableFunctionIntoWindowAggregateRuleConfig
                    config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        StreamPhysicalWindowAggregate windowAgg = call.rel(0);
        StreamPhysicalCalc calc = call.rel(2);
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(windowAgg.getCluster().getMetadataQuery());

        // condition and projection of Calc shouldn't contain calls on window columns,
        // otherwise, we can't transpose WindowTVF and Calc
        if (WindowUtil.calcContainsCallsOnWindowColumns(calc, fmq)) {
            return false;
        }

        ImmutableBitSet aggInputWindowProps = fmq.getRelWindowProperties(calc).getWindowColumns();
        // aggregate call shouldn't be on window columns
        // TODO: this can be supported in the future by referencing them as a RexFieldVariable
        return JavaScalaConversionUtil.toJava(windowAgg.aggCalls()).stream()
                .allMatch(
                        aggCall ->
                                aggInputWindowProps
                                        .intersect(ImmutableBitSet.of(aggCall.getArgList()))
                                        .isEmpty());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalWindowAggregate windowAgg = call.rel(0);
        StreamPhysicalCalc calc = call.rel(2);
        StreamPhysicalWindowTableFunction windowTVF = call.rel(3);
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(windowAgg.getCluster().getMetadataQuery());
        RelOptCluster cluster = windowAgg.getCluster();
        RelNode input = unwrapRel(windowTVF.getInput());

        if (input instanceof StreamPhysicalExchange) {
            input = ((StreamPhysicalExchange) input).getInput();
        }

        RelDataType inputRowType = input.getRowType();

        RelTraitSet requiredInputTraitSet =
                input.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        RelNode newInput = RelOptRule.convert(input, requiredInputTraitSet);

        // -------------------------------------------------------------------------
        //  1. transpose Calc and WindowTVF, build the new Calc node
        // -------------------------------------------------------------------------
        ImmutableBitSet windowColumns = fmq.getRelWindowProperties(windowTVF).getWindowColumns();
        Tuple4<RexProgram, int[], Integer, Boolean> programInfo =
                WindowUtil.buildNewProgramWithoutWindowColumns(
                        cluster.getRexBuilder(),
                        calc.getProgram(),
                        inputRowType,
                        windowTVF.windowing().getTimeAttributeIndex(),
                        windowColumns.toArray());
        RexProgram newProgram = programInfo._1();
        int[] aggInputFieldsShift = programInfo._2();
        int timeAttributeIndex = programInfo._3();

        StreamPhysicalCalc newCalc =
                new StreamPhysicalCalc(
                        cluster,
                        calc.getTraitSet(),
                        newInput,
                        newProgram,
                        newProgram.getOutputRowType());

        // -------------------------------------------------------------------------
        //  2. Adjust grouping index and convert Calc with new distribution
        // -------------------------------------------------------------------------
        int[] newGrouping =
                Arrays.stream(windowAgg.grouping())
                        .map(grouping -> aggInputFieldsShift[grouping])
                        .toArray();
        FlinkRelDistribution requiredDistribution =
                (newGrouping.length != 0)
                        ? FlinkRelDistribution.hash(newGrouping, true)
                        : FlinkRelDistribution.SINGLETON();
        RelTraitSet requiredTraitSet =
                newCalc.getTraitSet()
                        .replace(FlinkConventions.STREAM_PHYSICAL())
                        .replace(requiredDistribution);
        RelNode convertedCalc = RelOptRule.convert(newCalc, requiredTraitSet);

        // -----------------------------------------------------------------------------
        //  3. Adjust aggregate arguments index and construct new window aggregate node
        // -----------------------------------------------------------------------------
        WindowSpec newWindowSpec = updateWindowSpec(windowTVF.windowing().getWindow(), newCalc);
        TimeAttributeWindowingStrategy newWindowing =
                new TimeAttributeWindowingStrategy(
                        newWindowSpec,
                        windowTVF.windowing().getTimeAttributeType(),
                        timeAttributeIndex);
        RelTraitSet providedTraitSet =
                windowAgg.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());

        List<AggregateCall> newAggCalls =
                JavaScalaConversionUtil.toJava(windowAgg.aggCalls()).stream()
                        .map(
                                aggCall -> {
                                    List<Integer> newArgList =
                                            aggCall.getArgList().stream()
                                                    .map(arg -> aggInputFieldsShift[arg])
                                                    .collect(Collectors.toList());
                                    int newFilterArg =
                                            aggCall.hasFilter()
                                                    ? aggInputFieldsShift[aggCall.filterArg]
                                                    : aggCall.filterArg;
                                    List<RelFieldCollation> newFieldCollations =
                                            aggCall.getCollation().getFieldCollations().stream()
                                                    .map(
                                                            field ->
                                                                    field.withFieldIndex(
                                                                            aggInputFieldsShift[
                                                                                    field
                                                                                            .getFieldIndex()]))
                                                    .collect(Collectors.toList());
                                    return aggCall.copy(
                                            newArgList,
                                            newFilterArg,
                                            RelCollations.of(newFieldCollations));
                                })
                        .collect(Collectors.toList());

        StreamPhysicalWindowAggregate newWindowAgg =
                new StreamPhysicalWindowAggregate(
                        cluster,
                        providedTraitSet,
                        convertedCalc,
                        newGrouping,
                        JavaScalaConversionUtil.toScala(newAggCalls),
                        newWindowing,
                        windowAgg.namedWindowProperties());

        call.transformTo(newWindowAgg);
    }

    private RelNode unwrapRel(RelNode rel) {
        RelNode current = rel;
        while (current instanceof RelSubset) {
            current = ((RelSubset) current).getOriginal();
        }
        return current;
    }

    private WindowSpec updateWindowSpec(WindowSpec oldWindowSpec, StreamPhysicalCalc calc) {
        if (oldWindowSpec instanceof SessionWindowSpec) {
            final SessionWindowSpec sessionWindowSpec = (SessionWindowSpec) oldWindowSpec;
            final int[] windowPartitionKeys = sessionWindowSpec.getPartitionKeyIndices();
            final int[] newPartitionKeysThroughCalc =
                    getSessionPartitionKeysThroughCalc(windowPartitionKeys, calc);
            checkArgument(windowPartitionKeys.length == newPartitionKeysThroughCalc.length);
            return new SessionWindowSpec(sessionWindowSpec.getGap(), newPartitionKeysThroughCalc);
        }
        return oldWindowSpec;
    }

    private int[] getSessionPartitionKeysThroughCalc(
            int[] sessionWindowPartitionKeyIndices, StreamPhysicalCalc calc) {
        final List<Integer> newPartitionKeyIndices = new ArrayList<>();
        final RexProgram program = calc.getProgram();
        for (int index = 0; index < program.getNamedProjects().size(); index++) {
            final Pair<RexLocalRef, String> project = program.getNamedProjects().get(index);
            final RexNode expr = program.expandLocalRef(project.left);
            if (expr instanceof RexInputRef) {
                final int inputIndex = ((RexInputRef) expr).getIndex();
                if (IntStream.of(sessionWindowPartitionKeyIndices).anyMatch(i -> i == inputIndex)) {
                    newPartitionKeyIndices.add(index);
                }
            }
        }
        return newPartitionKeyIndices.stream().mapToInt(Integer::intValue).toArray();
    }

    /** Configuration for {@link PullUpWindowTableFunctionIntoWindowAggregateRule}. */
    @Value.Immutable(singleton = false)
    public interface PullUpWindowTableFunctionIntoWindowAggregateRuleConfig extends RelRule.Config {
        PullUpWindowTableFunctionIntoWindowAggregateRule
                        .PullUpWindowTableFunctionIntoWindowAggregateRuleConfig
                DEFAULT =
                        ImmutablePullUpWindowTableFunctionIntoWindowAggregateRule
                                .PullUpWindowTableFunctionIntoWindowAggregateRuleConfig.builder()
                                .build()
                                .withOperandSupplier(
                                        b0 ->
                                                b0.operand(StreamPhysicalWindowAggregate.class)
                                                        .oneInput(
                                                                b1 ->
                                                                        b1.operand(
                                                                                        StreamPhysicalExchange
                                                                                                .class)
                                                                                .oneInput(
                                                                                        b2 ->
                                                                                                b2.operand(
                                                                                                                StreamPhysicalCalc
                                                                                                                        .class)
                                                                                                        .oneInput(
                                                                                                                b3 ->
                                                                                                                        b3.operand(
                                                                                                                                        StreamPhysicalWindowTableFunction
                                                                                                                                                .class)
                                                                                                                                .anyInputs()))))
                                .withDescription("PullUpWindowTableFunctionIntoWindowAggregateRule")
                                .as(
                                        PullUpWindowTableFunctionIntoWindowAggregateRule
                                                .PullUpWindowTableFunctionIntoWindowAggregateRuleConfig
                                                .class);

        @Override
        default PullUpWindowTableFunctionIntoWindowAggregateRule toRule() {
            return new PullUpWindowTableFunctionIntoWindowAggregateRule(this);
        }
    }
}
