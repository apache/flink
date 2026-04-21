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

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalProcessTableFunction;
import org.apache.flink.table.planner.plan.rules.physical.common.PhysicalMLPredictTableFunctionRule;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule to convert a {@link FlinkLogicalTableFunctionScan} with table arguments into a {@link
 * StreamPhysicalProcessTableFunction}.
 */
public class StreamPhysicalProcessTableFunctionRule extends ConverterRule {

    public static final StreamPhysicalProcessTableFunctionRule INSTANCE =
            new StreamPhysicalProcessTableFunctionRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalTableFunctionScan.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalProcessTableFunctionRule"));

    private StreamPhysicalProcessTableFunctionRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalTableFunctionScan scan = call.rel(0);
        if (scan.getInputs().isEmpty()) {
            // Let StreamPhysicalConstantTableFunctionScanRule take over
            return false;
        }
        final RexCall rexCall = (RexCall) scan.getCall();
        final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(rexCall);
        return definition != null
                && !PhysicalMLPredictTableFunctionRule.isMLPredictFunction(definition)
                && definition.getKind() == FunctionKind.PROCESS_TABLE;
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) rel;
        final RexCall rexCall = (RexCall) scan.getCall();

        final List<RexNode> operands = rexCall.getOperands();
        final List<RelNode> newInputs =
                applyDistributionOnInputs(rexCall, operands, rel.getInputs());
        final RelTraitSet providedTraitSet =
                rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        return new StreamPhysicalProcessTableFunction(
                scan.getCluster(), providedTraitSet, newInputs, scan, scan.getRowType());
    }

    private static List<RelNode> applyDistributionOnInputs(
            RexCall rexCall, List<RexNode> operands, List<RelNode> inputs) {
        final List<StaticArgument> staticArgs =
                StreamPhysicalProcessTableFunction.getStaticArguments(rexCall);
        return Ord.zip(operands).stream()
                .filter(operand -> operand.e instanceof RexTableArgCall)
                .map(
                        tableOperand -> {
                            final RexTableArgCall tableArgCall = (RexTableArgCall) tableOperand.e;
                            final StaticArgument tableArg = staticArgs.get(tableOperand.i);
                            final StaticArgument resolvedTableArg =
                                    tableArg.applyConditionalTraits(
                                            StreamPhysicalProcessTableFunction.buildTraitContext(
                                                    rexCall, tableArgCall));
                            return applyDistributionOnInput(
                                    tableArgCall,
                                    resolvedTableArg,
                                    inputs.get(tableArgCall.getInputIndex()));
                        })
                .collect(Collectors.toList());
    }

    private static RelNode applyDistributionOnInput(
            RexTableArgCall tableOperand, StaticArgument resolvedTableArg, RelNode input) {
        final FlinkRelDistribution distribution =
                deriveDistribution(tableOperand, resolvedTableArg);
        final RelTraitSet requiredTraitSet =
                input.getCluster()
                        .getPlanner()
                        .emptyTraitSet()
                        .replace(distribution)
                        .replace(FlinkConventions.STREAM_PHYSICAL());
        return RelOptRule.convert(input, requiredTraitSet);
    }

    private static FlinkRelDistribution deriveDistribution(
            RexTableArgCall tableOperand, StaticArgument resolvedTableArg) {
        if (resolvedTableArg.is(StaticArgumentTrait.SET_SEMANTIC_TABLE)) {
            final int[] partitionKeys = tableOperand.getPartitionKeys();
            if (partitionKeys.length == 0) {
                return FlinkRelDistribution.SINGLETON();
            }
            return FlinkRelDistribution.hash(partitionKeys, true);
        }
        return FlinkRelDistribution.DEFAULT();
    }
}
