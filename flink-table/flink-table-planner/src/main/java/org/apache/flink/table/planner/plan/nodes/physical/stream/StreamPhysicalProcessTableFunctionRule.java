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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.TableCharacteristic;
import org.apache.calcite.sql.TableCharacteristic.Semantics;
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
        return definition != null && definition.getKind() == FunctionKind.PROCESS_TABLE;
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) rel;
        final RexCall rexCall = (RexCall) scan.getCall();
        final BridgingSqlFunction.WithTableFunction function =
                (BridgingSqlFunction.WithTableFunction) rexCall.getOperator();
        final List<RexNode> operands = rexCall.getOperands();
        final List<RelNode> newInputs =
                applyDistributionOnInputs(function, operands, rel.getInputs());
        final RelTraitSet providedTraitSet =
                rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        return new StreamPhysicalProcessTableFunction(
                scan.getCluster(), providedTraitSet, newInputs, scan, scan.getRowType());
    }

    private static List<RelNode> applyDistributionOnInputs(
            BridgingSqlFunction.WithTableFunction function,
            List<RexNode> operands,
            List<RelNode> inputs) {
        return Ord.zip(operands).stream()
                .filter(operand -> operand.e instanceof RexTableArgCall)
                .map(
                        tableOperand -> {
                            final int pos = tableOperand.i;
                            final RexTableArgCall tableArgCall = (RexTableArgCall) tableOperand.e;
                            final TableCharacteristic tableCharacteristic =
                                    function.tableCharacteristic(pos);
                            assert tableCharacteristic != null;
                            return applyDistributionOnInput(
                                    tableArgCall,
                                    tableCharacteristic,
                                    inputs.get(tableArgCall.getInputIndex()));
                        })
                .collect(Collectors.toList());
    }

    private static RelNode applyDistributionOnInput(
            RexTableArgCall tableOperand, TableCharacteristic tableCharacteristic, RelNode input) {
        final FlinkRelDistribution requiredDistribution =
                deriveDistribution(tableOperand, tableCharacteristic);
        final RelTraitSet requiredTraitSet =
                input.getCluster()
                        .getPlanner()
                        .emptyTraitSet()
                        .replace(requiredDistribution)
                        .replace(FlinkConventions.STREAM_PHYSICAL());
        return RelOptRule.convert(input, requiredTraitSet);
    }

    private static FlinkRelDistribution deriveDistribution(
            RexTableArgCall tableOperand, TableCharacteristic tableCharacteristic) {
        if (tableCharacteristic.semantics == Semantics.SET) {
            final int[] partitionKeys = tableOperand.getPartitionKeys();
            if (partitionKeys.length == 0) {
                return FlinkRelDistribution.SINGLETON();
            }
            return FlinkRelDistribution.hash(partitionKeys, true);
        }
        return FlinkRelDistribution.DEFAULT();
    }
}
