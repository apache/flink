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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalAsyncCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalProcessTableFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalValues;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

import java.util.Optional;

import scala.Option;

/**
 * Converts {@link FlinkLogicalTableFunctionScan} with constant parameters. Add the rule to support
 * selecting from a UDF directly, e.g. {@code SELECT * FROM func() as T(c)}.
 *
 * <p>For {@link org.apache.flink.table.functions.FunctionKind#TABLE}:
 *
 * <pre>
 *   empty {@link StreamPhysicalValues} -> {@link StreamPhysicalCorrelate}
 * </pre>
 *
 * <p>{@link StreamPhysicalCorrelateRule} powers queries such as {@code SELECT * FROM T, LATERAL
 * TABLE(func()) as T(c)} or {@code SELECT a, c FROM T, LATERAL TABLE(func(a)) as T(c)}.
 *
 * <p>For {@link org.apache.flink.table.functions.FunctionKind#PROCESS_TABLE}:
 *
 * <pre>
 *   empty {@link StreamPhysicalValues} -> {@link StreamPhysicalProcessTableFunction}
 * </pre>
 *
 * <p>{@link StreamPhysicalProcessTableFunction} powers queries such as {@code SELECT * FROM func(t
 * => TABLE T)} or {@code SELECT * FROM func(t => TABLE T PARTITION BY k)}.
 */
@Value.Enclosing
public class StreamPhysicalConstantTableFunctionScanRule
        extends RelRule<
                StreamPhysicalConstantTableFunctionScanRule
                        .StreamPhysicalConstantTableFunctionScanRuleConfig> {

    public static final StreamPhysicalConstantTableFunctionScanRule INSTANCE =
            StreamPhysicalConstantTableFunctionScanRule
                    .StreamPhysicalConstantTableFunctionScanRuleConfig.DEFAULT
                    .toRule();

    protected StreamPhysicalConstantTableFunctionScanRule(
            StreamPhysicalConstantTableFunctionScanRuleConfig config) {
        super(config);
    }

    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalTableFunctionScan scan = call.rel(0);
        return !RexUtil.containsInputRef(scan.getCall());
    }

    public void onMatch(RelOptRuleCall call) {
        final FlinkLogicalTableFunctionScan scan = call.rel(0);
        final RelOptCluster cluster = scan.getCluster();
        final RelTraitSet traitSet =
                call.getPlanner().emptyTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());

        final StreamPhysicalValues values =
                new StreamPhysicalValues(
                        cluster,
                        traitSet,
                        ImmutableList.of(ImmutableList.of()),
                        cluster.getTypeFactory()
                                .createStructType(ImmutableList.of(), ImmutableList.of()));

        final FunctionDefinition function = ShortcutUtils.unwrapFunctionDefinition(scan.getCall());
        assert function != null;
        final RelNode replacement;
        if (function.getKind() == FunctionKind.TABLE) {
            replacement =
                    new StreamPhysicalCorrelate(
                            cluster,
                            traitSet,
                            values,
                            scan,
                            Option.empty(),
                            scan.getRowType(),
                            JoinRelType.INNER);
        } else if (function.getKind() == FunctionKind.ASYNC_TABLE) {
            replacement =
                    new StreamPhysicalAsyncCorrelate(
                            cluster,
                            traitSet,
                            values,
                            scan,
                            Optional.empty(),
                            Optional.empty(),
                            scan.getRowType(),
                            JoinRelType.INNER);
        } else if (function.getKind() == FunctionKind.PROCESS_TABLE) {
            replacement =
                    new StreamPhysicalProcessTableFunction(
                            cluster, traitSet, values, scan, scan.getRowType());
        } else {
            throw new TableException("Unsupported function for scan:" + function.getKind());
        }

        call.transformTo(replacement);
    }

    /** Configuration for {@link StreamPhysicalConstantTableFunctionScanRule}. */
    @Value.Immutable
    public interface StreamPhysicalConstantTableFunctionScanRuleConfig extends RelRule.Config {
        StreamPhysicalConstantTableFunctionScanRule
                        .StreamPhysicalConstantTableFunctionScanRuleConfig
                DEFAULT =
                        ImmutableStreamPhysicalConstantTableFunctionScanRule
                                .StreamPhysicalConstantTableFunctionScanRuleConfig.builder()
                                .build()
                                .withOperandSupplier(
                                        b0 ->
                                                b0.operand(FlinkLogicalTableFunctionScan.class)
                                                        .anyInputs())
                                .withDescription("StreamPhysicalConstantTableFunctionScanRule")
                                .as(StreamPhysicalConstantTableFunctionScanRuleConfig.class);

        @Override
        default StreamPhysicalConstantTableFunctionScanRule toRule() {
            return new StreamPhysicalConstantTableFunctionScanRule(this);
        }
    }
}
