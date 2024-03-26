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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalValues;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

import scala.Option;

/**
 * Converts {@link FlinkLogicalTableFunctionScan} with constant RexCall. To
 *
 * <pre>
 *                           {@link StreamPhysicalCorrelate}
 *                              /                     \
 *       empty {@link StreamPhysicalValues}  {@link FlinkLogicalTableFunctionScan}
 * </pre>
 *
 * <p>Add the rule to support select from a UDF directly, such as the following SQL: {@code SELECT *
 * FROM LATERAL TABLE(func()) as T(c)}
 *
 * <p>Note: @{link StreamPhysicalCorrelateRule} is responsible for converting a reasonable physical
 * plan for the normal correlate query, such as the following SQL: example1: {@code SELECT * FROM T,
 * LATERAL TABLE(func()) as T(c) example2: SELECT a, c FROM T, LATERAL TABLE(func(a)) as T(c)}
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
        FlinkLogicalTableFunctionScan scan = call.rel(0);
        return !RexUtil.containsInputRef(scan.getCall()) && scan.getInputs().isEmpty();
    }

    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalTableFunctionScan scan = call.rel(0);

        // create correlate left
        RelOptCluster cluster = scan.getCluster();
        RelTraitSet traitSet =
                call.getPlanner().emptyTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        StreamPhysicalValues values =
                new StreamPhysicalValues(
                        cluster,
                        traitSet,
                        ImmutableList.of(ImmutableList.of()),
                        cluster.getTypeFactory()
                                .createStructType(ImmutableList.of(), ImmutableList.of()));

        StreamPhysicalCorrelate correlate =
                new StreamPhysicalCorrelate(
                        cluster,
                        traitSet,
                        values,
                        scan,
                        Option.empty(),
                        scan.getRowType(),
                        JoinRelType.INNER);
        call.transformTo(correlate);
    }

    /** Configuration for {@link StreamPhysicalConstantTableFunctionScanRule}. */
    @Value.Immutable(singleton = false)
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
