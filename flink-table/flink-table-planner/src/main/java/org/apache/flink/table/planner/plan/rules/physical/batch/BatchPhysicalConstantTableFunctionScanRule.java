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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalValues;

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
 * Converts {@link FlinkLogicalTableFunctionScan} with constant RexCall to
 *
 * <pre>
 *                            {@link BatchPhysicalCorrelate}
 *                                   /               \
 * empty {@link BatchPhysicalValuesRule}}     {@link FlinkLogicalTableFunctionScan}.
 * </pre>
 *
 * <p>Add the rule to support select from a UDF directly, such as the following SQL: {@code SELECT *
 * FROM LATERAL TABLE(func()) as T(c)}
 *
 * <p>Note: {@link BatchPhysicalCorrelateRule} is responsible for converting a reasonable physical
 * plan for the normal correlate query, such as the following SQL: example1: {@code SELECT * FROM T,
 * LATERAL TABLE(func()) as T(c) example2: SELECT a, c FROM T, LATERAL TABLE(func(a)) as T(c)}
 */
@Value.Enclosing
public class BatchPhysicalConstantTableFunctionScanRule
        extends RelRule<
                BatchPhysicalConstantTableFunctionScanRule
                        .BatchPhysicalConstantTableFunctionScanRuleConfig> {

    public static final BatchPhysicalConstantTableFunctionScanRule INSTANCE =
            BatchPhysicalConstantTableFunctionScanRuleConfig.DEFAULT.toRule();

    protected BatchPhysicalConstantTableFunctionScanRule(
            BatchPhysicalConstantTableFunctionScanRuleConfig config) {
        super(config);
    }

    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalTableFunctionScan scan = call.rel(0);
        return RexUtil.isConstant(scan.getCall()) && scan.getInputs().isEmpty();
    }

    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalTableFunctionScan scan = call.rel(0);

        // create correlate left
        RelOptCluster cluster = scan.getCluster();
        RelTraitSet traitSet =
                call.getPlanner().emptyTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
        BatchPhysicalValues values =
                new BatchPhysicalValues(
                        cluster,
                        traitSet,
                        ImmutableList.of(ImmutableList.of()),
                        cluster.getTypeFactory()
                                .createStructType(ImmutableList.of(), ImmutableList.of()));

        BatchPhysicalCorrelate correlate =
                new BatchPhysicalCorrelate(
                        cluster,
                        traitSet,
                        values,
                        scan,
                        Option.empty(),
                        scan.getRowType(),
                        JoinRelType.INNER);
        call.transformTo(correlate);
    }

    /** Configuration for {@link BatchPhysicalConstantTableFunctionScanRule}. */
    @Value.Immutable(singleton = false)
    public interface BatchPhysicalConstantTableFunctionScanRuleConfig extends RelRule.Config {
        BatchPhysicalConstantTableFunctionScanRule.BatchPhysicalConstantTableFunctionScanRuleConfig
                DEFAULT =
                        ImmutableBatchPhysicalConstantTableFunctionScanRule
                                .BatchPhysicalConstantTableFunctionScanRuleConfig.builder()
                                .build()
                                .withOperandSupplier(
                                        b0 ->
                                                b0.operand(FlinkLogicalTableFunctionScan.class)
                                                        .anyInputs())
                                .withDescription("BatchPhysicalConstantTableFunctionScanRule")
                                .as(
                                        BatchPhysicalConstantTableFunctionScanRule
                                                .BatchPhysicalConstantTableFunctionScanRuleConfig
                                                .class);

        @Override
        default BatchPhysicalConstantTableFunctionScanRule toRule() {
            return new BatchPhysicalConstantTableFunctionScanRule(this);
        }
    }
}
