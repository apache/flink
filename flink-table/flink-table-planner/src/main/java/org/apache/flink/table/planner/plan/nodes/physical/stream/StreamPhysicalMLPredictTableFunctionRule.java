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

import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.planner.calcite.RexModelCall;
import org.apache.flink.table.planner.functions.sql.ml.SqlMLPredictTableFunction;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexCall;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule to convert a {@link FlinkLogicalTableFunctionScan} with ml_predict call into a {@link
 * StreamPhysicalMLPredictTableFunction}.
 */
public class StreamPhysicalMLPredictTableFunctionRule extends ConverterRule {

    public static final StreamPhysicalMLPredictTableFunctionRule INSTANCE =
            new StreamPhysicalMLPredictTableFunctionRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalTableFunctionScan.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalModelTableFunctionRule"));

    private StreamPhysicalMLPredictTableFunctionRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalTableFunctionScan scan = call.rel(0);
        final RexCall rexCall = (RexCall) scan.getCall();
        if (!(rexCall.getOperator() instanceof SqlMLPredictTableFunction)) {
            return false;
        }

        final RexModelCall modelCall = (RexModelCall) rexCall.getOperands().get(1);
        return modelCall.getModelProvider() instanceof PredictRuntimeProvider
                || modelCall.getModelProvider() instanceof AsyncPredictRuntimeProvider;
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) rel;
        final RelNode newInput =
                RelOptRule.convert(scan.getInput(0), FlinkConventions.STREAM_PHYSICAL());

        final RelTraitSet providedTraitSet =
                rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        return new StreamPhysicalMLPredictTableFunction(
                scan.getCluster(), providedTraitSet, newInput, scan, scan.getRowType());
    }
}
