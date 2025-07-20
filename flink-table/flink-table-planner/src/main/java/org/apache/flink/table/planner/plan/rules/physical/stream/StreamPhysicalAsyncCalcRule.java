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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalAsyncCalc;
import org.apache.flink.table.planner.plan.utils.AsyncUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexProgram;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A physical rule for identifying logical async Calcs and converting it to a physical RelNode. */
public class StreamPhysicalAsyncCalcRule extends ConverterRule {
    public static final RelOptRule INSTANCE =
            new StreamPhysicalAsyncCalcRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalCalc.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalAsyncCalcRule"));

    protected StreamPhysicalAsyncCalcRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        RexProgram program = calc.getProgram();
        return program.getExprList().stream().anyMatch(AsyncUtil::containsAsyncCall);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        FlinkLogicalCalc calc = (FlinkLogicalCalc) rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        RelNode newInput = RelOptRule.convert(calc.getInput(), FlinkConventions.STREAM_PHYSICAL());

        return new StreamPhysicalAsyncCalc(
                rel.getCluster(), traitSet, newInput, calc.getProgram(), rel.getRowType());
    }
}
