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

import org.apache.flink.table.planner.functions.sql.ml.SqlVectorSearchTableFunction;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;

/** Rule to convert VECTOR_SEARCH call with literal value to a correlated VECTOR_SEARCH call. */
public class ConstantVectorSearchCallToCorrelateRule
        extends RelRule<
                ConstantVectorSearchCallToCorrelateRule
                        .ConstantVectorSearchCallToCorrelateRuleConfig> {

    public static final ConstantVectorSearchCallToCorrelateRule INSTANCE =
            ConstantVectorSearchCallToCorrelateRuleConfig.DEFAULT.toRule();

    private ConstantVectorSearchCallToCorrelateRule(
            ConstantVectorSearchCallToCorrelateRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalTableFunctionScan scan = call.rel(0);
        RexNode rexNode = scan.getCall();
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        RexCall rexCall = (RexCall) rexNode;
        return rexCall.getOperator() instanceof SqlVectorSearchTableFunction
                && RexUtil.isConstant(rexCall.getOperands().get(2));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableFunctionScan scan = call.rel(0);
        RexCall functionCall = (RexCall) scan.getCall();
        RexNode constantCall = functionCall.getOperands().get(2);
        RelOptCluster cluster = scan.getCluster();
        RelBuilder builder = call.builder();

        // left side
        LogicalValues values = LogicalValues.createOneRow(cluster);
        builder.push(values);
        builder.project(constantCall);

        // right side
        CorrelationId correlId = cluster.createCorrel();
        RexNode correlRex =
                cluster.getRexBuilder().makeCorrel(builder.peek().getRowType(), correlId);
        RexNode correlatedConstant = cluster.getRexBuilder().makeFieldAccess(correlRex, 0);
        builder.push(scan.getInput(0));
        ArrayList<RexNode> operands = new ArrayList<>(functionCall.operands);
        operands.set(2, correlatedConstant);
        builder.functionScan(functionCall.getOperator(), 1, operands);

        // add correlate node
        builder.join(
                JoinRelType.INNER,
                cluster.getRexBuilder().makeLiteral(true),
                Collections.singleton(correlId));

        // prune useless value input
        builder.projectExcept(builder.field(0));
        call.transformTo(builder.build());
    }

    @Value.Immutable
    public interface ConstantVectorSearchCallToCorrelateRuleConfig extends RelRule.Config {

        ConstantVectorSearchCallToCorrelateRuleConfig DEFAULT =
                ImmutableConstantVectorSearchCallToCorrelateRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 -> b0.operand(LogicalTableFunctionScan.class).anyInputs())
                        .withDescription("ConstantVectorSearchCallToCorrelateRule");

        @Override
        default ConstantVectorSearchCallToCorrelateRule toRule() {
            return new ConstantVectorSearchCallToCorrelateRule(this);
        }
    }
}
