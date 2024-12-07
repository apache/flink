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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRank;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.List;

/**
 * Planner rule that removes the output column of rank number iff there is an equality condition for
 * the rank column.
 */
@Value.Enclosing
public class ConstantRankNumberColumnRemoveRule
        extends RelRule<
                ConstantRankNumberColumnRemoveRule.ConstantRankNumberColumnRemoveRuleConfig> {

    public static final ConstantRankNumberColumnRemoveRule INSTANCE =
            ConstantRankNumberColumnRemoveRule.ConstantRankNumberColumnRemoveRuleConfig.DEFAULT
                    .toRule();

    public ConstantRankNumberColumnRemoveRule(ConstantRankNumberColumnRemoveRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalRank rank = call.rel(0);
        boolean isRowNumber = rank.rankType() == RankType.ROW_NUMBER;
        boolean constantRowNumber = false;
        RankRange range = rank.rankRange();
        if (range instanceof ConstantRankRange) {
            constantRowNumber =
                    ((ConstantRankRange) range).getRankStart()
                            == ((ConstantRankRange) range).getRankEnd();
        }
        return isRowNumber && constantRowNumber && rank.outputRankNumber();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalRank rank = call.rel(0);
        long rowNumber = ((ConstantRankRange) rank.rankRange()).getRankStart();
        FlinkLogicalRank newRank =
                new FlinkLogicalRank(
                        rank.getCluster(),
                        rank.getTraitSet(),
                        rank.getInput(),
                        rank.partitionKey(),
                        rank.orderKey(),
                        rank.rankType(),
                        rank.rankRange(),
                        rank.rankNumberType(),
                        false);

        RexBuilder rexBuilder = rank.getCluster().getRexBuilder();
        RexProgramBuilder programBuilder = new RexProgramBuilder(newRank.getRowType(), rexBuilder);
        int fieldCount = rank.getRowType().getFieldCount();
        List<String> fieldNames = rank.getRowType().getFieldNames();
        for (int i = 0; i < fieldCount; i++) {
            if (i < fieldCount - 1) {
                programBuilder.addProject(i, i, fieldNames.get(i));
            } else {
                RexLiteral rowNumberLiteral =
                        rexBuilder.makeBigintLiteral(BigDecimal.valueOf(rowNumber));
                programBuilder.addProject(i, rowNumberLiteral, fieldNames.get(i));
            }
        }

        RexProgram rexProgram = programBuilder.getProgram();
        RelNode calc = FlinkLogicalCalc.create(newRank, rexProgram);
        call.transformTo(calc);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface ConstantRankNumberColumnRemoveRuleConfig extends RelRule.Config {
        ConstantRankNumberColumnRemoveRule.ConstantRankNumberColumnRemoveRuleConfig DEFAULT =
                ImmutableConstantRankNumberColumnRemoveRule.ConstantRankNumberColumnRemoveRuleConfig
                        .builder()
                        .operandSupplier(b0 -> b0.operand(FlinkLogicalRank.class).anyInputs())
                        .description("ConstantRankNumberColumnRemoveRule")
                        .build();

        @Override
        default ConstantRankNumberColumnRemoveRule toRule() {
            return new ConstantRankNumberColumnRemoveRule(this);
        }
    }
}
