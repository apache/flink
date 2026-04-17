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

import org.apache.flink.table.api.TableException;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.immutables.value.Value;

/**
 * Rules that reject temporal table joins ({@code FOR SYSTEM_TIME AS OF}) in batch mode with a clear
 * error message.
 *
 * <p>In the batch {@code EXPAND_PLAN_RULES}, lookup join rules run first and rewrite valid lookup
 * joins (processing-time + {@code LookupTableSource}) into {@code TemporalJoin} nodes. Any
 * remaining {@link LogicalCorrelate} + {@link LogicalSnapshot} pattern therefore represents an
 * unsupported temporal join. These rules catch it and throw a {@link TableException} rather than
 * letting the correlate survive into {@code FlinkDecorrelateProgram}, where it would cause a
 * confusing "unexpected correlate variable" internal error.
 */
@Value.Enclosing
public class RejectTemporalJoinInBatchRule
        extends RelRule<RejectTemporalJoinInBatchRule.RejectTemporalJoinInBatchRuleConfig> {

    private static final String MESSAGE =
            "Temporal joins (FOR SYSTEM_TIME AS OF) on regular tables are not supported in "
                    + "batch mode. Use a lookup join or switch to streaming mode.";

    /**
     * Matches temporal joins where the right side of the Correlate is a Filter wrapping a Snapshot
     * (non-trivial join condition).
     */
    public static final RejectTemporalJoinInBatchRule WITH_FILTER =
            RejectTemporalJoinInBatchRuleConfig.WITH_FILTER.toRule();

    /**
     * Matches temporal joins where the right side of the Correlate is a Snapshot directly (trivial
     * join condition).
     */
    public static final RejectTemporalJoinInBatchRule WITHOUT_FILTER =
            RejectTemporalJoinInBatchRuleConfig.WITHOUT_FILTER.toRule();

    private RejectTemporalJoinInBatchRule(RejectTemporalJoinInBatchRuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        throw new TableException(MESSAGE);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface RejectTemporalJoinInBatchRuleConfig extends RelRule.Config {

        RejectTemporalJoinInBatchRuleConfig WITH_FILTER =
                ImmutableRejectTemporalJoinInBatchRule.RejectTemporalJoinInBatchRuleConfig.builder()
                        .operandSupplier(
                                b0 ->
                                        b0.operand(LogicalCorrelate.class)
                                                .inputs(
                                                        b1 -> b1.operand(RelNode.class).anyInputs(),
                                                        b2 ->
                                                                b2.operand(LogicalFilter.class)
                                                                        .oneInput(
                                                                                b3 ->
                                                                                        b3.operand(
                                                                                                        LogicalSnapshot
                                                                                                                .class)
                                                                                                .anyInputs())))
                        .description("RejectTemporalJoinInBatchRule_WithFilter")
                        .build();

        RejectTemporalJoinInBatchRuleConfig WITHOUT_FILTER =
                ImmutableRejectTemporalJoinInBatchRule.RejectTemporalJoinInBatchRuleConfig.builder()
                        .operandSupplier(
                                b0 ->
                                        b0.operand(LogicalCorrelate.class)
                                                .inputs(
                                                        b1 -> b1.operand(RelNode.class).anyInputs(),
                                                        b2 ->
                                                                b2.operand(LogicalSnapshot.class)
                                                                        .anyInputs()))
                        .description("RejectTemporalJoinInBatchRule_WithoutFilter")
                        .build();

        @Override
        default RejectTemporalJoinInBatchRule toRule() {
            return new RejectTemporalJoinInBatchRule(this);
        }
    }
}
