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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.rules.ProjectCorrelateTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;

/**
 * Pushes a {@link org.apache.calcite.rel.core.Project} down through a {@link Correlate} produced by
 * Flink's UNNEST rewrite, so unused left-side columns are pruned before the cross-product
 * expansion.
 *
 * <p>Delegates the transformation to Calcite's {@link ProjectCorrelateTransposeRule}; only the
 * match predicate is restricted to the UNNEST shape (right input is a {@code
 * LogicalTableFunctionScan} wrapping {@code INTERNAL_UNNEST_ROWS} or {@code
 * INTERNAL_UNNEST_ROWS_WITH_ORDINALITY}).
 */
public class FlinkProjectCorrelateUnnestTransposeRule extends ProjectCorrelateTransposeRule {

    public static final FlinkProjectCorrelateUnnestTransposeRule INSTANCE =
            new FlinkProjectCorrelateUnnestTransposeRule(
                    ProjectCorrelateTransposeRule.Config.DEFAULT
                            .withPreserveExprCondition(PushProjector.ExprCondition.FALSE)
                            .withDescription("FlinkProjectCorrelateUnnestTransposeRule")
                            .as(ProjectCorrelateTransposeRule.Config.class));

    private FlinkProjectCorrelateUnnestTransposeRule(ProjectCorrelateTransposeRule.Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Correlate correlate = call.rel(1);
        return UnnestRuleUtil.isUnnestCorrelate(correlate);
    }
}
