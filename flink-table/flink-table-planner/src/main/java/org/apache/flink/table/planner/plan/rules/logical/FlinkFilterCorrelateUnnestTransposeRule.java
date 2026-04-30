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
import org.apache.calcite.rel.rules.FilterCorrelateRule;

/**
 * Pushes a {@link org.apache.calcite.rel.core.Filter} down through a {@link Correlate} produced by
 * Flink's UNNEST rewrite. Left-only predicates land on the left input; right-only predicates land
 * on the right input for INNER correlates; mixed predicates stay above. The {@code LEFT} correlate
 * case correctly leaves right-only predicates above the join, preserving the null-padded row for
 * empty arrays.
 *
 * <p>Delegates the transformation to Calcite's {@link FilterCorrelateRule}; only the match
 * predicate is restricted to the UNNEST shape (right input is a {@code LogicalTableFunctionScan}
 * wrapping {@code INTERNAL_UNNEST_ROWS} or {@code INTERNAL_UNNEST_ROWS_WITH_ORDINALITY}).
 */
public class FlinkFilterCorrelateUnnestTransposeRule extends FilterCorrelateRule {

    public static final FlinkFilterCorrelateUnnestTransposeRule INSTANCE =
            new FlinkFilterCorrelateUnnestTransposeRule(
                    FilterCorrelateRule.Config.DEFAULT
                            .withDescription("FlinkFilterCorrelateUnnestTransposeRule")
                            .as(FilterCorrelateRule.Config.class));

    private FlinkFilterCorrelateUnnestTransposeRule(FilterCorrelateRule.Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Correlate correlate = call.rel(1);
        return UnnestRuleUtil.isUnnestCorrelate(correlate);
    }
}
