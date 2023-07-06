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

import org.apache.flink.table.planner.plan.utils.FlinkRelUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rex.RexNode;

/**
 * Extends calcite's FilterCalcMergeRule, modification: only merge the two neighbouring {@link
 * Filter} and {@link Calc} if each non-deterministic {@link RexNode} of bottom {@link Calc} should
 * appear at most once in the implicit project list and condition of top {@link Filter}.
 */
public class FlinkFilterCalcMergeRule extends FilterCalcMergeRule {

    public static final RelOptRule INSTANCE = new FlinkFilterCalcMergeRule(Config.DEFAULT);

    protected FlinkFilterCalcMergeRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalFilter topFilter = call.rel(0);
        final LogicalCalc bottomCalc = call.rel(1);
        return FlinkRelUtil.isMergeable(topFilter, bottomCalc);
    }
}
