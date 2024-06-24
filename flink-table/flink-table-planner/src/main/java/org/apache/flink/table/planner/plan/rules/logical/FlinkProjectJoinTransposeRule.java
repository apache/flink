/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project} past a {@link
 * org.apache.calcite.rel.core.Join} by splitting the projection into a projection on top of each
 * child of the join.
 */
public class FlinkProjectJoinTransposeRule extends ProjectJoinTransposeRule {
    public static final ProjectJoinTransposeRule INSTANCE =
            new FlinkProjectJoinTransposeRule(
                    Config.DEFAULT
                            .withPreserveExprCondition(PushProjector.ExprCondition.FALSE)
                            .withDescription("FlinkProjectJoinTransposeRule")
                            .as(Config.class));

    private FlinkProjectJoinTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(1);
        if (!join.getJoinType().projectsRight()) {
            return; // TODO: support SEMI/ANTI join later
        }
        super.onMatch(call);
    }
}
