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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;

/**
 * Extends calcite's ProjectSetOpTransposeRule, modification: only pushes a projection into the
 * inputs of EXCEPT ALL and INTERSECT ALL if it is a permutation of all input fields.
 *
 * <p>EXCEPT ALL and INTERSECT ALL match rows on all of their fields. Pushing a projection that
 * drops or rewrites a field into both inputs can make rows match that differed before, e.g. a
 * constant derived from the left input would replace the values of the right input. Stop overriding
 * once CALCITE-7909 is fixed.
 */
public class FlinkProjectSetOpTransposeRule extends ProjectSetOpTransposeRule {

    public static final RelOptRule INSTANCE = new FlinkProjectSetOpTransposeRule(Config.DEFAULT);

    protected FlinkProjectSetOpTransposeRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final SetOp setOp = call.rel(1);
        return setOp instanceof Union || project.getPermutation() != null;
    }
}
