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

import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;

/** Restricts sort pushdown through Python projections with explicit concurrency. */
public final class FlinkSortProjectTransposeRule extends SortProjectTransposeRule {

    public static final FlinkSortProjectTransposeRule INSTANCE =
            new FlinkSortProjectTransposeRule();

    private FlinkSortProjectTransposeRule() {
        super(Config.DEFAULT.withDescription("FlinkSortProjectTransposeRule").as(Config.class));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!super.matches(call)) {
            return false;
        }
        final Sort sort = call.rel(0);
        final Project project = call.rel(1);
        // Explicit concurrency can introduce a repartition after a pushed-down sort.
        return sort.getCollation().getFieldCollations().isEmpty()
                || project.getProjects().stream()
                        .noneMatch(node -> PythonUtil.firstExplicitParallelism(node).isPresent());
    }
}
