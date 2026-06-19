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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.immutables.value.Value;

/** Transpose {@link LogicalProject} past into {@link LogicalSnapshot}. */
@Value.Enclosing
public class ProjectSnapshotTransposeRule extends RelRule<ProjectSnapshotTransposeRule.Config> {

    public static final RelOptRule INSTANCE = ProjectSnapshotTransposeRule.Config.DEFAULT.toRule();

    public ProjectSnapshotTransposeRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        // Don't push a project which contains over into a snapshot, snapshot on window aggregate is
        // unsupported for now.
        return !project.containsOver();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalSnapshot snapshot = call.rel(1);
        RelNode newProject = project.copy(project.getTraitSet(), snapshot.getInputs());
        RelNode newSnapshot =
                snapshot.copy(snapshot.getTraitSet(), newProject, snapshot.getPeriod());
        call.transformTo(newSnapshot);
    }

    /** Configuration for {@link ProjectSnapshotTransposeRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableProjectSnapshotTransposeRule.Config.builder()
                        .build()
                        .withOperator()
                        .as(Config.class);

        @Override
        default RelOptRule toRule() {
            return new ProjectSnapshotTransposeRule(this);
        }

        default Config withOperator() {
            final RelRule.OperandTransform snapshotTransform =
                    operandBuilder -> operandBuilder.operand(LogicalSnapshot.class).noInputs();

            final RelRule.OperandTransform projectTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(LogicalProject.class)
                                    .oneInput(snapshotTransform);

            return withOperandSupplier(projectTransform).as(Config.class);
        }
    }
}
