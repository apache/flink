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

import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Planner rule that pushes a {@link LogicalProject} into a {@link LogicalTableScan} which wraps a
 * {@link SupportsProjectionPushDown} dynamic tableSource.
 */
public class PushProjectIntoTableSourceScanRule extends PushProjectIntoTableSourceRuleBase {
    public static final PushProjectIntoTableSourceScanRule INSTANCE =
            new PushProjectIntoTableSourceScanRule();

    public PushProjectIntoTableSourceScanRule() {
        super(
                operand(LogicalProject.class, operand(LogicalTableScan.class, none())),
                "PushProjectIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableScan scan = call.rel(1);
        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null
                || !(tableSourceTable.tableSource() instanceof SupportsProjectionPushDown)) {
            return false;
        }
        return Arrays.stream(tableSourceTable.extraDigests())
                .noneMatch(digest -> digest.startsWith("project=["));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final LogicalTableScan scan = call.rel(1);

        List<RexNode> newProjectExps = new ArrayList<>();
        TableSourceTable newTableSourceTable =
                applyPushIntoTableSource(scan, project.getProjects(), newProjectExps, call);
        // no need to transform
        if (newTableSourceTable == null) {
            return;
        }

        LogicalTableScan newScan =
                new LogicalTableScan(
                        scan.getCluster(),
                        scan.getTraitSet(),
                        scan.getHints(),
                        newTableSourceTable);
        // rewrite new source
        LogicalProject newProject =
                project.copy(project.getTraitSet(), newScan, newProjectExps, project.getRowType());

        if (ProjectRemoveRule.isTrivial(newProject)) {
            // drop project if the transformed program merely returns its input
            call.transformTo(newScan);
        } else {
            call.transformTo(newProject);
        }
    }
}
