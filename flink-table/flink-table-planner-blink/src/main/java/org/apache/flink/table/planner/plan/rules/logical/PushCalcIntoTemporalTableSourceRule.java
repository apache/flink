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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that pushes a {@link FlinkLogicalCalc} into a {@link FlinkLogicalTableSourceScan}
 * which wraps a {@link SupportsProjectionPushDown} temporal tableSource.
 */
public class PushCalcIntoTemporalTableSourceRule extends PushProjectIntoTableSourceRuleBase {
    public static final PushCalcIntoTemporalTableSourceRule INSTANCE =
            new PushCalcIntoTemporalTableSourceRule();

    public PushCalcIntoTemporalTableSourceRule() {
        super(
                operand(
                        FlinkLogicalSnapshot.class,
                        operand(
                                FlinkLogicalCalc.class,
                                operand(FlinkLogicalTableSourceScan.class, none()))),
                "PushCalcIntoTemporalTableSourceRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableScan scan = call.rel(2);
        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null
                || !(tableSourceTable.tableSource() instanceof SupportsProjectionPushDown)) {
            return false;
        }
        // Can not push calc into tableSource once watermarkAssigner has be pushed.
        // This is a case of temporal join with eventTime.
        // Because the input reference index of watermarkAssigner can not be adjusted.
        // So we can support pushing calc into tableSource across watermarkAssigner for temporal
        // tableSource.
        return Arrays.stream(tableSourceTable.extraDigests())
                .noneMatch(
                        digest -> digest.contains("project=[") || digest.contains("watermark=["));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final FlinkLogicalSnapshot snapshot = call.rel(0);
        final FlinkLogicalCalc calc = call.rel(1);

        final FlinkLogicalTableSourceScan scan = call.rel(2);
        RexProgram oldRexProgram = calc.getProgram();
        List<RexNode> newProjectExps = new ArrayList<>();
        List<RexNode> oldProjectExps =
                oldRexProgram.getProjectList().stream()
                        .map(ref -> oldRexProgram.expandLocalRef(ref))
                        .collect(Collectors.toList());
        RexLocalRef oldCondition = oldRexProgram.getCondition();

        if (oldCondition != null) {
            oldProjectExps.add(oldRexProgram.expandLocalRef(oldCondition));
        }
        TableSourceTable newTableSourceTable =
                applyPushIntoTableSource(scan, oldProjectExps, newProjectExps, call);
        // no need to transform
        if (newTableSourceTable == null) {
            return;
        }
        // new scan
        FlinkLogicalTableSourceScan newScan =
                new FlinkLogicalTableSourceScan(
                        scan.getCluster(), scan.getTraitSet(), newTableSourceTable);

        List<RexNode> projectExprs =
                oldCondition != null
                        ? newProjectExps.subList(0, newProjectExps.size() - 1)
                        : newProjectExps;
        RexNode newCondition =
                oldCondition != null ? newProjectExps.get(newProjectExps.size() - 1) : null;
        // new Calc
        RexProgram newRexProgram =
                RexProgram.create(
                        newScan.getRowType(),
                        projectExprs,
                        newCondition,
                        calc.getRowType(),
                        calc.getCluster().getRexBuilder());
        Calc newCalc =
                new FlinkLogicalCalc(calc.getCluster(), calc.getTraitSet(), newScan, newRexProgram);
        // new snapshot
        Snapshot newSnapshot;
        if (newRexProgram.isTrivial()) {
            newSnapshot = snapshot.copy(snapshot.getTraitSet(), newScan, snapshot.getPeriod());
        } else {
            newSnapshot = snapshot.copy(snapshot.getTraitSet(), newCalc, snapshot.getPeriod());
        }
        call.transformTo(newSnapshot);
    }
}
