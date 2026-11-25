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

import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkRelFactories;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.utils.NestedProjectionUtil;
import org.apache.flink.table.planner.plan.utils.NestedSchema;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Transpose between the {@link LogicalWatermarkAssigner} and {@link LogicalProject}. The transposed
 * {@link LogicalProject} works like a pruner to prune the unused fields from source. The top level
 * {@link LogicalProject} still has to do the calculation, filter and prune the rowtime column if
 * the query doesn't need. Nested fields are pruned as well, i.e., the transposed {@link
 * LogicalProject} only forwards the used nested fields.
 */
public class ProjectWatermarkAssignerTransposeRule extends RelOptRule {

    public static final ProjectWatermarkAssignerTransposeRule INSTANCE =
            new ProjectWatermarkAssignerTransposeRule();

    public ProjectWatermarkAssignerTransposeRule() {
        super(
                operand(LogicalProject.class, operand(LogicalWatermarkAssigner.class, any())),
                FlinkRelFactories.FLINK_REL_BUILDER(),
                "FlinkProjectWatermarkAssignerTransposeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalWatermarkAssigner watermarkAssigner = call.rel(1);

        NestedSchema usedFieldInProjectIncludingRowTimeFields =
                getUsedFieldsInTopLevelProjectAndWatermarkAssigner(project, watermarkAssigner);

        // check whether there are unused top-level fields or nested fields that can be pruned
        return usedFieldInProjectIncludingRowTimeFields.columns().size()
                        != watermarkAssigner.getRowType().getFieldCount()
                || usedFieldInProjectIncludingRowTimeFields.columns().values().stream()
                        .anyMatch(column -> !column.isLeaf());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalWatermarkAssigner watermarkAssigner = call.rel(1);

        // NOTES: DON'T use the nestedSchema datatype to build the transposed project.
        NestedSchema nestedSchema =
                getUsedFieldsInTopLevelProjectAndWatermarkAssigner(project, watermarkAssigner);
        FlinkRelBuilder builder =
                (FlinkRelBuilder) call.builder().push(watermarkAssigner.getInput());
        RexBuilder rexBuilder = builder.getRexBuilder();
        List<String> inputFieldNames = watermarkAssigner.getRowType().getFieldNames();

        // forward only the used (nested) fields; this also sets indexOfLeafInNewSchema
        List<RexNode> transposedProjects = new ArrayList<>();
        List<String> transposedNames = new ArrayList<>();
        for (int[] path : NestedProjectionUtil.convertToIndexArray(nestedSchema)) {
            RexNode expr = builder.field(path[0]);
            StringBuilder name = new StringBuilder(inputFieldNames.get(path[0]));
            for (int i = 1; i < path.length; i++) {
                name.append("_").append(expr.getType().getFieldNames().get(path[i]));
                expr = rexBuilder.makeFieldAccess(expr, path[i]);
            }
            transposedProjects.add(expr);
            transposedNames.add(name.toString());
        }
        // the rowtime column has no rowtime indicator
        builder.project(transposedProjects, transposedNames);

        RexNode newWatermarkExpr =
                NestedProjectionUtil.rewrite(
                                Collections.singletonList(watermarkAssigner.watermarkExpr()),
                                nestedSchema,
                                rexBuilder,
                                RelOptUtil.getFieldTypeList(builder.peek().getRowType()))
                        .get(0);
        String rowTimeName = inputFieldNames.get(watermarkAssigner.rowtimeFieldIndex());
        builder.watermark(
                nestedSchema.columns().get(rowTimeName).indexOfLeafInNewSchema(), newWatermarkExpr);

        List<RexNode> newProjects =
                NestedProjectionUtil.rewrite(
                        project.getProjects(),
                        nestedSchema,
                        rexBuilder,
                        RelOptUtil.getFieldTypeList(builder.peek().getRowType()));
        RelNode newProject =
                builder.project(newProjects, project.getRowType().getFieldNames()).build();
        call.transformTo(newProject);
    }

    private NestedSchema getUsedFieldsInTopLevelProjectAndWatermarkAssigner(
            LogicalProject project, LogicalWatermarkAssigner watermarkAssigner) {
        List<RexNode> usedFields = new ArrayList<>(project.getProjects());
        usedFields.add(watermarkAssigner.watermarkExpr());
        // the transposed project always has to forward the rowtime column
        usedFields.add(
                RexInputRef.of(
                        watermarkAssigner.rowtimeFieldIndex(), watermarkAssigner.getRowType()));
        return NestedProjectionUtil.build(usedFields, watermarkAssigner.getRowType());
    }
}
