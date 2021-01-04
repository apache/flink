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
import org.apache.flink.table.planner.plan.utils.NestedColumn;
import org.apache.flink.table.planner.plan.utils.NestedProjectionUtil;
import org.apache.flink.table.planner.plan.utils.NestedSchema;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Transpose between the {@link LogicalWatermarkAssigner} and {@link LogicalProject}. The transposed
 * {@link LogicalProject} works like a pruner to prune the unused fields from source. The top level
 * {@link LogicalProject} still has to do the calculation, filter and prune the rowtime column if
 * the query doesn't need.
 *
 * <p>NOTES: Currently the rule doesn't support nested projection push down.
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

        // check the field number of the input in watermark assigner is equal to the used fields in
        // watermark assigner and project

        return usedFieldInProjectIncludingRowTimeFields.columns().size()
                != watermarkAssigner.getRowType().getFieldCount();
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
        List<RexInputRef> transposedProjects = new LinkedList<>();
        List<String> usedNames = new LinkedList<>();

        // TODO: support nested projection push down in transpose
        // add the used column RexInputRef and names into list
        for (NestedColumn column : nestedSchema.columns().values()) {
            // mark by hand
            column.setIndexOfLeafInNewSchema(transposedProjects.size());
            column.markLeaf();

            usedNames.add(column.name());
            transposedProjects.add(builder.field(column.indexInOriginSchema()));
        }

        // get the rowtime field index in the transposed project
        String rowTimeName =
                watermarkAssigner
                        .getRowType()
                        .getFieldNames()
                        .get(watermarkAssigner.rowtimeFieldIndex());
        int indexOfRowTimeInTransposedProject;
        if (nestedSchema.columns().get(rowTimeName) == null) {
            // push the RexInputRef of the rowtime into the list
            int rowTimeIndexInInput = watermarkAssigner.rowtimeFieldIndex();
            indexOfRowTimeInTransposedProject = transposedProjects.size();
            transposedProjects.add(builder.field(rowTimeIndexInInput));
            usedNames.add(rowTimeName);
        } else {
            // find rowtime ref in the list and mark the location
            indexOfRowTimeInTransposedProject =
                    nestedSchema.columns().get(rowTimeName).indexOfLeafInNewSchema();
        }

        // the rowtime column has no rowtime indicator
        builder.project(transposedProjects, usedNames);

        // rewrite the top level field reference
        RexNode newWatermarkExpr =
                watermarkAssigner
                        .watermarkExpr()
                        .accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef(RexInputRef inputRef) {
                                        String fieldName =
                                                watermarkAssigner
                                                        .getRowType()
                                                        .getFieldNames()
                                                        .get(inputRef.getIndex());
                                        return builder.field(
                                                nestedSchema
                                                        .columns()
                                                        .get(fieldName)
                                                        .indexOfLeafInNewSchema());
                                    }
                                });

        builder.watermark(indexOfRowTimeInTransposedProject, newWatermarkExpr);

        List<RexNode> newProjects =
                NestedProjectionUtil.rewrite(
                        project.getProjects(), nestedSchema, call.builder().getRexBuilder());
        RelNode newProject =
                builder.project(newProjects, project.getRowType().getFieldNames()).build();
        call.transformTo(newProject);
    }

    private NestedSchema getUsedFieldsInTopLevelProjectAndWatermarkAssigner(
            LogicalProject project, LogicalWatermarkAssigner watermarkAssigner) {
        List<RexNode> usedFields = new ArrayList<>(project.getProjects());
        usedFields.add(watermarkAssigner.watermarkExpr());
        return NestedProjectionUtil.build(usedFields, watermarkAssigner.getRowType());
    }
}
