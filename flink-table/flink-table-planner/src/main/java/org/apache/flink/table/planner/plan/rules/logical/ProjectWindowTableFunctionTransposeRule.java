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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.SqlWindowTableFunction;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.utils.WindowUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Planner rule that pushes a {@link LogicalProject} into a {@link LogicalTableFunctionScan} which
 * contains a Window table function call by splitting the projection into a projection on top of
 * child of the TableFunctionScan.
 */
public class ProjectWindowTableFunctionTransposeRule extends RelOptRule {

    public static final ProjectWindowTableFunctionTransposeRule INSTANCE =
            new ProjectWindowTableFunctionTransposeRule();

    public ProjectWindowTableFunctionTransposeRule() {
        super(
                operand(LogicalProject.class, operand(LogicalTableFunctionScan.class, any())),
                "ProjectWindowTableFunctionTransposeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalTableFunctionScan scan = call.rel(1);
        return WindowUtil.isWindowTableFunctionCall(scan.getCall());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalTableFunctionScan scan = call.rel(1);
        RelNode scanInput = scan.getInput(0);
        TimeAttributeWindowingStrategy windowingStrategy =
                WindowUtil.convertToWindowingStrategy(
                        (RexCall) scan.getCall(), scanInput.getRowType());
        // 1. get fields to push down
        ImmutableBitSet projectFields = RelOptUtil.InputFinder.bits(project.getProjects(), null);
        int scanInputFieldCount = scanInput.getRowType().getFieldCount();
        ImmutableBitSet toPushFields =
                ImmutableBitSet.range(0, scanInputFieldCount)
                        .intersect(projectFields)
                        .set(windowingStrategy.getTimeAttributeIndex());
        if (toPushFields.cardinality() == scanInputFieldCount) {
            return;
        }

        // 2. create new input of window table function scan
        RelBuilder relBuilder = call.builder();
        RelNode newScanInput = createInnerProject(relBuilder, scanInput, toPushFields);

        // mapping origin field index to new field index, used to rewrite WindowTableFunction and
        // top project
        Map<Integer, Integer> mapping =
                getFieldMapping(
                        scan.getRowType().getFieldCount(), scanInputFieldCount, toPushFields);

        // 3. create new window table function scan
        LogicalTableFunctionScan newScan =
                createNewTableFunctionScan(
                        relBuilder,
                        scan,
                        windowingStrategy.getTimeAttributeType(),
                        newScanInput,
                        mapping);

        // 4. create top project
        RelNode topProject = createTopProject(relBuilder, project, newScan, mapping);
        call.transformTo(topProject);
    }

    private Map<Integer, Integer> getFieldMapping(
            int scanFieldCount, int scanInputFieldCount, ImmutableBitSet toPushFields) {
        int toPushFieldCount = toPushFields.cardinality();
        Map<Integer, Integer> mapping = new HashMap<>();
        IntStream.range(0, scanFieldCount)
                .forEach(
                        idx -> {
                            int newPosition;
                            if (idx < scanInputFieldCount) {
                                newPosition = toPushFields.indexOf(idx);
                            } else {
                                newPosition = toPushFieldCount + idx - scanInputFieldCount;
                            }
                            mapping.put(idx, newPosition);
                        });
        return mapping;
    }

    private RelNode createInnerProject(
            RelBuilder relBuilder, RelNode scanInput, ImmutableBitSet toPushFields) {
        relBuilder.push(scanInput);
        List<RexInputRef> newProjects =
                toPushFields.toList().stream().map(relBuilder::field).collect(Collectors.toList());
        return relBuilder.project(newProjects).build();
    }

    private LogicalTableFunctionScan createNewTableFunctionScan(
            RelBuilder relBuilder,
            LogicalTableFunctionScan oldScan,
            LogicalType timeAttributeType,
            RelNode newInput,
            Map<Integer, Integer> mapping) {
        relBuilder.push(newInput);
        RexNode newCall = rewriteWindowCall((RexCall) oldScan.getCall(), mapping, relBuilder);
        RelOptCluster cluster = oldScan.getCluster();
        FlinkTypeFactory typeFactory = (FlinkTypeFactory) cluster.getTypeFactory();
        RelDataType newScanOutputType =
                SqlWindowTableFunction.inferRowType(
                        typeFactory,
                        newInput.getRowType(),
                        typeFactory.createFieldTypeFromLogicalType(timeAttributeType));
        return LogicalTableFunctionScan.create(
                cluster,
                new ArrayList<>(Collections.singleton(newInput)),
                newCall,
                oldScan.getElementType(),
                newScanOutputType,
                oldScan.getColumnMappings());
    }

    private RexNode rewriteWindowCall(
            RexCall windowCall, Map<Integer, Integer> mapping, RelBuilder relBuilder) {
        List<RexNode> newOperands = new ArrayList<>();
        for (RexNode next : windowCall.getOperands()) {
            newOperands.add(adjustInputRef(next, mapping));
        }
        return relBuilder.call(windowCall.getOperator(), newOperands);
    }

    private RelNode createTopProject(
            RelBuilder relBuilder,
            LogicalProject oldProject,
            LogicalTableFunctionScan newInput,
            Map<Integer, Integer> mapping) {
        List<Pair<RexNode, String>> newTopProjects =
                oldProject.getNamedProjects().stream()
                        .map(r -> Pair.of(adjustInputRef(r.left, mapping), r.right))
                        .collect(Collectors.toList());
        return relBuilder
                .push(newInput)
                .project(Pair.left(newTopProjects), Pair.right(newTopProjects))
                .build();
    }

    private RexNode adjustInputRef(RexNode expr, Map<Integer, Integer> mapping) {
        return expr.accept(
                new RexShuttle() {

                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        Integer newIndex = mapping.get(inputRef.getIndex());
                        return new RexInputRef(newIndex, inputRef.getType());
                    }
                });
    }
}
