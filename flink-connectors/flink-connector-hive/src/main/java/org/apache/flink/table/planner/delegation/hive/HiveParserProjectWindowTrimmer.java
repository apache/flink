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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.table.planner.delegation.hive.copy.HiveParserRowResolver;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.buildHiveToCalciteColumnMap;

/**
 * For the sql with window expression, it will be converted two project nodes by HiveParser. One of
 * is a project node containing all input fields and the corresponding window node, and another is a
 * project node only contains the selected fields.
 *
 * <p>For example:
 *
 * <pre>{@code
 * create table src(a int, b int, c int);
 * select a, count(b) over(order by a rows between 1 preceding and 1 following) from src;
 * }</pre>
 *
 * <p>will be converted into the RelNode like:
 *
 * <pre>{@code
 * LogicalProject(a=[$0], _o_c2=[$4])
 *  LogicalProject(a=[$0], b=[$1], c=[$2],
 *  _o_col3=[count($1) over(order by $0 desc nulls last rows between 1 preceding and 1 following)])
 *
 * }</pre>
 *
 * <p>The project node with window will contain all the fields, some of which are not necessary. And
 * it will remove the redundant nodes in rule {@link
 * org.apache.calcite.rel.rules.ProjectWindowTransposeRule}, and adjust the index referred in {@link
 * RexInputRef} for it remove some nodes. But it hasn't adjusted the index of lowerBound/upperBound,
 * which then cause problem when try to access the lowerBound/upperBound.
 *
 * <p>The class's behavior is quite same to {@link
 * org.apache.calcite.rel.rules.ProjectWindowTransposeRule}, but also adjusts the index of
 * lowerBound/upperBound.
 */
public class HiveParserProjectWindowTrimmer {

    /**
     * Remove the redundant nodes from the project node which contains over window node.
     *
     * @param selectProject the project node contains selected fields in top of the project node
     *     with window
     * @param projectWithWindow the project node which contains windows in the end of project
     *     expressions.
     * @return the new project node after trimming
     */
    public static RelNode trimProjectWindow(
            Project selectProject,
            Project projectWithWindow,
            Map<RelNode, HiveParserRowResolver> relToRowResolver,
            Map<RelNode, Map<String, Integer>> relToHiveColNameCalcitePosMap) {
        // get the over window nodes
        List<RexOver> rexOverList =
                projectWithWindow.getProjects().stream()
                        .filter(node -> node instanceof RexOver)
                        .map(node -> (RexOver) node)
                        .collect(Collectors.toList());
        // the fields size excluding the over window field in the project node with window
        int windowInputColumn = projectWithWindow.getProjects().size() - rexOverList.size();
        // find all field referred by over window and select project node
        final ImmutableBitSet beReferred =
                findReference(selectProject, rexOverList, windowInputColumn);

        // If all the input columns are referred,
        // it is impossible to trim anyone of them out
        if (beReferred.cardinality() == windowInputColumn) {
            return selectProject;
        }

        // Keep only the fields which are referred and the over window field
        final List<RexNode> exps = new ArrayList<>();
        final RelDataTypeFactory.Builder builder =
                projectWithWindow.getCluster().getTypeFactory().builder();

        final List<RelDataTypeField> rowTypeWindowInput =
                projectWithWindow.getRowType().getFieldList();
        // add index for referred field
        List<Integer> remainIndexInProjectWindow = new ArrayList<>(beReferred.asList());
        // add index for the over window field
        remainIndexInProjectWindow.addAll(
                IntStream.range(windowInputColumn, projectWithWindow.getProjects().size())
                        .boxed()
                        .collect(Collectors.toList()));
        for (int index : remainIndexInProjectWindow) {
            exps.add(projectWithWindow.getProjects().get(index));
            builder.add(rowTypeWindowInput.get(index));
        }

        // As the un-referred columns are trimmed,
        // the indices specified in select project would need to be adjusted
        final RexShuttle indexAdjustment =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        final int newIndex =
                                getAdjustedIndex(
                                        inputRef.getIndex(), beReferred, windowInputColumn);
                        return new RexInputRef(newIndex, inputRef.getType());
                    }
                };

        // adjust the top select project node
        final List<RexNode> topProjExps = indexAdjustment.visitList(selectProject.getProjects());

        // create a project with the project trimmed
        LogicalProject trimmedProject =
                LogicalProject.create(
                        projectWithWindow.getInput(),
                        Collections.emptyList(),
                        exps,
                        builder.build());

        // put row resolver for newly trimmed project node
        HiveParserRowResolver oldRowResolver = relToRowResolver.remove(projectWithWindow);
        if (oldRowResolver != null) {
            HiveParserRowResolver newProjectRR = new HiveParserRowResolver();
            List<ColumnInfo> oldColumnsInfo = oldRowResolver.getColumnInfos();
            for (int index : remainIndexInProjectWindow) {
                newProjectRR.put(
                        oldColumnsInfo.get(index).getTabAlias(),
                        oldColumnsInfo.get(index).getAlias(),
                        oldColumnsInfo.get(index));
            }
            relToRowResolver.put(trimmedProject, newProjectRR);
            relToHiveColNameCalcitePosMap.remove(projectWithWindow);
            relToHiveColNameCalcitePosMap.put(
                    trimmedProject, buildHiveToCalciteColumnMap(newProjectRR));
        }

        // create new project with adjusted field ref
        RelNode newProject =
                LogicalProject.create(
                        trimmedProject,
                        Collections.emptyList(),
                        topProjExps,
                        selectProject.getRowType());
        // put row resolver for newly project node
        relToRowResolver.put(newProject, relToRowResolver.remove(selectProject));
        relToHiveColNameCalcitePosMap.put(
                newProject, relToHiveColNameCalcitePosMap.remove(selectProject));
        return newProject;
    }

    private static ImmutableBitSet findReference(
            final Project project, List<RexOver> rexOverList, int windowInputColumn) {
        final ImmutableBitSet.Builder beReferred = ImmutableBitSet.builder();

        final RexShuttle referenceFinder =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        final int index = inputRef.getIndex();
                        if (index < windowInputColumn) {
                            beReferred.set(index);
                        }
                        return inputRef;
                    }
                };

        // reference in project
        referenceFinder.visitEach(project.getProjects());

        // reference in over windows
        for (RexOver rexOver : rexOverList) {
            RexWindow rexWindow = rexOver.getWindow();
            try {
                // reference in partition-By
                referenceFinder.visitEach(getPartitionKeys(rexWindow));
                // reference in order-By
                referenceFinder.visitEach(
                        getOrderKeys(rexWindow).stream()
                                .map(Pair::getKey)
                                .collect(Collectors.toList()));
                // reference in operand
                referenceFinder.visitEach(rexOver.getOperands());
            } catch (Exception e) {
                throw new RuntimeException("sd");
            }
        }
        return beReferred.build();
    }

    private static int getAdjustedIndex(
            final int initIndex, final ImmutableBitSet beReferred, final int windowInputColumn) {
        if (initIndex >= windowInputColumn) {
            return beReferred.cardinality() + (initIndex - windowInputColumn);
        } else {
            return beReferred.get(0, initIndex).cardinality();
        }
    }

    @SuppressWarnings("unchecked")
    private static Collection<RexNode> getPartitionKeys(RexWindow rexWindow) {
        // Hack logic,we must use reflection to get the fields, otherwise, it'll throw
        // NoSuchFieldException.
        // Hive connector depends on flink-table-calcite-bridge which won't do any shade,
        // so if we try to access partitionKeys using rexWindow.partitionKeys, it will then
        // consider to access a field named partitionKeys with type com.google.common.collect
        // .ImmutableList.
        // But in flink-table-planner, it'll shade com.google.xx to org.apache.flink.calcite.shaded
        // .com.google.xx. Then rexWindow will contains a field named partitionKeys with type
        // org.apache.flink.calcite.shaded
        // .com.google.collect.ImmutableList instead of com.google.collect.ImmutableList, so the
        // NoSuchFieldException will be thrown
        // todo: remove the hack logic after FLINK-32286
        return (Collection<RexNode>) getFieldValue(rexWindow, "partitionKeys");
    }

    @SuppressWarnings("unchecked")
    private static Collection<RexFieldCollation> getOrderKeys(RexWindow rexWindow) {
        // hack logic, we must use reflection to get the fields.
        // the reason is same as said in above method getPartitionKeys
        return (Collection<RexFieldCollation>) getFieldValue(rexWindow, "orderKeys");
    }

    private static Object getFieldValue(RexWindow rexWindow, String fieldName) {
        try {
            Class<?> clazz = rexWindow.getClass();
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(rexWindow);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field %s not found in class %s", fieldName, rexWindow.getClass()),
                    e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(
                    String.format(
                            "Unable to access field %s in class %s",
                            fieldName, rexWindow.getClass()),
                    e);
        }
    }
}
