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

package org.apache.flink.table.planner.connectors;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Build a new projection for updating based on the update exprs and required columns. */
public class UpdateRowProjectionBuilder {

    /** The delimiter to construct the access path. */
    private static final String DELIMITER = "->";

    /**
     * The nested nodes of the required columns. The nodes typically can be
     * <li>{@link RexInputRef} for non-updated column
     * <li>Rex expression node for user-specified updated column
     * <li>Row expression for the row column with partial updated column
     */
    private final Map<String, RexNode> requiredNodes = new HashMap<>();

    /** All the complete access path of the fields, deriving from the RowType. */
    private final List<List<Integer>> allPaths = new ArrayList<>();

    /**
     * Required columns' indexes.
     *
     * <p>Note: The required column can exceed the original table rowType, because we can attach
     * readable metadata column derived from the table.
     */
    private final List<Integer> requiredColumnIndexes;

    /** The columns count of the original sink table. */
    private final int originColsCount;

    /** The row type of the sink table after enriched. */
    private final RowType rowType;

    private final int[][] updatedColumnIndexes;

    /** The input of the LogicalSink. */
    private final LogicalProject project;

    /** The filter of the update operation. */
    @Nullable private final LogicalFilter filter;

    /** The flink type factory. */
    private final FlinkTypeFactory typeFactory;

    /** The rex builder. */
    private final RexBuilder rexBuilder;

    public UpdateRowProjectionBuilder(
            RowType rowType,
            int[][] updatedColumnIndexes,
            LogicalProject project,
            @Nullable LogicalFilter filter,
            int originColsCount,
            List<Integer> requiredColumnIndexes,
            RexBuilder rexBuilder,
            FlinkTypeFactory typeFactory) {
        this.rexBuilder = rexBuilder;
        this.originColsCount = originColsCount;
        this.requiredColumnIndexes = requiredColumnIndexes;
        this.rowType = rowType;
        this.typeFactory = typeFactory;
        this.filter = filter;
        this.updatedColumnIndexes = updatedColumnIndexes;
        // the rex nodes for the project are like: index for all col, update expressions for the
        // updated columns
        this.project = project;
        buildAllPath();
        fillRequiredNodes();
    }

    /**
     * Create the new project based on the update expr and the required column.
     *
     * @return new project
     */
    public Project create() {
        List<Integer> path = new ArrayList<>();
        List<RexNode> newNodes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        for (int index : requiredColumnIndexes) {
            fieldNames.add(rowType.getFields().get(index).getName());
            path.add(index);
            String accessPath = getAccessPath(path);
            if (requiredNodes.containsKey(accessPath)) {
                newNodes.add(wrapCaseWhen(requiredNodes.get(accessPath), index));
                path.remove(path.size() - 1);
            } else {
                RexNode node = traverse(path, rowType.getFields().get(index));
                newNodes.add(wrapCaseWhen(node, index));
            }
        }

        return project.copy(
                project.getTraitSet(),
                // if filter is not null, we need to remove the filter in the plan since we
                // have rewritten the expression to
                // CASE WHEN filter THEN updated_expr ELSE col_expr END
                filter != null ? filter.getInput() : project.getInput(),
                newNodes,
                RexUtil.createStructType(typeFactory, newNodes, fieldNames, null));
    }

    private RexNode traverse(List<Integer> path, RowType.RowField field) {
        String accessPath = getAccessPath(path);
        if (requiredNodes.containsKey(accessPath)) {
            path.remove(path.size() - 1);
            return requiredNodes.get(accessPath);
        } else {
            Preconditions.checkArgument(
                    field.getType() instanceof RowType,
                    String.format(
                            "The field: %s type is %s, It's expected to be RowType",
                            field.getName(), field.getType()));
            RowType row = (RowType) field.getType();
            List<RexNode> nodes = new ArrayList<>();
            for (int i = 0; i < row.getFields().size(); i++) {
                path.add(i);
                nodes.add(traverse(path, row.getFields().get(i)));
            }
            RelDataType dataType =
                    RexUtil.createStructType(typeFactory, nodes, row.getFieldNames(), null);
            path.remove(path.size() - 1);
            return rexBuilder.makeCall(dataType, FlinkSqlOperatorTable.ROW, nodes);
        }
    }

    /**
     * When the required column contains the updated field, and the filter is supplied, wrap the
     * node into {@code CASE WHEN} call.
     *
     * @param node The node to be wrapped.
     * @param requiredColumnIndex The required column index.
     * @return The wrapped or original rex node.
     */
    private RexNode wrapCaseWhen(RexNode node, int requiredColumnIndex) {
        List<Integer> related =
                findUpdateIdxByRequiredColumn(updatedColumnIndexes, requiredColumnIndex);
        if (related.isEmpty() || filter == null) {
            return node;
        }

        RexInputRef inputRef = rexBuilder.makeInputRef(project.getInput(), requiredColumnIndex);
        return rexBuilder.makeCall(
                inputRef.getType(),
                FlinkSqlOperatorTable.CASE,
                Arrays.asList(filter.getCondition(), node, inputRef));
    }

    /** Get the related update clause index covered by required column. */
    private List<Integer> findUpdateIdxByRequiredColumn(
            int[][] targetUpdateIndexes, int requiredColumnIndex) {
        List<Integer> related = new ArrayList<>();
        for (int i = 0; i < targetUpdateIndexes.length; i++) {
            if (targetUpdateIndexes[i][0] == requiredColumnIndex) {
                related.add(i);
            }
        }
        return related;
    }

    /** Pre-build the requiredNodes according to the update column info. */
    private void fillRequiredNodes() {
        for (Integer requiredIndex : requiredColumnIndexes) {
            List<Integer> updateOrderedIds =
                    findUpdateIdxByRequiredColumn(updatedColumnIndexes, requiredIndex);
            // The required column contains the update col.
            if (!updateOrderedIds.isEmpty()) {
                // (1) Insert the update cols expr to requiredColumnNestedNodes.
                for (Integer idx : updateOrderedIds) {
                    int[] updatedColumnPath = updatedColumnIndexes[idx];
                    // project layout is: original table's fields + update expressions.
                    RexNode updateExpr = project.getProjects().get(originColsCount + idx);
                    String accessPath =
                            getAccessPath(
                                    Arrays.stream(updatedColumnPath)
                                            .boxed()
                                            .collect(Collectors.toList()));
                    requiredNodes.put(accessPath, updateExpr);
                }

                // (2) Insert the related nested cols of the required column index to
                // requiredColumnNestedNodes.
                List<List<Integer>> updatedRelatedPath =
                        allPaths.stream()
                                .filter(p -> p.get(0).equals(requiredIndex))
                                .collect(Collectors.toList());
                RexInputRef inputRef = rexBuilder.makeInputRef(project.getInput(), requiredIndex);
                for (List<Integer> path : updatedRelatedPath) {
                    String accessPath = getAccessPath(path);
                    if (!requiredNodes.containsKey(accessPath)) {
                        RexNode node = inputRef;
                        if (path.size() >= 2) {
                            for (int i = 1; i < path.size(); i++) {
                                node = rexBuilder.makeFieldAccess(node, path.get(i));
                            }
                        }
                        requiredNodes.put(accessPath, node);
                    }
                }
            } else {
                // just create input ref for non-update fields.
                requiredNodes.put(
                        getAccessPath(Collections.singletonList(requiredIndex)),
                        rexBuilder.makeInputRef(project.getInput(), requiredIndex));
            }
        }
    }

    /** Construct the complete path of each field. */
    private void buildAllPath() {
        for (int i = 0; i < rowType.getChildren().size(); i++) {
            List<Integer> path = new ArrayList<>();
            buildPath(rowType.getChildren().get(i), i, path);
        }
    }

    private void buildPath(LogicalType childType, int idxInRow, List<Integer> path) {
        path.add(idxInRow);
        // non struct type
        if (childType.getChildren().isEmpty()) {
            allPaths.add(new ArrayList<>(path));
        } else {
            for (int i = 0; i < childType.getChildren().size(); i++) {
                buildPath(childType.getChildren().get(i), i, path);
            }
        }
        path.remove(path.size() - 1);
    }

    private String getAccessPath(List<Integer> path) {
        return path.stream().map(String::valueOf).collect(Collectors.joining(DELIMITER));
    }
}
