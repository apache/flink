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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.SqlProperty;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.calcite.util.Static.RESOURCE;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Projects the source query of an {@code INSERT INTO} with static partitions or a target column
 * list onto the sink's persisted columns, after validation. Static partition columns become their
 * value cast to the column type, unlisted columns become {@code NULL}. The target columns are
 * validated before that in {@link org.apache.flink.table.planner.calcite.PreValidateReWriter}.
 */
@Internal
public final class PartialInsertUtil {

    private PartialInsertUtil() {}

    /**
     * Returns the query projected onto the sink's persisted columns, or the query itself when the
     * statement has no static partitions and its target column list neither reorders nor pads it.
     */
    public static PlannerQueryOperation alignWithSink(
            FlinkPlannerImpl planner,
            RichSqlInsert insert,
            ResolvedSchema sinkSchema,
            PlannerQueryOperation query) {
        final SqlNodeList targetColumnList = insert.getTargetColumnList();
        final SqlNodeList staticPartitions = insert.getStaticPartitions();
        if (targetColumnList == null && staticPartitions.isEmpty()) {
            return query;
        }

        final RelNode queryRel = query.getCalciteTree();
        final int queryColumnCount = queryRel.getRowType().getFieldCount();
        final RelOptCluster cluster = queryRel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final FlinkTypeFactory typeFactory = (FlinkTypeFactory) cluster.getTypeFactory();
        final RowType sinkRowType = (RowType) sinkSchema.toSinkRowDataType().getLogicalType();

        final List<RowType.RowField> sinkFields = sinkRowType.getFields();
        final Map<String, Integer> sinkFieldIndices =
                createSinkFieldIndices(planner, sinkRowType.getFieldNames());

        final SqlLiteral[] partitionValues = new SqlLiteral[sinkFields.size()];
        for (SqlNode node : staticPartitions) {
            final SqlProperty property = (SqlProperty) node;
            partitionValues[resolve(sinkFieldIndices, property.getKey())] =
                    (SqlLiteral) property.getValue();
        }

        // query column per sink column, -1 for an unlisted column
        final int[] queryColumns;
        if (targetColumnList == null) {
            queryColumns = null;
        } else {
            // PreValidateReWriter can only check this before validation for sources whose column
            // count is known syntactically, so not for a SELECT *.
            if (targetColumnList.size() != queryColumnCount) {
                throw validationError(
                        SqlUtil.newContextException(
                                insert.getSource().getParserPosition(),
                                RESOURCE.columnCountMismatch()));
            }
            queryColumns = new int[sinkFields.size()];
            Arrays.fill(queryColumns, -1);
            for (int i = 0; i < targetColumnList.size(); i++) {
                queryColumns[resolve(sinkFieldIndices, (SqlIdentifier) targetColumnList.get(i))] =
                        i;
            }
        }

        final List<RexNode> projects = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        boolean projectionNeeded = !staticPartitions.isEmpty();
        int nextQueryColumn = 0;
        for (int i = 0; i < sinkFields.size(); i++) {
            final RowType.RowField field = sinkFields.get(i);
            final RelDataType fieldType =
                    typeFactory.createFieldTypeFromLogicalType(field.getType());
            if (partitionValues[i] != null) {
                projects.add(convertStaticPartitionValue(planner, partitionValues[i], fieldType));
            } else if (queryColumns != null) {
                final int queryColumn = queryColumns[i];
                if (queryColumn < 0) {
                    // PreValidateReWriter has verified that the column is nullable
                    projects.add(rexBuilder.makeNullLiteral(fieldType));
                    projectionNeeded = true;
                } else {
                    if (queryColumn != projects.size()) {
                        projectionNeeded = true;
                    }
                    projects.add(rexBuilder.makeInputRef(queryRel, queryColumn));
                }
            } else if (nextQueryColumn < queryColumnCount) {
                projects.add(rexBuilder.makeInputRef(queryRel, nextQueryColumn++));
            } else {
                // Fewer query columns than sink columns: the sink validation reports this. Keep
                // looping so that later static partition columns stay in the projection.
                continue;
            }
            fieldNames.add(field.getName());
        }
        // surplus query columns are kept so that the sink validation reports the mismatch
        while (queryColumns == null && nextQueryColumn < queryColumnCount) {
            projects.add(rexBuilder.makeInputRef(queryRel, nextQueryColumn));
            fieldNames.add(queryRel.getRowType().getFieldNames().get(nextQueryColumn));
            nextQueryColumn++;
        }

        if (!projectionNeeded) {
            return query;
        }
        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
        final RelNode projected =
                relBuilder.push(queryRel).project(projects, fieldNames, true).build();
        return new PlannerQueryOperation(
                projected,
                () -> OperationConverterUtils.getQuotedSqlString(insert.getSource(), planner));
    }

    /** Indexes the sink columns by name with the validator's case sensitivity. */
    private static Map<String, Integer> createSinkFieldIndices(
            FlinkPlannerImpl planner, List<String> sinkFieldNames) {
        final boolean caseSensitive =
                planner.getOrCreateSqlValidator()
                        .getCatalogReader()
                        .nameMatcher()
                        .isCaseSensitive();
        // first match wins, like RelDataType#getField
        final Map<String, Integer> indices =
                caseSensitive ? new HashMap<>() : new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < sinkFieldNames.size(); i++) {
            indices.putIfAbsent(sinkFieldNames.get(i), i);
        }
        return indices;
    }

    private static int resolve(Map<String, Integer> sinkFieldIndices, SqlIdentifier column) {
        final Integer index = sinkFieldIndices.get(column.getSimple());
        checkState(index != null, "Unresolved target column %s", column);
        return index;
    }

    private static RexNode convertStaticPartitionValue(
            FlinkPlannerImpl planner, SqlLiteral value, RelDataType targetType) {
        final FlinkCalciteSqlValidator validator = planner.getOrCreateSqlValidator();
        final RelDataTypeFactory typeFactory = validator.getTypeFactory();
        try {
            final SqlNode castValue =
                    validator.maybeCast(value, value.createSqlType(typeFactory), targetType);
            return planner.rex(castValue, typeFactory.builder().build(), null);
        } catch (RuntimeException e) {
            throw validationError(e);
        }
    }

    /** Same message as {@link FlinkPlannerImpl#validate}, which does not cover this conversion. */
    private static ValidationException validationError(RuntimeException cause) {
        return new ValidationException("SQL validation failed. " + cause.getMessage(), cause);
    }
}
