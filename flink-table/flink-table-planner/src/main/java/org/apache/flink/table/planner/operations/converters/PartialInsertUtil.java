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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

        final Map<String, SqlLiteral> partitionValues = new HashMap<>();
        for (SqlNode node : staticPartitions) {
            final SqlProperty property = (SqlProperty) node;
            partitionValues.put(property.getKey().getSimple(), (SqlLiteral) property.getValue());
        }

        final List<String> targetColumns;
        if (targetColumnList == null) {
            targetColumns = null;
        } else {
            targetColumns =
                    targetColumnList.stream()
                            .map(column -> ((SqlIdentifier) column).getSimple())
                            .collect(Collectors.toList());
            // PreValidateReWriter can only check this before validation for sources whose column
            // count is known syntactically, so not for a SELECT *.
            if (targetColumns.size() != queryColumnCount) {
                throw validationError(
                        SqlUtil.newContextException(
                                insert.getSource().getParserPosition(),
                                RESOURCE.columnCountMismatch()));
            }
        }

        // PreValidateReWriter resolved these names against the same schema. An unresolved name
        // here would pad the sink column with NULL and drop the query column.
        final List<String> sinkFieldNames = sinkRowType.getFieldNames();
        checkState(
                sinkFieldNames.containsAll(partitionValues.keySet())
                        && (targetColumns == null || sinkFieldNames.containsAll(targetColumns)),
                "Unresolved target column for sink columns %s",
                sinkFieldNames);

        final List<RexNode> projects = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        boolean projectionNeeded = !partitionValues.isEmpty();
        int nextQueryColumn = 0;
        for (RowType.RowField field : sinkRowType.getFields()) {
            final RelDataType fieldType =
                    typeFactory.createFieldTypeFromLogicalType(field.getType());
            final SqlLiteral partitionValue = partitionValues.get(field.getName());
            if (partitionValue != null) {
                projects.add(convertStaticPartitionValue(planner, partitionValue, fieldType));
            } else if (targetColumns != null) {
                final int queryColumn = targetColumns.indexOf(field.getName());
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
        while (targetColumns == null && nextQueryColumn < queryColumnCount) {
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
