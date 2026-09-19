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
import org.apache.calcite.runtime.CalciteContextException;
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

/**
 * Aligns the source query of an {@code INSERT INTO} statement with the persisted columns of the
 * sink table when the statement declares static partitions and/or an explicit target column list.
 *
 * <p>The query is projected onto the persisted row type of the sink: listed columns are taken from
 * the query in the order of the column list, static partition columns become their constant value
 * cast to the column type, and every remaining column is padded with {@code NULL}. Without a column
 * list, the query columns fill the non-partition columns in order.
 *
 * <p>This runs on the relational plan after validation on purpose. Rewriting the SQL AST before
 * validation required validating the source query once for the rewrite and once for real, which
 * left the table arguments of set-semantic process table functions unvalidated and failed the
 * conversion (the same problem FLINK-40039 fixed for CTAS/RTAS). The target columns themselves
 * (existence, duplicates, nullability of unlisted columns) are still validated before the source in
 * {@link org.apache.flink.table.planner.calcite.PreValidateReWriter}.
 */
@Internal
public final class PartialInsertUtil {

    /**
     * Prefix that {@link org.apache.flink.table.planner.calcite.FlinkPlannerImpl#validate} puts in
     * front of every validation error. The column count of a source with a star can only be checked
     * after validation, i.e. outside that method, so it is reproduced here to keep one message for
     * the same mistake.
     */
    private static final String VALIDATION_ERROR_PREFIX = "SQL validation failed. ";

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
            // count is known syntactically, e.g. not for a SELECT *.
            if (targetColumns.size() != queryColumnCount) {
                final CalciteContextException cause =
                        SqlUtil.newContextException(
                                insert.getSource().getParserPosition(),
                                RESOURCE.columnCountMismatch());
                throw new ValidationException(VALIDATION_ERROR_PREFIX + cause.getMessage(), cause);
            }
        }

        if (targetColumns != null) {
            // PreValidateReWriter resolves the target columns against the same persisted schema, so
            // this cannot trigger; without it an unresolved name would silently pad the sink column
            // with NULL and drop the query column instead of failing.
            final List<String> sinkFieldNames = sinkRowType.getFieldNames();
            targetColumns.stream()
                    .filter(column -> !sinkFieldNames.contains(column))
                    .findFirst()
                    .ifPresent(
                            column -> {
                                throw new ValidationException(
                                        VALIDATION_ERROR_PREFIX
                                                + SqlUtil.newContextException(
                                                                insert.getSource()
                                                                        .getParserPosition(),
                                                                RESOURCE.unknownTargetColumn(
                                                                        column))
                                                        .getMessage());
                            });
        }

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
                // the query has fewer columns than the sink, the sink validation reports this.
                // The loop still runs to the end so that the static partition columns after it
                // stay in the projection and the mismatch names every sink column.
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

    /** Converts a static partition value into a constant of the partition column's type. */
    private static RexNode convertStaticPartitionValue(
            FlinkPlannerImpl planner, SqlLiteral value, RelDataType targetType) {
        final FlinkCalciteSqlValidator validator = planner.getOrCreateSqlValidator();
        final RelDataTypeFactory typeFactory = validator.getTypeFactory();
        final SqlNode castValue =
                validator.maybeCast(value, value.createSqlType(typeFactory), targetType);
        return planner.rex(castValue, typeFactory.builder().build(), null);
    }
}
