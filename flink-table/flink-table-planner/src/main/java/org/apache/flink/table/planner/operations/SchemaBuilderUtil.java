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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedPrimaryKey;
import org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** A utility class for {@link MergeTableAsUtil} and {@link MergeTableLikeUtil} classes. */
public class SchemaBuilderUtil {
    final SqlValidator sqlValidator;
    final Function<SqlNode, String> escapeExpressions;
    final DataTypeFactory dataTypeFactory;

    Map<String, Schema.UnresolvedColumn> columns = new LinkedHashMap<>();
    Map<String, UnresolvedWatermarkSpec> watermarkSpecs = new HashMap<>();
    UnresolvedPrimaryKey primaryKey = null;

    SchemaBuilderUtil(
            SqlValidator sqlValidator,
            Function<SqlNode, String> escapeExpressions,
            DataTypeFactory dataTypeFactory) {
        this.sqlValidator = sqlValidator;
        this.escapeExpressions = escapeExpressions;
        this.dataTypeFactory = dataTypeFactory;
    }

    /** Sets the primary key for the schema. */
    void setPrimaryKey(SqlTableConstraint primaryKeyConstraint) {
        if (primaryKey != null) {
            throw new ValidationException(
                    "There already exists a primary key constraint in the table.");
        }

        for (SqlNode primaryKeyNode : primaryKeyConstraint.getColumns()) {
            String primaryKey = ((SqlIdentifier) primaryKeyNode).getSimple();

            if (!columns.containsKey(primaryKey)) {
                throw new ValidationException(
                        String.format(
                                "Primary key column '%s' is not defined in the schema at %s",
                                primaryKey, primaryKeyNode.getParserPosition()));
            }

            if (!(columns.get(primaryKey) instanceof UnresolvedPhysicalColumn)) {
                throw new ValidationException(
                        String.format(
                                "Could not create a PRIMARY KEY with column '%s' at %s.\n"
                                        + "A PRIMARY KEY constraint must be declared on physical columns.",
                                primaryKey, primaryKeyNode.getParserPosition()));
            }
        }

        primaryKey = toUnresolvedPrimaryKey(primaryKeyConstraint);
    }

    void addWatermarks(
            List<SqlWatermark> derivedWatermarkSpecs,
            Map<String, RelDataType> allFieldsTypes,
            boolean overwriteWatermark) {
        for (SqlWatermark derivedWatermarkSpec : derivedWatermarkSpecs) {
            SqlIdentifier eventTimeColumnName = derivedWatermarkSpec.getEventTimeColumnName();
            String rowtimeAttribute = eventTimeColumnName.toString();

            if (!overwriteWatermark && watermarkSpecs.containsKey(rowtimeAttribute)) {
                throw new ValidationException(
                        String.format(
                                "There already exists a watermark on column '%s'.",
                                rowtimeAttribute));
            }

            verifyRowtimeAttribute(derivedWatermarkSpec, allFieldsTypes);
            watermarkSpecs.put(
                    rowtimeAttribute,
                    toUnresolvedWatermarkSpec(derivedWatermarkSpec, allFieldsTypes));
        }
    }

    /**
     * Builds and returns a {@link Schema} from the columns, watermark specs, and primary key
     * specified in the builder.
     */
    public Schema build() {
        Schema.Builder resultBuilder = Schema.newBuilder();
        resultBuilder.fromColumns(new ArrayList<>(columns.values()));

        for (UnresolvedWatermarkSpec watermarkSpec : watermarkSpecs.values()) {
            resultBuilder.watermark(
                    watermarkSpec.getColumnName(), watermarkSpec.getWatermarkExpression());
        }

        if (primaryKey != null) {
            resultBuilder.primaryKeyNamed(
                    primaryKey.getConstraintName(),
                    primaryKey.getColumnNames().toArray(new String[0]));
        }

        return resultBuilder.build();
    }

    /**
     * Verify the watermark rowtime attribute is part of the table schema specified in the {@code
     * allFieldsTypes}.
     *
     * @param sqlWatermark The watermark with the rowtime attribute to verify.
     * @param allFieldsTypes The table schema to verify the rowtime attribute against.
     */
    static void verifyRowtimeAttribute(
            SqlWatermark sqlWatermark, Map<String, RelDataType> allFieldsTypes) {
        SqlIdentifier eventTimeColumnName = sqlWatermark.getEventTimeColumnName();
        String fullRowtimeExpression = eventTimeColumnName.toString();

        List<String> components = eventTimeColumnName.names;
        if (!allFieldsTypes.containsKey(components.get(0))) {
            throw new ValidationException(
                    String.format(
                            "The rowtime attribute field '%s' is not defined in the table schema, at %s\n"
                                    + "Available fields: [%s]",
                            fullRowtimeExpression,
                            eventTimeColumnName.getParserPosition(),
                            allFieldsTypes.keySet().stream()
                                    .collect(Collectors.joining("', '", "'", "'"))));
        }

        if (components.size() > 1) {
            RelDataType componentType = allFieldsTypes.get(components.get(0));
            for (int i = 1; i < components.size(); i++) {
                RelDataTypeField field = componentType.getField(components.get(i), true, false);
                if (field == null) {
                    throw new ValidationException(
                            String.format(
                                    "The rowtime attribute field '%s' is not defined in the table schema, at %s\n"
                                            + "Nested field '%s' was not found in a composite type: %s.",
                                    fullRowtimeExpression,
                                    eventTimeColumnName.getComponent(i).getParserPosition(),
                                    components.get(i),
                                    FlinkTypeFactory.toLogicalType(
                                            allFieldsTypes.get(components.get(0)))));
                }
                componentType = field.getType();
            }
        }
    }

    /** Converts a {@link SqlRegularColumn} to an {@link UnresolvedPhysicalColumn} object. */
    UnresolvedPhysicalColumn toUnresolvedPhysicalColumn(SqlRegularColumn column) {
        final String name = column.getName().getSimple();
        final Optional<String> comment = getComment(column);
        final LogicalType logicalType = toLogicalType(toRelDataType(column.getType()));

        return new UnresolvedPhysicalColumn(
                name, fromLogicalToDataType(logicalType), comment.orElse(null));
    }

    /** Converts a {@link SqlComputedColumn} to an {@link UnresolvedComputedColumn} object. */
    UnresolvedComputedColumn toUnresolvedComputedColumn(
            SqlComputedColumn column, SqlNode validatedExpression) {
        final String name = column.getName().getSimple();
        final Optional<String> comment = getComment(column);

        return new UnresolvedComputedColumn(
                name,
                new SqlCallExpression(escapeExpressions.apply(validatedExpression)),
                comment.orElse(null));
    }

    /** Converts a {@link SqlMetadataColumn} to an {@link UnresolvedMetadataColumn} object. */
    UnresolvedMetadataColumn toUnresolvedMetadataColumn(SqlMetadataColumn column) {
        final String name = column.getName().getSimple();
        final Optional<String> comment = getComment(column);
        final LogicalType logicalType = toLogicalType(toRelDataType(column.getType()));

        return new UnresolvedMetadataColumn(
                name,
                fromLogicalToDataType(logicalType),
                column.getMetadataAlias().orElse(null),
                column.isVirtual(),
                comment.orElse(null));
    }

    /** Converts a {@link SqlWatermark} to an {@link UnresolvedWatermarkSpec} object. */
    UnresolvedWatermarkSpec toUnresolvedWatermarkSpec(
            SqlWatermark watermark, Map<String, RelDataType> accessibleFieldNamesToTypes) {
        // this will validate and expand function identifiers.
        SqlNode validated =
                sqlValidator.validateParameterizedExpression(
                        watermark.getWatermarkStrategy(), accessibleFieldNamesToTypes);

        return new UnresolvedWatermarkSpec(
                watermark.getEventTimeColumnName().toString(),
                new SqlCallExpression(escapeExpressions.apply(validated)));
    }

    /** Converts a {@link SqlTableConstraint} to an {@link UnresolvedPrimaryKey} object. */
    public UnresolvedPrimaryKey toUnresolvedPrimaryKey(SqlTableConstraint primaryKey) {
        List<String> columnNames =
                primaryKey.getColumns().getList().stream()
                        .map(n -> ((SqlIdentifier) n).getSimple())
                        .collect(Collectors.toList());

        String constraintName =
                primaryKey
                        .getConstraintName()
                        .orElseGet(
                                () ->
                                        columnNames.stream()
                                                .collect(Collectors.joining("_", "PK_", "")));

        return new UnresolvedPrimaryKey(constraintName, columnNames);
    }

    /**
     * Gets the column data type of {@link UnresolvedPhysicalColumn} column and convert it to a
     * {@link LogicalType}.
     */
    LogicalType getLogicalType(UnresolvedPhysicalColumn column) {
        return dataTypeFactory.createDataType(column.getDataType()).getLogicalType();
    }

    Optional<String> getComment(SqlTableColumn column) {
        return column.getComment().map(c -> ((SqlLiteral) c).getValueAs(String.class));
    }

    RelDataType toRelDataType(SqlDataTypeSpec type) {
        boolean nullable = type.getNullable() == null || type.getNullable();
        return type.deriveType(sqlValidator, nullable);
    }
}
