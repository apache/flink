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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedPrimaryKey;
import org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver.ExpressionResolverBuilder;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.expressions.ApiExpressionUtils.localRef;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.canBeTimeAttributeType;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.utils.DataTypeUtils.replaceLogicalType;

/** Default implementation of {@link SchemaResolver}. */
@Internal
class DefaultSchemaResolver implements SchemaResolver {

    private final boolean isStreamingMode;
    private final DataTypeFactory dataTypeFactory;
    private final ExpressionResolverBuilder resolverBuilder;

    DefaultSchemaResolver(
            boolean isStreamingMode,
            DataTypeFactory dataTypeFactory,
            ExpressionResolverBuilder resolverBuilder) {
        this.isStreamingMode = isStreamingMode;
        this.dataTypeFactory = dataTypeFactory;
        this.resolverBuilder = resolverBuilder;
    }

    @Override
    public ResolvedSchema resolve(Schema schema) {
        final List<Column> columns = resolveColumns(schema.getColumns());

        final List<WatermarkSpec> watermarkSpecs =
                resolveWatermarkSpecs(schema.getWatermarkSpecs(), columns);

        final List<Column> columnsWithRowtime = adjustRowtimeAttributes(watermarkSpecs, columns);

        final UniqueConstraint primaryKey =
                resolvePrimaryKey(schema.getPrimaryKey().orElse(null), columnsWithRowtime);

        return new ResolvedSchema(columnsWithRowtime, watermarkSpecs, primaryKey);
    }

    // --------------------------------------------------------------------------------------------

    private List<Column> resolveColumns(List<Schema.UnresolvedColumn> unresolvedColumns) {

        validateDuplicateColumns(unresolvedColumns);

        final Column[] resolvedColumns = new Column[unresolvedColumns.size()];
        // process source columns first before computed columns
        for (int pos = 0; pos < unresolvedColumns.size(); pos++) {
            final Schema.UnresolvedColumn unresolvedColumn = unresolvedColumns.get(pos);
            if (unresolvedColumn instanceof UnresolvedPhysicalColumn) {
                resolvedColumns[pos] =
                        resolvePhysicalColumn((UnresolvedPhysicalColumn) unresolvedColumn);
            } else if (unresolvedColumn instanceof UnresolvedMetadataColumn) {
                resolvedColumns[pos] =
                        resolveMetadataColumn((UnresolvedMetadataColumn) unresolvedColumn);
            } else if (!(unresolvedColumn instanceof UnresolvedComputedColumn)) {
                throw new IllegalArgumentException(
                        "Unknown unresolved column type: " + unresolvedColumn.getClass().getName());
            }
        }
        // fill in computed columns
        final List<Column> sourceColumns =
                Stream.of(resolvedColumns).filter(Objects::nonNull).collect(Collectors.toList());
        for (int pos = 0; pos < unresolvedColumns.size(); pos++) {
            final Schema.UnresolvedColumn unresolvedColumn = unresolvedColumns.get(pos);
            if (unresolvedColumn instanceof UnresolvedComputedColumn) {
                resolvedColumns[pos] =
                        resolveComputedColumn(
                                (UnresolvedComputedColumn) unresolvedColumn, sourceColumns);
            }
        }

        return Arrays.asList(resolvedColumns);
    }

    private PhysicalColumn resolvePhysicalColumn(UnresolvedPhysicalColumn unresolvedColumn) {
        return Column.physical(
                        unresolvedColumn.getName(),
                        dataTypeFactory.createDataType(unresolvedColumn.getDataType()))
                .withComment(unresolvedColumn.getComment().orElse(null));
    }

    private MetadataColumn resolveMetadataColumn(UnresolvedMetadataColumn unresolvedColumn) {
        return Column.metadata(
                        unresolvedColumn.getName(),
                        dataTypeFactory.createDataType(unresolvedColumn.getDataType()),
                        unresolvedColumn.getMetadataKey(),
                        unresolvedColumn.isVirtual())
                .withComment(unresolvedColumn.getComment().orElse(null));
    }

    private ComputedColumn resolveComputedColumn(
            UnresolvedComputedColumn unresolvedColumn, List<Column> inputColumns) {
        final ResolvedExpression resolvedExpression;
        try {
            resolvedExpression =
                    resolveExpression(inputColumns, unresolvedColumn.getExpression(), null);
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Invalid expression for computed column '%s'.",
                            unresolvedColumn.getName()),
                    e);
        }
        return Column.computed(unresolvedColumn.getName(), resolvedExpression)
                .withComment(unresolvedColumn.getComment().orElse(null));
    }

    private void validateDuplicateColumns(List<Schema.UnresolvedColumn> columns) {
        final List<String> names =
                columns.stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList());
        final List<String> duplicates =
                names.stream()
                        .filter(name -> Collections.frequency(names, name) > 1)
                        .distinct()
                        .collect(Collectors.toList());
        if (duplicates.size() > 0) {
            throw new ValidationException(
                    String.format(
                            "Schema must not contain duplicate column names. Found duplicates: %s",
                            duplicates));
        }
    }

    private List<WatermarkSpec> resolveWatermarkSpecs(
            List<UnresolvedWatermarkSpec> unresolvedWatermarkSpecs, List<Column> inputColumns) {
        if (unresolvedWatermarkSpecs.size() == 0) {
            return Collections.emptyList();
        }
        if (unresolvedWatermarkSpecs.size() > 1) {
            throw new ValidationException("Multiple watermark definitions are not supported yet.");
        }
        final UnresolvedWatermarkSpec watermarkSpec = unresolvedWatermarkSpecs.get(0);

        // validate time attribute
        final String timeColumn = watermarkSpec.getColumnName();
        final Column validatedTimeColumn = validateTimeColumn(timeColumn, inputColumns);

        // resolve watermark expression
        final ResolvedExpression watermarkExpression;
        try {
            watermarkExpression =
                    resolveExpression(
                            inputColumns,
                            watermarkSpec.getWatermarkExpression(),
                            validatedTimeColumn.getDataType());
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Invalid expression for watermark '%s'.", watermarkSpec.toString()),
                    e);
        }
        final LogicalType outputType = watermarkExpression.getOutputDataType().getLogicalType();
        final LogicalType timeColumnType = validatedTimeColumn.getDataType().getLogicalType();
        validateWatermarkExpression(outputType);

        if (outputType.getTypeRoot() != timeColumnType.getTypeRoot()) {
            throw new ValidationException(
                    String.format(
                            "The watermark declaration's output data type '%s' is different "
                                    + "from the time field's data type '%s'.",
                            outputType, timeColumnType));
        }

        return Collections.singletonList(
                WatermarkSpec.of(watermarkSpec.getColumnName(), watermarkExpression));
    }

    private Column validateTimeColumn(String columnName, List<Column> columns) {
        final Optional<Column> timeColumn =
                columns.stream().filter(c -> c.getName().equals(columnName)).findFirst();
        if (!timeColumn.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Invalid column name '%s' for rowtime attribute in watermark declaration. Available columns are: %s",
                            columnName,
                            columns.stream().map(Column::getName).collect(Collectors.toList())));
        }
        final LogicalType timeFieldType = timeColumn.get().getDataType().getLogicalType();
        if (!canBeTimeAttributeType(timeFieldType) || getPrecision(timeFieldType) > 3) {
            throw new ValidationException(
                    String.format(
                            "Invalid data type of time field for watermark definition. "
                                    + "The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p),"
                                    + " the supported precision 'p' is from 0 to 3, but the time field type is %s",
                            timeFieldType));
        }
        if (isProctimeAttribute(timeFieldType)) {
            throw new ValidationException(
                    "A watermark can not be defined for a processing-time attribute.");
        }
        return timeColumn.get();
    }

    private void validateWatermarkExpression(LogicalType watermarkType) {
        if (!canBeTimeAttributeType(watermarkType) || getPrecision(watermarkType) > 3) {
            throw new ValidationException(
                    String.format(
                            "Invalid data type of expression for watermark definition. "
                                    + "The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p),"
                                    + " the supported precision 'p' is from 0 to 3, but the watermark expression type is %s",
                            watermarkType));
        }
    }

    /** Updates the data type of columns that are referenced by {@link WatermarkSpec}. */
    private List<Column> adjustRowtimeAttributes(
            List<WatermarkSpec> watermarkSpecs, List<Column> columns) {
        return columns.stream()
                .map(column -> adjustRowtimeAttribute(watermarkSpecs, column))
                .collect(Collectors.toList());
    }

    private Column adjustRowtimeAttribute(List<WatermarkSpec> watermarkSpecs, Column column) {
        final String name = column.getName();
        final DataType dataType = column.getDataType();
        final boolean hasWatermarkSpec =
                watermarkSpecs.stream().anyMatch(s -> s.getRowtimeAttribute().equals(name));
        if (hasWatermarkSpec && isStreamingMode) {
            switch (dataType.getLogicalType().getTypeRoot()) {
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    final TimestampType originalType = (TimestampType) dataType.getLogicalType();
                    final LogicalType rowtimeType =
                            new TimestampType(
                                    originalType.isNullable(),
                                    TimestampKind.ROWTIME,
                                    originalType.getPrecision());
                    return column.copy(replaceLogicalType(dataType, rowtimeType));
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    final LocalZonedTimestampType timestampLtzType =
                            (LocalZonedTimestampType) dataType.getLogicalType();
                    final LogicalType rowtimeLtzType =
                            new LocalZonedTimestampType(
                                    timestampLtzType.isNullable(),
                                    TimestampKind.ROWTIME,
                                    timestampLtzType.getPrecision());
                    return column.copy(replaceLogicalType(dataType, rowtimeLtzType));
                default:
                    throw new ValidationException(
                            "Invalid data type of expression for rowtime definition. "
                                    + "The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p),"
                                    + " the supported precision 'p' is from 0 to 3.");
            }
        }
        return column;
    }

    private @Nullable UniqueConstraint resolvePrimaryKey(
            @Nullable UnresolvedPrimaryKey unresolvedPrimaryKey, List<Column> columns) {
        if (unresolvedPrimaryKey == null) {
            return null;
        }

        final UniqueConstraint primaryKey =
                UniqueConstraint.primaryKey(
                        unresolvedPrimaryKey.getConstraintName(),
                        unresolvedPrimaryKey.getColumnNames());

        validatePrimaryKey(primaryKey, columns);

        return primaryKey;
    }

    private void validatePrimaryKey(UniqueConstraint primaryKey, List<Column> columns) {
        final Map<String, Column> columnsByNameLookup =
                columns.stream().collect(Collectors.toMap(Column::getName, Function.identity()));

        final Set<String> duplicateColumns =
                primaryKey.getColumns().stream()
                        .filter(name -> Collections.frequency(primaryKey.getColumns(), name) > 1)
                        .collect(Collectors.toSet());

        if (!duplicateColumns.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Invalid primary key '%s'. A primary key must not contain duplicate columns. Found: %s",
                            primaryKey.getName(), duplicateColumns));
        }

        for (String columnName : primaryKey.getColumns()) {
            Column column = columnsByNameLookup.get(columnName);
            if (column == null) {
                throw new ValidationException(
                        String.format(
                                "Invalid primary key '%s'. Column '%s' does not exist.",
                                primaryKey.getName(), columnName));
            }

            if (!column.isPhysical()) {
                throw new ValidationException(
                        String.format(
                                "Invalid primary key '%s'. Column '%s' is not a physical column.",
                                primaryKey.getName(), columnName));
            }

            final LogicalType columnType = column.getDataType().getLogicalType();
            if (columnType.isNullable()) {
                throw new ValidationException(
                        String.format(
                                "Invalid primary key '%s'. Column '%s' is nullable.",
                                primaryKey.getName(), columnName));
            }
        }
    }

    private ResolvedExpression resolveExpression(
            List<Column> columns, Expression expression, @Nullable DataType outputDataType) {
        final LocalReferenceExpression[] localRefs =
                columns.stream()
                        .map(c -> localRef(c.getName(), c.getDataType()))
                        .toArray(LocalReferenceExpression[]::new);
        return resolverBuilder
                .withLocalReferences(localRefs)
                .withOutputDataType(outputDataType)
                .build()
                .resolve(Collections.singletonList(expression))
                .get(0);
    }
}
