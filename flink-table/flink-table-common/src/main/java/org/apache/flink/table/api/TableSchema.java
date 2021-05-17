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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.TableColumn.ComputedColumn;
import org.apache.flink.table.api.TableColumn.MetadataColumn;
import org.apache.flink.table.api.TableColumn.PhysicalColumn;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.Field;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.canBeTimeAttributeType;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * A table schema that represents a table's structure with field names and data types.
 *
 * @deprecated This class has been deprecated as part of FLIP-164. It has been replaced by two more
 *     dedicated classes {@link Schema} and {@link ResolvedSchema}. Use {@link Schema} for
 *     declaration in APIs. {@link ResolvedSchema} is offered by the framework after resolution and
 *     validation.
 */
@Deprecated
@PublicEvolving
public class TableSchema {

    private static final String ATOMIC_TYPE_FIELD_NAME = "f0";

    private final List<TableColumn> columns;

    private final List<WatermarkSpec> watermarkSpecs;
    private final @Nullable UniqueConstraint primaryKey;

    private TableSchema(
            List<TableColumn> columns,
            List<WatermarkSpec> watermarkSpecs,
            @Nullable UniqueConstraint primaryKey) {
        this.columns = Preconditions.checkNotNull(columns);
        this.watermarkSpecs = Preconditions.checkNotNull(watermarkSpecs);
        this.primaryKey = primaryKey;
    }

    /** @deprecated Use the {@link Builder} instead. */
    @Deprecated
    public TableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        DataType[] fieldDataTypes = fromLegacyInfoToDataType(fieldTypes);
        validateNameTypeNumberEqual(fieldNames, fieldDataTypes);
        List<TableColumn> columns = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            columns.add(TableColumn.physical(fieldNames[i], fieldDataTypes[i]));
        }
        validateColumnsAndWatermarkSpecs(columns, Collections.emptyList());
        this.columns = columns;
        this.watermarkSpecs = Collections.emptyList();
        this.primaryKey = null;
    }

    /** Returns a deep copy of the table schema. */
    public TableSchema copy() {
        return new TableSchema(
                new ArrayList<>(columns), new ArrayList<>(watermarkSpecs), primaryKey);
    }

    /** Returns all field data types as an array. */
    public DataType[] getFieldDataTypes() {
        return columns.stream().map(TableColumn::getType).toArray(DataType[]::new);
    }

    /**
     * @deprecated This method will be removed in future versions as it uses the old type system. It
     *     is recommended to use {@link #getFieldDataTypes()} instead which uses the new type system
     *     based on {@link DataTypes}. Please make sure to use either the old or the new type system
     *     consistently to avoid unintended behavior. See the website documentation for more
     *     information.
     */
    @Deprecated
    public TypeInformation<?>[] getFieldTypes() {
        return fromDataTypeToLegacyInfo(getFieldDataTypes());
    }

    /**
     * Returns the specified data type for the given field index.
     *
     * @param fieldIndex the index of the field
     */
    public Optional<DataType> getFieldDataType(int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= columns.size()) {
            return Optional.empty();
        }
        return Optional.of(columns.get(fieldIndex).getType());
    }

    /**
     * @deprecated This method will be removed in future versions as it uses the old type system. It
     *     is recommended to use {@link #getFieldDataType(int)} instead which uses the new type
     *     system based on {@link DataTypes}. Please make sure to use either the old or the new type
     *     system consistently to avoid unintended behavior. See the website documentation for more
     *     information.
     */
    @Deprecated
    public Optional<TypeInformation<?>> getFieldType(int fieldIndex) {
        return getFieldDataType(fieldIndex).map(TypeConversions::fromDataTypeToLegacyInfo);
    }

    /**
     * Returns the specified data type for the given field name.
     *
     * @param fieldName the name of the field
     */
    public Optional<DataType> getFieldDataType(String fieldName) {
        return this.columns.stream()
                .filter(column -> column.getName().equals(fieldName))
                .findFirst()
                .map(TableColumn::getType);
    }

    /**
     * @deprecated This method will be removed in future versions as it uses the old type system. It
     *     is recommended to use {@link #getFieldDataType(String)} instead which uses the new type
     *     system based on {@link DataTypes}. Please make sure to use either the old or the new type
     *     system consistently to avoid unintended behavior. See the website documentation for more
     *     information.
     */
    @Deprecated
    public Optional<TypeInformation<?>> getFieldType(String fieldName) {
        return getFieldDataType(fieldName).map(TypeConversions::fromDataTypeToLegacyInfo);
    }

    /** Returns the number of fields. */
    public int getFieldCount() {
        return columns.size();
    }

    /** Returns all field names as an array. */
    public String[] getFieldNames() {
        return this.columns.stream().map(TableColumn::getName).toArray(String[]::new);
    }

    /**
     * Returns the specified name for the given field index.
     *
     * @param fieldIndex the index of the field
     */
    public Optional<String> getFieldName(int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= columns.size()) {
            return Optional.empty();
        }
        return Optional.of(this.columns.get(fieldIndex).getName());
    }

    /**
     * Returns the {@link TableColumn} instance for the given field index.
     *
     * @param fieldIndex the index of the field
     */
    public Optional<TableColumn> getTableColumn(int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= columns.size()) {
            return Optional.empty();
        }
        return Optional.of(this.columns.get(fieldIndex));
    }

    /**
     * Returns the {@link TableColumn} instance for the given field name.
     *
     * @param fieldName the name of the field
     */
    public Optional<TableColumn> getTableColumn(String fieldName) {
        return this.columns.stream()
                .filter(column -> column.getName().equals(fieldName))
                .findFirst();
    }

    /** Returns all the {@link TableColumn}s for this table schema. */
    public List<TableColumn> getTableColumns() {
        return new ArrayList<>(this.columns);
    }

    /**
     * Converts all columns of this schema into a (possibly nested) row data type.
     *
     * <p>This method returns the <b>source-to-query schema</b>.
     *
     * <p>Note: The returned row data type contains physical, computed, and metadata columns. Be
     * careful when using this method in a table source or table sink. In many cases, {@link
     * #toPhysicalRowDataType()} might be more appropriate.
     *
     * @see DataTypes#ROW(Field...)
     * @see #toPhysicalRowDataType()
     * @see #toPersistedRowDataType()
     */
    public DataType toRowDataType() {
        final Field[] fields =
                columns.stream()
                        .map(column -> FIELD(column.getName(), column.getType()))
                        .toArray(Field[]::new);
        // The row should be never null.
        return ROW(fields).notNull();
    }

    /**
     * Converts all physical columns of this schema into a (possibly nested) row data type.
     *
     * <p>Note: The returned row data type contains only physical columns. It does not include
     * computed or metadata columns.
     *
     * @see DataTypes#ROW(Field...)
     * @see #toRowDataType()
     * @see #toPersistedRowDataType()
     */
    public DataType toPhysicalRowDataType() {
        final Field[] fields =
                columns.stream()
                        .filter(TableColumn::isPhysical)
                        .map(column -> FIELD(column.getName(), column.getType()))
                        .toArray(Field[]::new);
        // The row should be never null.
        return ROW(fields).notNull();
    }

    /**
     * Converts all persisted columns of this schema into a (possibly nested) row data type.
     *
     * <p>This method returns the <b>query-to-sink schema</b>.
     *
     * <p>Note: Computed columns and virtual columns are excluded in the returned row data type. The
     * data type contains the columns of {@link #toPhysicalRowDataType()} plus persisted metadata
     * columns.
     *
     * @see DataTypes#ROW(Field...)
     * @see #toRowDataType()
     * @see #toPhysicalRowDataType()
     */
    public DataType toPersistedRowDataType() {
        final Field[] fields =
                columns.stream()
                        .filter(TableColumn::isPersisted)
                        .map(column -> FIELD(column.getName(), column.getType()))
                        .toArray(Field[]::new);
        // The row should be never null.
        return ROW(fields).notNull();
    }

    /** @deprecated Use {@link #toRowDataType()} instead. */
    @Deprecated
    @SuppressWarnings("unchecked")
    public TypeInformation<Row> toRowType() {
        return (TypeInformation<Row>) fromDataTypeToLegacyInfo(toRowDataType());
    }

    /**
     * Returns a list of the watermark specification which contains rowtime attribute and watermark
     * strategy expression.
     *
     * <p>NOTE: Currently, there is at most one {@link WatermarkSpec} in the list, because we don't
     * support multiple watermarks definition yet. But in the future, we may support multiple
     * watermarks.
     */
    public List<WatermarkSpec> getWatermarkSpecs() {
        return watermarkSpecs;
    }

    public Optional<UniqueConstraint> getPrimaryKey() {
        return Optional.ofNullable(primaryKey);
    }

    /** Helps to migrate to the new {@link Schema} class. */
    public Schema toSchema() {
        final Schema.Builder builder = Schema.newBuilder();

        columns.forEach(
                column -> {
                    if (column instanceof PhysicalColumn) {
                        final PhysicalColumn c = (PhysicalColumn) column;
                        builder.column(c.getName(), c.getType());
                    } else if (column instanceof MetadataColumn) {
                        final MetadataColumn c = (MetadataColumn) column;
                        builder.columnByMetadata(
                                c.getName(),
                                c.getType(),
                                c.getMetadataAlias().orElse(null),
                                c.isVirtual());
                    } else if (column instanceof ComputedColumn) {
                        final ComputedColumn c = (ComputedColumn) column;
                        builder.columnByExpression(c.getName(), c.getExpression());
                    } else {
                        throw new IllegalArgumentException("Unsupported column type: " + column);
                    }
                });

        watermarkSpecs.forEach(
                spec -> builder.watermark(spec.getRowtimeAttribute(), spec.getWatermarkExpr()));

        if (primaryKey != null) {
            builder.primaryKeyNamed(primaryKey.getName(), primaryKey.getColumns());
        }

        return builder.build();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("root\n");
        for (TableColumn column : columns) {
            sb.append(" |-- ");
            sb.append(column.asSummaryString());
            sb.append('\n');
        }
        if (!watermarkSpecs.isEmpty()) {
            for (WatermarkSpec watermarkSpec : watermarkSpecs) {
                sb.append(" |-- ");
                sb.append(watermarkSpec.asSummaryString());
                sb.append('\n');
            }
        }

        if (primaryKey != null) {
            sb.append(" |-- ").append(primaryKey.asSummaryString());
            sb.append('\n');
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSchema that = (TableSchema) o;
        return Objects.equals(columns, that.columns)
                && Objects.equals(watermarkSpecs, that.watermarkSpecs)
                && Objects.equals(primaryKey, that.primaryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, watermarkSpecs, primaryKey);
    }

    /**
     * Creates a table schema from a {@link TypeInformation} instance. If the type information is a
     * {@link CompositeType}, the field names and types for the composite type are used to construct
     * the {@link TableSchema} instance. Otherwise, a table schema with a single field is created.
     * The field name is "f0" and the field type the provided type.
     *
     * @param typeInfo The {@link TypeInformation} from which the table schema is generated.
     * @return The table schema that was generated from the given {@link TypeInformation}.
     * @deprecated This method will be removed soon. Use {@link DataTypes} to declare types.
     */
    @Deprecated
    public static TableSchema fromTypeInfo(TypeInformation<?> typeInfo) {
        if (typeInfo instanceof CompositeType<?>) {
            final CompositeType<?> compositeType = (CompositeType<?>) typeInfo;
            // get field names and types from composite type
            final String[] fieldNames = compositeType.getFieldNames();
            final TypeInformation<?>[] fieldTypes = new TypeInformation[fieldNames.length];
            for (int i = 0; i < fieldTypes.length; i++) {
                fieldTypes[i] = compositeType.getTypeAt(i);
            }
            return new TableSchema(fieldNames, fieldTypes);
        } else {
            // create table schema with a single field named "f0" of the given type.
            return new TableSchema(
                    new String[] {ATOMIC_TYPE_FIELD_NAME}, new TypeInformation<?>[] {typeInfo});
        }
    }

    /** Helps to migrate to the new {@link ResolvedSchema} to old API methods. */
    public static TableSchema fromResolvedSchema(ResolvedSchema resolvedSchema) {
        final TableSchema.Builder builder = TableSchema.builder();

        resolvedSchema.getColumns().stream()
                .map(
                        column -> {
                            if (column instanceof Column.PhysicalColumn) {
                                final Column.PhysicalColumn c = (Column.PhysicalColumn) column;
                                return TableColumn.physical(c.getName(), c.getDataType());
                            } else if (column instanceof Column.MetadataColumn) {
                                final Column.MetadataColumn c = (Column.MetadataColumn) column;
                                return TableColumn.metadata(
                                        c.getName(),
                                        c.getDataType(),
                                        c.getMetadataKey().orElse(null),
                                        c.isVirtual());
                            } else if (column instanceof Column.ComputedColumn) {
                                final Column.ComputedColumn c = (Column.ComputedColumn) column;
                                return TableColumn.computed(
                                        c.getName(),
                                        c.getDataType(),
                                        c.getExpression().asSerializableString());
                            }
                            throw new IllegalArgumentException(
                                    "Unsupported column type: " + column);
                        })
                .forEach(builder::add);

        resolvedSchema
                .getWatermarkSpecs()
                .forEach(
                        spec ->
                                builder.watermark(
                                        spec.getRowtimeAttribute(),
                                        spec.getWatermarkExpression().asSerializableString(),
                                        spec.getWatermarkExpression().getOutputDataType()));

        resolvedSchema
                .getPrimaryKey()
                .ifPresent(
                        pk ->
                                builder.primaryKey(
                                        pk.getName(), pk.getColumns().toArray(new String[0])));

        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    // ~ Tools ------------------------------------------------------------------

    /**
     * Validate the field names {@code fieldNames} and field types {@code fieldTypes} have equal
     * number.
     *
     * @param fieldNames Field names
     * @param fieldTypes Field data types
     */
    private static void validateNameTypeNumberEqual(String[] fieldNames, DataType[] fieldTypes) {
        if (fieldNames.length != fieldTypes.length) {
            throw new ValidationException(
                    "Number of field names and field data types must be equal.\n"
                            + "Number of names is "
                            + fieldNames.length
                            + ", number of data types is "
                            + fieldTypes.length
                            + ".\n"
                            + "List of field names: "
                            + Arrays.toString(fieldNames)
                            + "\n"
                            + "List of field data types: "
                            + Arrays.toString(fieldTypes));
        }
    }

    /** Table column and watermark specification sanity check. */
    private static void validateColumnsAndWatermarkSpecs(
            List<TableColumn> columns, List<WatermarkSpec> watermarkSpecs) {
        // Validate and create name to type mapping.
        // Field name to data type mapping, we need this because the row time attribute
        // field can be nested.

        // This also check duplicate fields.
        final Map<String, LogicalType> fieldNameToType = new HashMap<>();
        for (TableColumn column : columns) {
            validateAndCreateNameToTypeMapping(
                    fieldNameToType, column.getName(), column.getType().getLogicalType(), "");
        }

        // Validate watermark and rowtime attribute.
        for (WatermarkSpec watermark : watermarkSpecs) {
            String rowtimeAttribute = watermark.getRowtimeAttribute();
            LogicalType rowtimeType =
                    Optional.ofNullable(fieldNameToType.get(rowtimeAttribute))
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    String.format(
                                                            "Rowtime attribute '%s' is not defined in schema.",
                                                            rowtimeAttribute)));
            if (!(rowtimeType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                    || rowtimeType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
                throw new ValidationException(
                        String.format(
                                "Rowtime attribute '%s' must be of type TIMESTAMP or TIMESTAMP_LTZ but is of type '%s'.",
                                rowtimeAttribute, rowtimeType));
            }
            LogicalType watermarkOutputType =
                    watermark.getWatermarkExprOutputType().getLogicalType();
            if (!canBeTimeAttributeType(watermarkOutputType)) {
                throw new ValidationException(
                        String.format(
                                "Watermark strategy %s must be of type TIMESTAMP or TIMESTAMP_LTZ but is of type '%s'.",
                                watermark.getWatermarkExpr(),
                                watermarkOutputType.asSummaryString()));
            }
        }
    }

    private static void validatePrimaryKey(List<TableColumn> columns, UniqueConstraint primaryKey) {
        Map<String, TableColumn> columnsByNameLookup =
                columns.stream()
                        .collect(Collectors.toMap(TableColumn::getName, Function.identity()));

        for (String columnName : primaryKey.getColumns()) {
            TableColumn column = columnsByNameLookup.get(columnName);
            if (column == null) {
                throw new ValidationException(
                        String.format(
                                "Could not create a PRIMARY KEY '%s'. Column '%s' does not exist.",
                                primaryKey.getName(), columnName));
            }

            if (!column.isPhysical()) {
                throw new ValidationException(
                        String.format(
                                "Could not create a PRIMARY KEY '%s'. Column '%s' is not a physical column.",
                                primaryKey.getName(), columnName));
            }

            if (column.getType().getLogicalType().isNullable()) {
                throw new ValidationException(
                        String.format(
                                "Could not create a PRIMARY KEY '%s'. Column '%s' is nullable.",
                                primaryKey.getName(), columnName));
            }
        }
    }

    /**
     * Creates a mapping from field name to data type, the field name can be a nested field. This is
     * mainly used for validating whether the rowtime attribute (might be nested) exists in the
     * schema. During creating, it also validates whether there is duplicate field names.
     *
     * <p>For example, a "f0" field of ROW type has two nested fields "q1" and "q2". Then the
     * mapping will be ["f0" -> ROW, "f0.q1" -> INT, "f0.q2" -> STRING].
     *
     * <pre>{@code
     * f0 ROW<q1 INT, q2 STRING>
     * }</pre>
     *
     * @param fieldNameToType Field name to type mapping that to update
     * @param fieldName Name of this field, e.g. "q1" or "q2" in the above example
     * @param fieldType Data type of this field
     * @param parentFieldName Field name of parent type, e.g. "f0" in the above example
     */
    private static void validateAndCreateNameToTypeMapping(
            Map<String, LogicalType> fieldNameToType,
            String fieldName,
            LogicalType fieldType,
            String parentFieldName) {
        String fullFieldName =
                parentFieldName.isEmpty() ? fieldName : parentFieldName + "." + fieldName;
        LogicalType oldType = fieldNameToType.put(fullFieldName, fieldType);
        if (oldType != null) {
            throw new ValidationException(
                    "Field names must be unique. Duplicate field: '" + fullFieldName + "'");
        }
        if (isCompositeType(fieldType) && !(fieldType instanceof LegacyTypeInformationType)) {
            final List<String> fieldNames = LogicalTypeChecks.getFieldNames(fieldType);
            final List<LogicalType> fieldTypes = fieldType.getChildren();
            IntStream.range(0, fieldNames.size())
                    .forEach(
                            i ->
                                    validateAndCreateNameToTypeMapping(
                                            fieldNameToType,
                                            fieldNames.get(i),
                                            fieldTypes.get(i),
                                            fullFieldName));
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for creating a {@link TableSchema}. */
    public static class Builder {

        private List<TableColumn> columns;

        private final List<WatermarkSpec> watermarkSpecs;

        private UniqueConstraint primaryKey;

        public Builder() {
            columns = new ArrayList<>();
            watermarkSpecs = new ArrayList<>();
        }

        /**
         * Add a field with name and data type.
         *
         * <p>The call order of this method determines the order of fields in the schema.
         */
        public Builder field(String name, DataType dataType) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(dataType);
            columns.add(TableColumn.physical(name, dataType));
            return this;
        }

        /**
         * Add a computed field which is generated by the given expression. This also defines the
         * field name and the data type.
         *
         * <p>The call order of this method determines the order of fields in the schema.
         *
         * <p>The returned expression should be a SQL-style expression whose identifiers should be
         * all quoted and expanded.
         *
         * <p>It should be expanded because this expression may be persisted then deserialized from
         * the catalog, an expanded identifier would avoid the ambiguity if there are same name UDF
         * referenced from different paths. For example, if there is a UDF named "my_udf" from path
         * "my_catalog.my_database", you could pass in an expression like
         * "`my_catalog`.`my_database`.`my_udf`(`f0`) + 1";
         *
         * <p>It should be quoted because user could use a reserved keyword as the identifier, and
         * we have no idea if it is quoted when deserialize from the catalog, so we force to use
         * quoted identifier here. But framework will not check whether it is qualified and quoted
         * or not.
         *
         * @param name Field name
         * @param dataType Field data type
         * @param expression Computed column expression.
         */
        public Builder field(String name, DataType dataType, String expression) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(dataType);
            Preconditions.checkNotNull(expression);
            columns.add(TableColumn.computed(name, dataType, expression));
            return this;
        }

        /**
         * Adds a {@link TableColumn} to this builder.
         *
         * <p>The call order of this method determines the order of fields in the schema.
         */
        public Builder add(TableColumn column) {
            columns.add(column);
            return this;
        }

        /**
         * Add an array of fields with names and data types.
         *
         * <p>The call order of this method determines the order of fields in the schema.
         */
        public Builder fields(String[] names, DataType[] dataTypes) {
            Preconditions.checkNotNull(names);
            Preconditions.checkNotNull(dataTypes);
            validateNameTypeNumberEqual(names, dataTypes);
            List<TableColumn> columns =
                    IntStream.range(0, names.length)
                            .mapToObj(idx -> TableColumn.physical(names[idx], dataTypes[idx]))
                            .collect(Collectors.toList());
            this.columns.addAll(columns);
            return this;
        }

        /**
         * @deprecated This method will be removed in future versions as it uses the old type
         *     system. It is recommended to use {@link #field(String, DataType)} instead which uses
         *     the new type system based on {@link DataTypes}. Please make sure to use either the
         *     old or the new type system consistently to avoid unintended behavior. See the website
         *     documentation for more information.
         */
        @Deprecated
        public Builder field(String name, TypeInformation<?> typeInfo) {
            return field(name, fromLegacyInfoToDataType(typeInfo));
        }

        /**
         * Specifies the previously defined field as an event-time attribute and specifies the
         * watermark strategy.
         *
         * @param rowtimeAttribute the field name as a rowtime attribute, can be a nested field
         *     using dot separator.
         * @param watermarkExpressionString the string representation of watermark generation
         *     expression, e.g. "ts - INTERVAL '5' SECOND". The string is a qualified SQL expression
         *     string (UDFs are expanded) but will not be validated by {@link TableSchema}.
         * @param watermarkExprOutputType the data type of the computation result of watermark
         *     generation expression. Whether the data type equals to the output type of expression
         *     will also not be validated by {@link TableSchema}.
         */
        public Builder watermark(
                String rowtimeAttribute,
                String watermarkExpressionString,
                DataType watermarkExprOutputType) {
            Preconditions.checkNotNull(rowtimeAttribute);
            Preconditions.checkNotNull(watermarkExpressionString);
            Preconditions.checkNotNull(watermarkExprOutputType);
            if (!this.watermarkSpecs.isEmpty()) {
                throw new IllegalStateException(
                        "Multiple watermark definition is not supported yet.");
            }
            this.watermarkSpecs.add(
                    new WatermarkSpec(
                            rowtimeAttribute, watermarkExpressionString, watermarkExprOutputType));
            return this;
        }

        /** Adds the given {@link WatermarkSpec} to this builder. */
        public Builder watermark(WatermarkSpec watermarkSpec) {
            if (!this.watermarkSpecs.isEmpty()) {
                throw new IllegalStateException(
                        "Multiple watermark definition is not supported yet.");
            }
            this.watermarkSpecs.add(watermarkSpec);
            return this;
        }

        /**
         * Creates a primary key constraint for a set of given columns. The primary key is
         * informational only. It will not be enforced. It can be used for optimizations. It is the
         * owner's of the data responsibility to ensure uniqueness of the data.
         *
         * <p>The primary key will be assigned a random name.
         *
         * @param columns array of columns that form a unique primary key
         */
        public Builder primaryKey(String... columns) {
            return primaryKey(UUID.randomUUID().toString(), columns);
        }

        /**
         * Creates a primary key constraint for a set of given columns. The primary key is
         * informational only. It will not be enforced. It can be used for optimizations. It is the
         * owner's of the data responsibility to ensure
         *
         * @param columns array of columns that form a unique primary key
         * @param name name for the primary key, can be used to reference the constraint
         */
        public Builder primaryKey(String name, String[] columns) {
            if (this.primaryKey != null) {
                throw new ValidationException("Can not create multiple PRIMARY keys.");
            }
            if (StringUtils.isNullOrWhitespaceOnly(name)) {
                throw new ValidationException("PRIMARY KEY's name can not be null or empty.");
            }
            if (columns == null || columns.length == 0) {
                throw new ValidationException(
                        "PRIMARY KEY constraint must be defined for at least a single column.");
            }
            this.primaryKey = UniqueConstraint.primaryKey(name, Arrays.asList(columns));
            return this;
        }

        /** Returns a {@link TableSchema} instance. */
        public TableSchema build() {
            validateColumnsAndWatermarkSpecs(this.columns, this.watermarkSpecs);

            if (primaryKey != null) {
                validatePrimaryKey(this.columns, primaryKey);
            }

            return new TableSchema(columns, watermarkSpecs, primaryKey);
        }
    }
}
