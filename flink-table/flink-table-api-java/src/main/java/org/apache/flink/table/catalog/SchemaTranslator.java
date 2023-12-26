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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedPrimaryKey;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.utils.DataTypeUtils.flattenToDataTypes;
import static org.apache.flink.table.types.utils.DataTypeUtils.flattenToNames;

/**
 * Utility to derive a physical {@link DataType}, {@link Schema}, and projections for sinks and
 * sources.
 */
@Internal
public final class SchemaTranslator {

    /**
     * Converts the given {@link DataType} into the final {@link ProducingResult}.
     *
     * <p>This method serves three types of use cases:
     *
     * <ul>
     *   <li>1. Derive physical columns from the input schema.
     *   <li>2. Derive physical columns from the input schema but enrich with metadata column and
     *       primary key.
     *   <li>3. Entirely use declared schema.
     * </ul>
     */
    public static ProducingResult createProducingResult(
            ResolvedSchema inputSchema, @Nullable Schema declaredSchema) {

        // no schema has been declared by the user,
        // the schema will be entirely derived from the input
        if (declaredSchema == null) {
            // go through data type to erase time attributes
            final DataType physicalDataType = inputSchema.toSourceRowDataType();
            final Schema schema = Schema.newBuilder().fromRowDataType(physicalDataType).build();
            return new ProducingResult(null, schema, null);
        }

        final List<UnresolvedColumn> declaredColumns = declaredSchema.getColumns();

        // the declared schema does not contain physical information,
        // thus, it only replaces physical columns with metadata rowtime or adds a primary key
        if (declaredColumns.stream().noneMatch(SchemaTranslator::isPhysical)) {
            // go through data type to erase time attributes
            final DataType sourceDataType = inputSchema.toSourceRowDataType();
            final DataType physicalDataType =
                    patchDataTypeWithoutMetadataRowtime(sourceDataType, declaredColumns);
            final Schema.Builder builder = Schema.newBuilder();
            builder.fromRowDataType(physicalDataType);
            builder.fromSchema(declaredSchema);
            return new ProducingResult(null, builder.build(), null);
        }

        return new ProducingResult(null, declaredSchema, null);
    }

    /**
     * Converts the given {@link DataType} into the final {@link ProducingResult}.
     *
     * <p>This method serves one type of use case:
     *
     * <ul>
     *   <li>1. Derive physical columns from the input data type.
     * </ul>
     */
    public static ProducingResult createProducingResult(
            DataTypeFactory dataTypeFactory,
            ResolvedSchema inputSchema,
            AbstractDataType<?> targetDataType) {
        final List<String> inputFieldNames = inputSchema.getColumnNames();
        final List<String> inputFieldNamesNormalized =
                inputFieldNames.stream()
                        .map(n -> n.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toList());
        final DataType resolvedDataType = dataTypeFactory.createDataType(targetDataType);
        final List<String> targetFieldNames = flattenToNames(resolvedDataType);
        final List<String> targetFieldNamesNormalized =
                targetFieldNames.stream()
                        .map(n -> n.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toList());
        final List<DataType> targetFieldDataTypes = flattenToDataTypes(resolvedDataType);

        // help in reorder fields for POJOs if all field names are present but out of order,
        // otherwise let the sink validation fail later
        List<String> projections = null;
        if (targetFieldNames.size() == inputFieldNames.size()) {
            // reordering by name (case-sensitive)
            if (targetFieldNames.containsAll(inputFieldNames)) {
                projections = targetFieldNames;
            }
            // reordering by name (case-insensitive) but fields must be unique
            else if (targetFieldNamesNormalized.containsAll(inputFieldNamesNormalized)
                    && targetFieldNamesNormalized.stream().distinct().count()
                            == targetFieldNames.size()
                    && inputFieldNamesNormalized.stream().distinct().count()
                            == inputFieldNames.size()) {
                projections =
                        targetFieldNamesNormalized.stream()
                                .map(
                                        targetName -> {
                                            final int inputFieldPos =
                                                    inputFieldNamesNormalized.indexOf(targetName);
                                            return inputFieldNames.get(inputFieldPos);
                                        })
                                .collect(Collectors.toList());
            }
        }

        final Schema schema =
                Schema.newBuilder().fromFields(targetFieldNames, targetFieldDataTypes).build();

        return new ProducingResult(projections, schema, resolvedDataType);
    }

    /**
     * Converts the given {@link TypeInformation} and an optional declared {@link Schema} (possibly
     * incomplete) into the final {@link ConsumingResult}.
     *
     * <p>This method serves three types of use cases:
     *
     * <ul>
     *   <li>1. Derive physical columns from the input type information.
     *   <li>2. Derive physical columns but merge them with declared computed columns and other
     *       schema information.
     *   <li>3. Derive and enrich physical columns and merge other schema information.
     * </ul>
     */
    public static ConsumingResult createConsumingResult(
            DataTypeFactory dataTypeFactory,
            TypeInformation<?> inputTypeInfo,
            @Nullable Schema declaredSchema) {
        final DataType inputDataType =
                TypeInfoDataTypeConverter.toDataType(dataTypeFactory, inputTypeInfo);
        return createConsumingResult(dataTypeFactory, inputDataType, declaredSchema, true);
    }

    /**
     * Converts the given {@link DataType} and an optional declared {@link Schema} (possibly
     * incomplete) into the final {@link ConsumingResult}.
     *
     * <p>This method serves three types of use cases:
     *
     * <ul>
     *   <li>1. Derive physical columns from the input data type.
     *   <li>2. Derive physical columns but merge them with declared computed columns and other
     *       schema information.
     *   <li>3. Derive and enrich physical columns and merge other schema information (only if
     *       {@param mergePhysicalSchema} is set to {@code true}).
     * </ul>
     */
    public static ConsumingResult createConsumingResult(
            DataTypeFactory dataTypeFactory,
            DataType inputDataType,
            @Nullable Schema declaredSchema,
            boolean mergePhysicalSchema) {
        final LogicalType inputType = inputDataType.getLogicalType();

        // we don't allow modifying the number of columns during enrichment, therefore we preserve
        // whether the original type was qualified as a top-level record or not
        final boolean isTopLevelRecord = LogicalTypeChecks.isCompositeType(inputType);

        // no schema has been declared by the user,
        // the schema will be entirely derived from the input
        if (declaredSchema == null) {
            final Schema.Builder builder = Schema.newBuilder();
            addPhysicalSourceDataTypeFields(builder, inputDataType, null);
            return new ConsumingResult(inputDataType, isTopLevelRecord, builder.build(), null);
        }

        final List<UnresolvedColumn> declaredColumns = declaredSchema.getColumns();
        final UnresolvedPrimaryKey declaredPrimaryKey = declaredSchema.getPrimaryKey().orElse(null);

        // the declared schema does not contain physical information,
        // thus, it only enriches the non-physical column parts
        if (declaredColumns.stream().noneMatch(SchemaTranslator::isPhysical)) {
            final Schema.Builder builder = Schema.newBuilder();
            addPhysicalSourceDataTypeFields(builder, inputDataType, declaredPrimaryKey);
            builder.fromSchema(declaredSchema);
            return new ConsumingResult(inputDataType, isTopLevelRecord, builder.build(), null);
        }

        if (!mergePhysicalSchema) {
            return new ConsumingResult(inputDataType, isTopLevelRecord, declaredSchema, null);
        }

        // the declared schema enriches the physical data type and the derived schema,
        // it possibly projects the result
        final DataType patchedDataType =
                patchDataTypeFromDeclaredSchema(dataTypeFactory, inputDataType, declaredColumns);
        final Schema patchedSchema =
                createPatchedSchema(isTopLevelRecord, patchedDataType, declaredSchema);
        final List<String> projections = extractProjections(patchedSchema, declaredSchema);
        return new ConsumingResult(patchedDataType, isTopLevelRecord, patchedSchema, projections);
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static DataType patchDataTypeWithoutMetadataRowtime(
            DataType dataType, List<UnresolvedColumn> declaredColumns) {
        // best effort logic to remove a rowtime column at the end of a derived schema,
        // this enables 100% symmetrical API calls for from/toChangelogStream,
        // otherwise the full schema must be declared
        final List<DataType> columnDataTypes = dataType.getChildren();
        final int columnCount = columnDataTypes.size();
        final long persistedMetadataCount =
                declaredColumns.stream()
                        .filter(c -> c instanceof UnresolvedMetadataColumn)
                        .map(UnresolvedMetadataColumn.class::cast)
                        .filter(c -> !c.isVirtual())
                        .count();
        // we only truncate if exactly one persisted metadata column exists
        if (persistedMetadataCount != 1L || columnCount < 1) {
            return dataType;
        }
        // we only truncate timestamps
        if (!columnDataTypes
                .get(columnCount - 1)
                .getLogicalType()
                .is(LogicalTypeFamily.TIMESTAMP)) {
            return dataType;
        }
        // truncate last field
        final int[] indices = IntStream.range(0, columnCount - 1).toArray();
        return Projection.of(indices).project(dataType);
    }

    private static @Nullable List<String> extractProjections(
            Schema patchedSchema, Schema declaredSchema) {
        final List<String> patchedColumns =
                patchedSchema.getColumns().stream()
                        .map(UnresolvedColumn::getName)
                        .collect(Collectors.toList());
        final List<String> declaredColumns =
                declaredSchema.getColumns().stream()
                        .map(UnresolvedColumn::getName)
                        .collect(Collectors.toList());
        if (patchedColumns.equals(declaredColumns)) {
            return null;
        }
        return declaredColumns;
    }

    private static Schema createPatchedSchema(
            boolean isTopLevelRecord, DataType patchedDataType, Schema declaredSchema) {
        final Schema.Builder builder = Schema.newBuilder();

        // physical columns
        if (isTopLevelRecord) {
            addPhysicalSourceDataTypeFields(builder, patchedDataType, null);
        } else {
            builder.column(
                    LogicalTypeUtils.getAtomicName(Collections.emptyList()), patchedDataType);
        }

        // remaining schema
        final List<UnresolvedColumn> nonPhysicalColumns =
                declaredSchema.getColumns().stream()
                        .filter(c -> !isPhysical(c))
                        .collect(Collectors.toList());
        builder.fromColumns(nonPhysicalColumns);
        declaredSchema
                .getWatermarkSpecs()
                .forEach(
                        spec ->
                                builder.watermark(
                                        spec.getColumnName(), spec.getWatermarkExpression()));
        declaredSchema
                .getPrimaryKey()
                .ifPresent(
                        key ->
                                builder.primaryKeyNamed(
                                        key.getConstraintName(), key.getColumnNames()));
        return builder.build();
    }

    private static DataType patchDataTypeFromDeclaredSchema(
            DataTypeFactory dataTypeFactory,
            DataType inputDataType,
            List<UnresolvedColumn> declaredColumns) {
        final List<UnresolvedPhysicalColumn> physicalColumns =
                declaredColumns.stream()
                        .filter(SchemaTranslator::isPhysical)
                        .map(UnresolvedPhysicalColumn.class::cast)
                        .collect(Collectors.toList());

        DataType patchedDataType = inputDataType;
        for (UnresolvedPhysicalColumn physicalColumn : physicalColumns) {
            patchedDataType =
                    patchDataTypeFromColumn(dataTypeFactory, patchedDataType, physicalColumn);
        }
        return patchedDataType;
    }

    private static DataType patchDataTypeFromColumn(
            DataTypeFactory dataTypeFactory,
            DataType dataType,
            UnresolvedPhysicalColumn physicalColumn) {
        final List<String> fieldNames = flattenToNames(dataType);
        final String columnName = physicalColumn.getName();
        if (!fieldNames.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            "Unable to find a field named '%s' in the physical data type derived "
                                    + "from the given type information for schema declaration. "
                                    + "Make sure that the type information is not a generic raw "
                                    + "type. Currently available fields are: %s",
                            columnName, fieldNames));
        }
        final DataType columnDataType =
                dataTypeFactory.createDataType(physicalColumn.getDataType());
        final LogicalType type = dataType.getLogicalType();

        // the following lines make assumptions on what comes out of the TypeInfoDataTypeConverter
        // e.g. we can assume that there will be no DISTINCT type and only anonymously defined
        // structured types without a super type
        if (type.is(LogicalTypeRoot.ROW)) {
            return patchRowDataType(dataType, columnName, columnDataType);
        } else if (type.is(LogicalTypeRoot.STRUCTURED_TYPE)) {
            return patchStructuredDataType(dataType, columnName, columnDataType);
        } else {
            // this also covers the case where a top-level generic type enters the
            // Table API, the type can be patched to a more specific type but the schema will still
            // keep it nested in a single field without flattening
            return columnDataType;
        }
    }

    private static DataType patchRowDataType(
            DataType dataType, String patchedFieldName, DataType patchedFieldDataType) {
        final RowType type = (RowType) dataType.getLogicalType();
        final List<String> oldFieldNames = flattenToNames(dataType);
        final List<DataType> oldFieldDataTypes = dataType.getChildren();
        final Class<?> oldConversion = dataType.getConversionClass();

        final DataTypes.Field[] fields =
                patchFields(
                        oldFieldNames, oldFieldDataTypes, patchedFieldName, patchedFieldDataType);

        final DataType newDataType = DataTypes.ROW(fields).bridgedTo(oldConversion);
        if (!type.isNullable()) {
            return newDataType.notNull();
        }
        return newDataType;
    }

    private static DataType patchStructuredDataType(
            DataType dataType, String patchedFieldName, DataType patchedFieldDataType) {
        final StructuredType type = (StructuredType) dataType.getLogicalType();
        final List<String> oldFieldNames = flattenToNames(dataType);
        final List<DataType> oldFieldDataTypes = dataType.getChildren();
        final Class<?> oldConversion = dataType.getConversionClass();

        final DataTypes.Field[] fields =
                patchFields(
                        oldFieldNames, oldFieldDataTypes, patchedFieldName, patchedFieldDataType);

        final DataType newDataType =
                DataTypes.STRUCTURED(
                                type.getImplementationClass()
                                        .orElseThrow(IllegalStateException::new),
                                fields)
                        .bridgedTo(oldConversion);
        if (!type.isNullable()) {
            return newDataType.notNull();
        }
        return newDataType;
    }

    private static DataTypes.Field[] patchFields(
            List<String> oldFieldNames,
            List<DataType> oldFieldDataTypes,
            String patchedFieldName,
            DataType patchedFieldDataType) {
        return IntStream.range(0, oldFieldNames.size())
                .mapToObj(
                        pos -> {
                            final String oldFieldName = oldFieldNames.get(pos);
                            final DataType newFieldDataType;
                            if (oldFieldName.equals(patchedFieldName)) {
                                newFieldDataType = patchedFieldDataType;
                            } else {
                                newFieldDataType = oldFieldDataTypes.get(pos);
                            }
                            return DataTypes.FIELD(oldFieldName, newFieldDataType);
                        })
                .toArray(DataTypes.Field[]::new);
    }

    private static void addPhysicalSourceDataTypeFields(
            Schema.Builder builder, DataType dataType, @Nullable UnresolvedPrimaryKey primaryKey) {
        final List<String> fieldNames = flattenToNames(dataType);
        final List<DataType> fieldDataTypes = flattenToDataTypes(dataType);

        // fields of a Row class are always nullable, which causes problems in the primary key
        // validation, it would force users to always fully declare the schema for row types and
        // would make the automatic schema derivation less useful, we therefore patch the
        // nullability for primary key columns
        final List<DataType> fieldDataTypesWithPatchedNullability =
                IntStream.range(0, fieldNames.size())
                        .mapToObj(
                                pos -> {
                                    final String fieldName = fieldNames.get(pos);
                                    final DataType fieldDataType = fieldDataTypes.get(pos);
                                    if (primaryKey != null
                                            && primaryKey.getColumnNames().contains(fieldName)) {
                                        return fieldDataTypes.get(pos).notNull();
                                    }
                                    return fieldDataType;
                                })
                        .collect(Collectors.toList());

        builder.fromFields(fieldNames, fieldDataTypesWithPatchedNullability);
    }

    private static boolean isPhysical(UnresolvedColumn column) {
        return column instanceof UnresolvedPhysicalColumn;
    }

    // --------------------------------------------------------------------------------------------
    // Result representation
    // --------------------------------------------------------------------------------------------

    /**
     * Result of {@link #createConsumingResult(DataTypeFactory, TypeInformation, Schema)}.
     *
     * <p>The result should be applied as: physical data type -> schema -> projections.
     */
    @Internal
    public static final class ConsumingResult {

        /**
         * Data type expected from the first table ecosystem operator for input conversion. The data
         * type might not be a row type and can possibly be nullable.
         */
        private final DataType physicalDataType;

        /**
         * Whether the first table ecosystem operator should treat the physical record as top-level
         * record and thus perform implicit flattening. Otherwise the record needs to be wrapped in
         * a top-level row.
         */
        private final boolean isTopLevelRecord;

        /**
         * Schema derived from the physical data type. It does not include the projections of the
         * user-provided schema.
         */
        private final Schema schema;

        /**
         * List of indices to adjust the presents and order of columns from {@link #schema} for the
         * final column structure.
         */
        private final @Nullable List<String> projections;

        private ConsumingResult(
                DataType physicalDataType,
                boolean isTopLevelRecord,
                Schema schema,
                @Nullable List<String> projections) {
            this.physicalDataType = physicalDataType;
            this.isTopLevelRecord = isTopLevelRecord;
            this.schema = schema;
            this.projections = projections;
        }

        public DataType getPhysicalDataType() {
            return physicalDataType;
        }

        public boolean isTopLevelRecord() {
            return isTopLevelRecord;
        }

        public Schema getSchema() {
            return schema;
        }

        public @Nullable List<String> getProjections() {
            return projections;
        }
    }

    /**
     * Result of {@link #createConsumingResult(DataTypeFactory, TypeInformation, Schema)}.
     *
     * <p>The result should be applied as: projections -> schema -> physical data type.
     */
    @Internal
    public static final class ProducingResult {

        /**
         * List of field names to adjust the order of columns in {@link #schema} for the final
         * column structure.
         */
        private final @Nullable List<String> projections;

        /** Schema derived from the physical data type. */
        private final Schema schema;

        /**
         * Data type expected from the first external operator after output conversion. The data
         * type might not be a row type and can possibly be nullable.
         *
         * <p>Null if equal to {@link ResolvedSchema#toPhysicalRowDataType()}.
         */
        private final @Nullable DataType physicalDataType;

        private ProducingResult(
                @Nullable List<String> projections,
                Schema schema,
                @Nullable DataType physicalDataType) {
            this.projections = projections;
            this.schema = schema;
            this.physicalDataType = physicalDataType;
        }

        public Optional<List<String>> getProjections() {
            return Optional.ofNullable(projections);
        }

        public Schema getSchema() {
            return schema;
        }

        public Optional<DataType> getPhysicalDataType() {
            return Optional.ofNullable(physicalDataType);
        }
    }
}
