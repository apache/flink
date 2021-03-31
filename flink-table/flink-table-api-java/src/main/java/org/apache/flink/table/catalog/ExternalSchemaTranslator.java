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
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.utils.DataTypeUtils.flattenToDataTypes;
import static org.apache.flink.table.types.utils.DataTypeUtils.flattenToNames;

/**
 * Utility to derive a physical {@link DataType}, {@link Schema}, and projections when entering or
 * leaving the table ecosystem from and to other APIs where {@link TypeInformation} is required.
 */
@Internal
public final class ExternalSchemaTranslator {

    /**
     * Converts the given {@link DataType} into the final {@link OutputResult}.
     *
     * <p>Currently, this method serves only the following use case:
     *
     * <ul>
     *   <li>1. Derive physical columns from the input data type.
     * </ul>
     */
    public static OutputResult fromInternal(
            DataTypeFactory dataTypeFactory,
            ResolvedSchema inputSchema,
            AbstractDataType<?> targetDataType) {
        final List<String> inputFieldNames = inputSchema.getColumnNames();
        final DataType resolvedDataType = dataTypeFactory.createDataType(targetDataType);
        final List<String> targetFieldNames = flattenToNames(resolvedDataType);
        final List<DataType> targetFieldDataTypes = flattenToDataTypes(resolvedDataType);

        // help in reorder fields for POJOs if all field names are present but out of order,
        // otherwise let the sink validation fail later
        final List<String> projections;
        if (targetFieldNames.size() == inputFieldNames.size()
                && !targetFieldNames.equals(inputFieldNames)
                && targetFieldNames.containsAll(inputFieldNames)) {
            projections = targetFieldNames;
        } else {
            projections = null;
        }

        final Schema schema =
                Schema.newBuilder().fromFields(targetFieldNames, targetFieldDataTypes).build();

        return new OutputResult(projections, schema, resolvedDataType);
    }

    /**
     * Converts the given {@link TypeInformation} and an optional declared {@link Schema} (possibly
     * incomplete) into the final {@link InputResult}.
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
    public static InputResult fromExternal(
            DataTypeFactory dataTypeFactory,
            TypeInformation<?> inputTypeInfo,
            @Nullable Schema declaredSchema) {
        final DataType inputDataType =
                TypeInfoDataTypeConverter.toDataType(dataTypeFactory, inputTypeInfo);
        final LogicalType inputType = inputDataType.getLogicalType();

        // we don't allow modifying the number of columns during enrichment, therefore we preserve
        // whether the original type was qualified as a top-level record or not
        final boolean isTopLevelRecord = LogicalTypeChecks.isCompositeType(inputType);

        // no schema has been declared by the user,
        // the schema will be entirely derived from the input
        if (declaredSchema == null) {
            final Schema.Builder builder = Schema.newBuilder();
            addPhysicalDataTypeFields(builder, inputDataType);
            return new InputResult(inputDataType, isTopLevelRecord, builder.build(), null);
        }

        final List<UnresolvedColumn> declaredColumns = declaredSchema.getColumns();

        // the declared schema does not contain physical information,
        // thus, it only enriches the non-physical column parts
        if (declaredColumns.stream().noneMatch(ExternalSchemaTranslator::isPhysical)) {
            final Schema.Builder builder = Schema.newBuilder();
            addPhysicalDataTypeFields(builder, inputDataType);
            builder.fromSchema(declaredSchema);
            return new InputResult(inputDataType, isTopLevelRecord, builder.build(), null);
        }

        // the declared schema enriches the physical data type and the derived schema,
        // it possibly projects the result
        final DataType patchedDataType =
                patchDataTypeFromDeclaredSchema(dataTypeFactory, inputDataType, declaredColumns);
        final Schema patchedSchema =
                createPatchedSchema(isTopLevelRecord, patchedDataType, declaredSchema);
        final List<String> projections = extractProjections(patchedSchema, declaredSchema);
        return new InputResult(patchedDataType, isTopLevelRecord, patchedSchema, projections);
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
            addPhysicalDataTypeFields(builder, patchedDataType);
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
                        .filter(ExternalSchemaTranslator::isPhysical)
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
        if (hasRoot(type, LogicalTypeRoot.ROW)) {
            return patchRowDataType(dataType, columnName, columnDataType);
        } else if (hasRoot(type, LogicalTypeRoot.STRUCTURED_TYPE)) {
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

    private static void addPhysicalDataTypeFields(Schema.Builder builder, DataType dataType) {
        final List<DataType> fieldDataTypes = flattenToDataTypes(dataType);
        final List<String> fieldNames = flattenToNames(dataType);
        builder.fromFields(fieldNames, fieldDataTypes);
    }

    private static boolean isPhysical(UnresolvedColumn column) {
        return column instanceof UnresolvedPhysicalColumn;
    }

    // --------------------------------------------------------------------------------------------
    // Result representation
    // --------------------------------------------------------------------------------------------

    /**
     * Result of {@link #fromExternal(DataTypeFactory, TypeInformation, Schema)}.
     *
     * <p>The result should be applied as: physical data type -> schema -> projections.
     */
    public static final class InputResult {

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

        private InputResult(
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
     * Result of {@link #fromExternal(DataTypeFactory, TypeInformation, Schema)}.
     *
     * <p>The result should be applied as: projections -> schema -> physical data type.
     */
    public static final class OutputResult {

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
         */
        private final DataType physicalDataType;

        private OutputResult(
                @Nullable List<String> projections, Schema schema, DataType physicalDataType) {
            this.projections = projections;
            this.schema = schema;
            this.physicalDataType = physicalDataType;
        }

        public @Nullable List<String> getProjections() {
            return projections;
        }

        public Schema getSchema() {
            return schema;
        }

        public DataType getPhysicalDataType() {
            return physicalDataType;
        }
    }
}
