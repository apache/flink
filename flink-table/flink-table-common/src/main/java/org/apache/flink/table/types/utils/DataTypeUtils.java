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

package org.apache.flink.table.types.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.extraction.DataTypeExtractor;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.primitiveToWrapper;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DISTINCT_TYPE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.STRUCTURED_TYPE;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldNames;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;
import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.getAtomicName;
import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.removeTimeAttributes;
import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass;

/** Utilities for handling {@link DataType}s. */
@Internal
public final class DataTypeUtils {

    // --------------------------------------------------------------------------------------------
    // Time attribute utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Removes time attributes from the {@link DataType}. As everywhere else in the code base, this
     * method does not support nested time attributes for now.
     */
    public static DataType removeTimeAttribute(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type.is(LogicalTypeFamily.TIMESTAMP)) {
            return replaceLogicalType(dataType, removeTimeAttributes(type));
        }
        return dataType;
    }

    /** Returns a PROCTIME data type. */
    public static DataType createProctimeDataType() {
        return new AtomicDataType(new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3));
    }

    /**
     * {@link ResolvedSchema#toPhysicalRowDataType()} erases time attributes. This method keeps them
     * during conversion for very specific use cases mostly in Table API.
     */
    public static DataType fromResolvedSchemaPreservingTimeAttributes(
            ResolvedSchema resolvedSchema) {
        final List<String> fieldNames = resolvedSchema.getColumnNames();
        final List<DataType> fieldTypes = resolvedSchema.getColumnDataTypes();
        return DataTypes.ROW(
                        IntStream.range(0, fieldNames.size())
                                .mapToObj(
                                        pos ->
                                                DataTypes.FIELD(
                                                        fieldNames.get(pos), fieldTypes.get(pos)))
                                .collect(Collectors.toList()))
                .notNull();
    }

    // --------------------------------------------------------------------------------------------
    // Composite data type utilities
    // --------------------------------------------------------------------------------------------

    /** Appends the given list of fields to an existing row data type. */
    public static DataType appendRowFields(DataType dataType, List<Field> fields) {
        Preconditions.checkArgument(dataType.getLogicalType().is(ROW), "Row data type expected.");
        if (fields.isEmpty()) {
            return dataType;
        }
        DataType newRow =
                Stream.concat(DataType.getFields(dataType).stream(), fields.stream())
                        .collect(Collectors.collectingAndThen(Collectors.toList(), DataTypes::ROW));
        if (!dataType.getLogicalType().isNullable()) {
            newRow = newRow.notNull();
        }
        return newRow;
    }

    /**
     * Expands a composite {@link DataType} to a corresponding {@link ResolvedSchema}. Useful for
     * flattening a column or mapping a physical to logical type of a table source
     *
     * <p>Throws an exception for a non composite type. You can use {@link
     * LogicalTypeChecks#isCompositeType(LogicalType)} to check that.
     *
     * <p>It does not expand an atomic type on purpose, because that operation depends on the
     * context. E.g. in case of a {@code FLATTEN} function such operation is not allowed, whereas
     * when mapping a physical type to logical the field name should be derived from the logical
     * schema.
     *
     * @param dataType Data type to expand. Must be a composite type.
     * @return A corresponding table schema.
     */
    public static ResolvedSchema expandCompositeTypeToSchema(DataType dataType) {
        if (dataType instanceof FieldsDataType) {
            return expandCompositeType((FieldsDataType) dataType);
        } else if (dataType.getLogicalType() instanceof LegacyTypeInformationType
                && dataType.getLogicalType().getTypeRoot() == STRUCTURED_TYPE) {
            return expandLegacyCompositeType(dataType);
        }

        throw new IllegalArgumentException("Expected a composite type");
    }

    private static ResolvedSchema expandCompositeType(FieldsDataType dataType) {
        DataType[] fieldDataTypes = dataType.getChildren().toArray(new DataType[0]);
        return dataType.getLogicalType()
                .accept(
                        new LogicalTypeDefaultVisitor<>() {
                            @Override
                            public ResolvedSchema visit(RowType rowType) {
                                return expandCompositeType(rowType, fieldDataTypes);
                            }

                            @Override
                            public ResolvedSchema visit(StructuredType structuredType) {
                                return expandCompositeType(structuredType, fieldDataTypes);
                            }

                            @Override
                            public ResolvedSchema visit(DistinctType distinctType) {
                                return distinctType.getSourceType().accept(this);
                            }

                            @Override
                            protected ResolvedSchema defaultMethod(LogicalType logicalType) {
                                throw new IllegalArgumentException("Expected a composite type");
                            }
                        });
    }

    private static ResolvedSchema expandLegacyCompositeType(DataType dataType) {
        // legacy composite type
        CompositeType<?> compositeType =
                (CompositeType<?>)
                        ((LegacyTypeInformationType<?>) dataType.getLogicalType())
                                .getTypeInformation();

        String[] fieldNames = compositeType.getFieldNames();
        DataType[] fieldTypes =
                Arrays.stream(fieldNames)
                        .map(compositeType::getTypeAt)
                        .map(TypeConversions::fromLegacyInfoToDataType)
                        .toArray(DataType[]::new);

        return ResolvedSchema.physical(fieldNames, fieldTypes);
    }

    private static ResolvedSchema expandCompositeType(
            LogicalType compositeType, DataType[] fieldDataTypes) {
        final String[] fieldNames = getFieldNames(compositeType).toArray(new String[0]);
        return ResolvedSchema.physical(fieldNames, fieldDataTypes);
    }

    /**
     * Retrieves a nested field from a composite type at given position.
     *
     * <p>Throws an exception for a non composite type. You can use {@link
     * LogicalTypeChecks#isCompositeType(LogicalType)} to check that.
     *
     * @param compositeType Data type to expand. Must be a composite type.
     * @param index Index of the field to retrieve.
     * @return The field at the given position.
     */
    public static Optional<DataType> getField(DataType compositeType, int index) {
        ResolvedSchema tableSchema = expandCompositeTypeToSchema(compositeType);
        return tableSchema.getColumn(index).map(Column::getDataType);
    }

    /**
     * Retrieves a nested field from a composite type with given name.
     *
     * <p>Throws an exception for a non composite type. You can use {@link
     * LogicalTypeChecks#isCompositeType(LogicalType)} to check that.
     *
     * @param compositeType Data type to expand. Must be a composite type.
     * @param name Name of the field to retrieve.
     * @return The field with the given name.
     */
    public static Optional<DataType> getField(DataType compositeType, String name) {
        final ResolvedSchema resolvedSchema = expandCompositeTypeToSchema(compositeType);
        return resolvedSchema.getColumn(name).map(Column::getDataType);
    }

    /**
     * Returns the data types of the flat representation in the first level of the given data type.
     */
    public static List<DataType> flattenToDataTypes(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type.is(DISTINCT_TYPE)) {
            return flattenToDataTypes(dataType.getChildren().get(0));
        } else if (isCompositeType(type)) {
            return dataType.getChildren();
        }
        return Collections.singletonList(dataType);
    }

    /**
     * Returns the names of the flat representation of the given data type. In case of {@link
     * StructuredType}, the list also includes the super type fields.
     */
    public static List<String> flattenToNames(DataType dataType) {
        return flattenToNames(dataType, Collections.emptyList());
    }

    /**
     * @see DataTypeUtils#flattenToNames(DataType)
     */
    public static List<String> flattenToNames(DataType dataType, List<String> existingNames) {
        final LogicalType type = dataType.getLogicalType();
        if (type.is(DISTINCT_TYPE)) {
            return flattenToNames(dataType.getChildren().get(0), existingNames);
        } else if (isCompositeType(type)) {
            return getFieldNames(type);
        } else {
            return Collections.singletonList(getAtomicName(existingNames));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Logical type utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Replaces the {@link LogicalType} of a {@link DataType}, i.e., it keeps the bridging class.
     */
    public static DataType replaceLogicalType(DataType dataType, LogicalType replacement) {
        return LogicalTypeDataTypeConverter.toDataType(replacement)
                .bridgedTo(dataType.getConversionClass());
    }

    // --------------------------------------------------------------------------------------------
    // Conversion class utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a {@link DataType} from the given {@link LogicalType} with internal data structures.
     */
    public static DataType toInternalDataType(LogicalType logicalType) {
        final DataType defaultDataType = TypeConversions.fromLogicalToDataType(logicalType);
        return toInternalDataType(defaultDataType);
    }

    /** Creates a {@link DataType} from the given {@link DataType} with internal data structures. */
    public static DataType toInternalDataType(DataType dataType) {
        return dataType.bridgedTo(toInternalConversionClass(dataType.getLogicalType()));
    }

    /** Checks whether a given data type is an internal data structure. */
    public static boolean isInternal(DataType dataType) {
        return isInternal(dataType, true);
    }

    /** Checks whether a given data type is an internal data structure. */
    public static boolean isInternal(DataType dataType, boolean autobox) {
        final Class<?> clazz;
        if (autobox) {
            clazz = primitiveToWrapper(dataType.getConversionClass());
        } else {
            clazz = dataType.getConversionClass();
        }

        return clazz == toInternalConversionClass(dataType.getLogicalType());
    }

    /**
     * Checks whether this data type and its children use the {@link
     * LogicalType#getDefaultConversion()} defined by the logical type.
     */
    public static boolean isDefaultClassNested(DataType dataType) {
        return isDefaultClass(dataType)
                && dataType.getChildren().stream().allMatch(DataTypeUtils::isDefaultClassNested);
    }

    /**
     * Checks whether this data type uses the {@link LogicalType#getDefaultConversion()} defined by
     * the logical type.
     */
    public static boolean isDefaultClass(DataType dataType) {
        return Objects.equals(
                dataType.getConversionClass(), dataType.getLogicalType().getDefaultConversion());
    }

    // --------------------------------------------------------------------------------------------
    // Data type validation
    // --------------------------------------------------------------------------------------------

    /**
     * The {@link DataType} class can only partially verify the conversion class. This method can
     * perform the final check when we know if the data type should be used for input.
     */
    public static void validateInputDataType(DataType dataType) {
        dataType.accept(DataTypeInputClassValidator.INSTANCE);
    }

    /**
     * The {@link DataType} class can only partially verify the conversion class. This method can
     * perform the final check when we know if the data type should be used for output.
     */
    public static void validateOutputDataType(DataType dataType) {
        dataType.accept(DataTypeOutputClassValidator.INSTANCE);
    }

    private static class DataTypeInputClassValidator extends DataTypeDefaultVisitor<Void> {

        private static final DataTypeInputClassValidator INSTANCE =
                new DataTypeInputClassValidator();

        @Override
        protected Void defaultMethod(DataType dataType) {
            if (!dataType.getLogicalType().supportsInputConversion(dataType.getConversionClass())) {
                throw new ValidationException(
                        String.format(
                                "Data type '%s' does not support an input conversion from class '%s'.",
                                dataType, dataType.getConversionClass().getName()));
            }
            dataType.getChildren().forEach(child -> child.accept(this));
            return null;
        }
    }

    private static class DataTypeOutputClassValidator extends DataTypeDefaultVisitor<Void> {

        private static final DataTypeOutputClassValidator INSTANCE =
                new DataTypeOutputClassValidator();

        @Override
        protected Void defaultMethod(DataType dataType) {
            if (!dataType.getLogicalType()
                    .supportsOutputConversion(dataType.getConversionClass())) {
                throw new ValidationException(
                        String.format(
                                "Data type '%s' does not support an output conversion to class '%s'.",
                                dataType, dataType.getConversionClass().getName()));
            }
            dataType.getChildren().forEach(child -> child.accept(this));
            return null;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Data type transformations
    // --------------------------------------------------------------------------------------------

    /**
     * Transforms the given data type to a different data type using the given transformations.
     *
     * @see #transform(DataTypeFactory, DataType, TypeTransformation...)
     */
    public static DataType transform(
            DataType typeToTransform, TypeTransformation... transformations) {
        return transform(null, typeToTransform, transformations);
    }

    /**
     * Transforms the given data type to a different data type using the given transformations.
     *
     * <p>The transformations will be called in the given order. In case of constructed or composite
     * types, a transformation will be applied transitively to children first.
     *
     * <p>Both the {@link DataType#getLogicalType()} and {@link DataType#getConversionClass()} can
     * be transformed.
     *
     * @param factory {@link DataTypeFactory} if available
     * @param typeToTransform data type to be transformed.
     * @param transformations the transformations to transform data type to another type.
     * @return the new data type
     */
    public static DataType transform(
            @Nullable DataTypeFactory factory,
            DataType typeToTransform,
            TypeTransformation... transformations) {
        Preconditions.checkArgument(
                transformations.length > 0, "transformations should not be empty.");
        DataType newType = typeToTransform;
        for (TypeTransformation transformation : transformations) {
            newType = newType.accept(new DataTypeTransformer(factory, transformation));
        }
        return newType;
    }

    /**
     * Transforms a {@link DataType}.
     *
     * <p>In case of constructed or composite types, a transformation will be applied transitively
     * to children first.
     */
    private static class DataTypeTransformer implements DataTypeVisitor<DataType> {

        private final @Nullable DataTypeFactory factory;

        private final TypeTransformation transformation;

        private DataTypeTransformer(
                @Nullable DataTypeFactory factory, TypeTransformation transformation) {
            this.factory = factory;
            this.transformation = transformation;
        }

        @Override
        public DataType visit(AtomicDataType atomicDataType) {
            return transformation.transform(factory, atomicDataType);
        }

        @Override
        public DataType visit(CollectionDataType collectionDataType) {
            final DataType newElementType = collectionDataType.getElementDataType().accept(this);
            final LogicalType logicalType = collectionDataType.getLogicalType();
            final LogicalType newLogicalType;
            if (logicalType instanceof ArrayType) {
                newLogicalType =
                        new ArrayType(logicalType.isNullable(), newElementType.getLogicalType());
            } else if (logicalType instanceof MultisetType) {
                newLogicalType =
                        new MultisetType(logicalType.isNullable(), newElementType.getLogicalType());
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported logical type : " + logicalType);
            }
            return transformation.transform(
                    factory,
                    new CollectionDataType(
                            newLogicalType,
                            collectionDataType.getConversionClass(),
                            newElementType));
        }

        @Override
        public DataType visit(FieldsDataType fieldsDataType) {
            final List<DataType> newDataTypes =
                    fieldsDataType.getChildren().stream()
                            .map(dt -> dt.accept(this))
                            .collect(Collectors.toList());

            final LogicalType logicalType = fieldsDataType.getLogicalType();
            final LogicalType newLogicalType;
            if (logicalType instanceof RowType) {
                final List<RowField> oldFields = ((RowType) logicalType).getFields();
                final List<RowField> newFields =
                        IntStream.range(0, oldFields.size())
                                .mapToObj(
                                        i ->
                                                new RowField(
                                                        oldFields.get(i).getName(),
                                                        newDataTypes.get(i).getLogicalType(),
                                                        oldFields
                                                                .get(i)
                                                                .getDescription()
                                                                .orElse(null)))
                                .collect(Collectors.toList());

                newLogicalType = new RowType(logicalType.isNullable(), newFields);
            } else if (logicalType instanceof StructuredType) {
                final StructuredType structuredType = (StructuredType) logicalType;
                if (structuredType.getSuperType().isPresent()) {
                    throw new UnsupportedOperationException(
                            "Hierarchies of structured types are not supported yet.");
                }
                final List<StructuredAttribute> oldAttributes = structuredType.getAttributes();
                final List<StructuredAttribute> newAttributes =
                        IntStream.range(0, oldAttributes.size())
                                .mapToObj(
                                        i ->
                                                new StructuredAttribute(
                                                        oldAttributes.get(i).getName(),
                                                        newDataTypes.get(i).getLogicalType(),
                                                        oldAttributes
                                                                .get(i)
                                                                .getDescription()
                                                                .orElse(null)))
                                .collect(Collectors.toList());

                final StructuredType.Builder builder = createStructuredBuilder(structuredType);
                builder.attributes(newAttributes);
                builder.setNullable(structuredType.isNullable());
                builder.setFinal(structuredType.isFinal());
                builder.setInstantiable(structuredType.isInstantiable());
                builder.comparison(structuredType.getComparison());
                structuredType.getDescription().ifPresent(builder::description);

                newLogicalType = builder.build();
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported logical type : " + logicalType);
            }
            return transformation.transform(
                    factory,
                    new FieldsDataType(
                            newLogicalType, fieldsDataType.getConversionClass(), newDataTypes));
        }

        @Override
        public DataType visit(KeyValueDataType keyValueDataType) {
            final DataType newKeyType = keyValueDataType.getKeyDataType().accept(this);
            final DataType newValueType = keyValueDataType.getValueDataType().accept(this);
            final LogicalType logicalType = keyValueDataType.getLogicalType();
            final LogicalType newLogicalType;
            if (logicalType instanceof MapType) {
                newLogicalType =
                        new MapType(
                                logicalType.isNullable(),
                                newKeyType.getLogicalType(),
                                newValueType.getLogicalType());
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported logical type : " + logicalType);
            }
            return transformation.transform(
                    factory,
                    new KeyValueDataType(
                            newLogicalType,
                            keyValueDataType.getConversionClass(),
                            newKeyType,
                            newValueType));
        }

        // ----------------------------------------------------------------------------------------

        private StructuredType.Builder createStructuredBuilder(StructuredType structuredType) {
            final ObjectIdentifier identifier = structuredType.getObjectIdentifier().orElse(null);
            final String className = structuredType.getClassName().orElse(null);
            final Class<?> implementationClass =
                    structuredType.getImplementationClass().orElse(null);

            if (identifier != null && implementationClass != null) {
                return StructuredType.newBuilder(identifier, implementationClass);
            } else if (identifier != null) {
                return StructuredType.newBuilder(identifier);
            } else if (implementationClass != null) {
                return StructuredType.newBuilder(implementationClass);
            } else {
                return StructuredType.newBuilder(className);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Structured type alignment
    // --------------------------------------------------------------------------------------------

    /**
     * Aligns the {@link DataType} and its nested conversion classes with the given {@link
     * StructuredType#getImplementationClass()}.
     *
     * <p>By default, a data type is created from a {@link LogicalType} and uses default conversion
     * classes. But for conversion to the implementation class, the data type must reflect the
     * correct expected classes (e.g. {@code List<Integer>} instead of {@code Integer[]}) for all
     * attributes.
     */
    public static DataType alignStructuredTypes(DataTypeFactory factory, DataType dataType) {
        return dataType.accept(new StructuredTypeAligner(factory));
    }

    private static class StructuredTypeAligner implements DataTypeVisitor<DataType> {

        private final DataTypeFactory factory;

        public StructuredTypeAligner(DataTypeFactory factory) {
            this.factory = factory;
        }

        @Override
        public DataType visit(AtomicDataType atomicDataType) {
            return atomicDataType;
        }

        @Override
        public DataType visit(CollectionDataType collectionDataType) {
            return new CollectionDataType(
                    collectionDataType.getLogicalType(),
                    collectionDataType.getConversionClass(),
                    collectionDataType.getElementDataType().accept(this));
        }

        @Override
        public DataType visit(FieldsDataType fieldsDataType) {
            final LogicalType logicalType = fieldsDataType.getLogicalType();
            if (logicalType.is(LogicalTypeRoot.STRUCTURED_TYPE)
                    && logicalType.getDefaultConversion() != StructuredType.FALLBACK_CONVERSION) {
                return alignDataType(fieldsDataType, (StructuredType) logicalType);
            }
            return new FieldsDataType(
                    fieldsDataType.getLogicalType(),
                    fieldsDataType.getConversionClass(),
                    fieldsDataType.getChildren().stream()
                            .map(f -> f.accept(this))
                            .collect(Collectors.toList()));
        }

        @Override
        public DataType visit(KeyValueDataType keyValueDataType) {
            return new KeyValueDataType(
                    keyValueDataType.getLogicalType(),
                    keyValueDataType.getConversionClass(),
                    keyValueDataType.getKeyDataType().accept(this),
                    keyValueDataType.getValueDataType().accept(this));
        }

        private DataType alignDataType(DataType defaultDataType, StructuredType structuredType) {
            final Class<?> implementationClass =
                    structuredType.getImplementationClass().orElseThrow(IllegalStateException::new);
            try {
                return DataTypeExtractor.extractFromStructuredClass(factory, implementationClass);
            } catch (ValidationException e) {
                // Necessary for legacy code paths and tests written in Scala
                return defaultDataType;
            }
        }
    }

    private DataTypeUtils() {
        // no instantiation
    }
}
