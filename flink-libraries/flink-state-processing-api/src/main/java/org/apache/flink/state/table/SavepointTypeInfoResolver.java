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

package org.apache.flink.state.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.utils.TypeUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.state.table.SavepointConnectorOptions.FIELDS;
import static org.apache.flink.state.table.SavepointConnectorOptions.VALUE_CLASS;

/** Resolver for TypeInformation from savepoint metadata and configuration. */
@Internal
class SavepointTypeInfoResolver {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointTypeInfoResolver.class);

    /** Context for type inference to determine what aspect of the type we need. */
    enum InferenceContext {
        /** Inferring the key type of keyed state (always primitive). */
        KEY,
        /** Inferring the key type of a MAP state. */
        MAP_KEY,
        /** Inferring the value type (behavior depends on logical type). */
        VALUE
    }

    private final Map<String, StateMetaInfoSnapshot> preloadedStateMetadata;
    private final SerializerConfig serializerConfig;

    public SavepointTypeInfoResolver(
            Map<String, StateMetaInfoSnapshot> preloadedStateMetadata,
            SerializerConfig serializerConfig) {
        this.preloadedStateMetadata = preloadedStateMetadata;
        this.serializerConfig = serializerConfig;
    }

    /**
     * Resolves TypeInformation for keyed state keys (primitive types only).
     *
     * <p>This is a simplified version of type resolution specifically for key types, which are
     * always primitive and don't require complex metadata inference.
     *
     * @param options Configuration containing table options
     * @param classOption Config option for explicit class specification
     * @param typeInfoFactoryOption Config option for type factory specification
     * @param rowField The row field containing name and LogicalType
     * @return The resolved TypeInformation for the key
     * @throws IllegalArgumentException If both class and factory options are specified
     * @throws RuntimeException If type instantiation fails
     */
    public TypeInformation<?> resolveKeyType(
            Configuration options,
            ConfigOption<String> classOption,
            ConfigOption<String> typeInfoFactoryOption,
            RowType.RowField rowField) {
        try {
            // Priority 1: Explicit configuration (backward compatibility)
            TypeInformation<?> explicitTypeInfo =
                    getExplicitTypeInfo(options, classOption, typeInfoFactoryOption);
            if (explicitTypeInfo != null) {
                return explicitTypeInfo;
            }

            // Priority 2: Simple primitive type inference from LogicalType
            LogicalType logicalType = rowField.getType();
            String columnName = rowField.getName();
            return TypeInformation.of(getPrimitiveClass(logicalType, columnName));
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Resolves TypeSerializer for a table field using a three-tier priority system with direct
     * serializer extraction for metadata inference.
     *
     * <h3>Three-Tier Priority System (Serializer-First)</h3>
     *
     * <ol>
     *   <li><strong>Priority 1: Explicit Configuration</strong> (Highest priority) <br>
     *       Uses user-specified class name or type factory from table options, then converts to
     *       serializer.
     *   <li><strong>Priority 2: Metadata Inference</strong> <br>
     *       Directly extracts serializers from preloaded savepoint metadata (NO TypeInformation
     *       conversion).
     *   <li><strong>Priority 3: LogicalType Fallback</strong> (Lowest priority) <br>
     *       Infers TypeInformation from table schema's LogicalType, then converts to serializer.
     * </ol>
     *
     * <p>This approach eliminates TypeInformation extraction complexity for metadata inference,
     * making it work with ANY serializer type (Avro, custom types, etc.).
     *
     * @param options Configuration containing table options
     * @param classOption Config option for explicit class specification
     * @param typeInfoFactoryOption Config option for type factory specification
     * @param rowField The table field containing name and LogicalType
     * @param inferStateType Whether to enable automatic type inference. If false, returns null when
     *     no explicit configuration is provided.
     * @param context The inference context determining what type aspect to extract.
     * @return The resolved TypeSerializer, or null if inferStateType is false and no explicit
     *     configuration is provided.
     * @throws IllegalArgumentException If both class and factory options are specified
     * @throws RuntimeException If serializer creation fails
     */
    public TypeSerializer<?> resolveSerializer(
            Configuration options,
            ConfigOption<String> classOption,
            ConfigOption<String> typeInfoFactoryOption,
            RowType.RowField rowField,
            boolean inferStateType,
            InferenceContext context) {
        try {
            // Priority 1: Explicit configuration (backward compatibility)
            TypeInformation<?> explicitTypeInfo =
                    getExplicitTypeInfo(options, classOption, typeInfoFactoryOption);
            if (explicitTypeInfo != null) {
                return explicitTypeInfo.createSerializer(serializerConfig);
            }
            if (!inferStateType) {
                return null;
            }

            // Priority 2: Direct serializer extraction from metadata
            Optional<TypeSerializer<?>> metadataSerializer =
                    getSerializerFromMetadata(rowField, context);
            if (metadataSerializer.isPresent()) {
                LOG.info(
                        "Using serializer directly from metadata for state '{}' with context {}: {}",
                        rowField.getName(),
                        context,
                        metadataSerializer.get().getClass().getSimpleName());
                return metadataSerializer.get();
            }

            // Priority 3: Fallback to LogicalType-based inference
            TypeInformation<?> fallbackTypeInfo = inferTypeFromLogicalType(rowField, context);
            return fallbackTypeInfo.createSerializer(serializerConfig);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve serializer for field " + rowField.getName(), e);
        }
    }

    /**
     * Extracts explicit TypeInformation from user configuration (Priority 1).
     *
     * @param options Configuration containing table options
     * @param classOption Config option for explicit class specification
     * @param typeInfoFactoryOption Config option for type factory specification
     * @return The explicit TypeInformation if specified, null otherwise
     * @throws IllegalArgumentException If both class and factory options are specified
     * @throws ReflectiveOperationException If type instantiation fails
     */
    private TypeInformation<?> getExplicitTypeInfo(
            Configuration options,
            ConfigOption<String> classOption,
            ConfigOption<String> typeInfoFactoryOption)
            throws ReflectiveOperationException {

        Optional<String> clazz = options.getOptional(classOption);
        Optional<String> typeInfoFactory = options.getOptional(typeInfoFactoryOption);

        if (clazz.isPresent() && typeInfoFactory.isPresent()) {
            throw new IllegalArgumentException(
                    "Either "
                            + classOption.key()
                            + " or "
                            + typeInfoFactoryOption.key()
                            + " can be specified, not both.");
        }

        if (clazz.isPresent()) {
            return TypeInformation.of(Class.forName(clazz.get()));
        } else if (typeInfoFactory.isPresent()) {
            SavepointTypeInformationFactory savepointTypeInformationFactory =
                    (SavepointTypeInformationFactory)
                            TypeUtils.getInstance(typeInfoFactory.get(), new Object[0]);
            return savepointTypeInformationFactory.getTypeInformation();
        }

        return null;
    }

    /**
     * Directly extracts TypeSerializer from preloaded metadata (Priority 2).
     *
     * <p>This method performs NO I/O and NO TypeInformation conversion. It directly extracts the
     * serializer that was used to write the state data.
     *
     * @param rowField The row field to extract serializer for
     * @param context The inference context determining what serializer to extract
     * @return The serializer if found in metadata, empty otherwise
     */
    private Optional<TypeSerializer<?>> getSerializerFromMetadata(
            RowType.RowField rowField, InferenceContext context) {
        try {
            // Get state name for this field (defaults to field name)
            String stateName = rowField.getName();

            // Look up from preloaded metadata (NO I/O)
            StateMetaInfoSnapshot stateMetaInfo = preloadedStateMetadata.get(stateName);

            if (stateMetaInfo == null) {
                LOG.debug("State '{}' not found in preloaded metadata", stateName);
                return Optional.empty();
            }

            // Extract appropriate serializer based on context
            TypeSerializerSnapshot<?> serializerSnapshot = null;
            switch (context) {
                case KEY:
                    serializerSnapshot =
                            stateMetaInfo.getTypeSerializerSnapshot(
                                    StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER);
                    break;
                case MAP_KEY:
                    // For MAP_KEY, we need the key serializer from the value serializer
                    // (which is MapSerializer)
                    TypeSerializerSnapshot<?> valueSnapshot =
                            stateMetaInfo.getTypeSerializerSnapshot(
                                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
                    if (valueSnapshot != null) {
                        TypeSerializer<?> valueSerializer = valueSnapshot.restoreSerializer();
                        if (valueSerializer instanceof MapSerializer) {
                            serializerSnapshot =
                                    ((MapSerializer<?, ?>) valueSerializer)
                                            .getKeySerializer()
                                            .snapshotConfiguration();
                        }
                    }
                    break;
                case VALUE:
                    serializerSnapshot =
                            stateMetaInfo.getTypeSerializerSnapshot(
                                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
                    break;
            }

            if (serializerSnapshot == null) {
                LOG.debug(
                        "No serializer snapshot found for state '{}' with context {}",
                        stateName,
                        context);
                return Optional.empty();
            }

            // Restore serializer from snapshot
            TypeSerializer<?> serializer = serializerSnapshot.restoreSerializer();

            // For VALUE context with complex types, extract the appropriate sub-serializer
            if (context == InferenceContext.VALUE) {
                return extractValueSerializerForLogicalType(serializer, rowField.getType());
            }

            return Optional.of(serializer);

        } catch (Exception e) {
            LOG.warn(
                    "Failed to extract serializer from metadata for field '{}': {}",
                    rowField.getName(),
                    e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Extracts the appropriate value serializer based on LogicalType for VALUE context.
     *
     * @param fullSerializer The complete serializer from metadata
     * @param logicalType The LogicalType from the table schema
     * @return The appropriate value serializer
     */
    private Optional<TypeSerializer<?>> extractValueSerializerForLogicalType(
            TypeSerializer<?> fullSerializer, LogicalType logicalType) {

        switch (logicalType.getTypeRoot()) {
            case ARRAY:
                // ARRAY logical type → LIST state → extract element serializer
                if (fullSerializer
                        instanceof org.apache.flink.api.common.typeutils.base.ListSerializer) {
                    org.apache.flink.api.common.typeutils.base.ListSerializer<?> listSerializer =
                            (org.apache.flink.api.common.typeutils.base.ListSerializer<?>)
                                    fullSerializer;
                    return Optional.of(listSerializer.getElementSerializer());
                }
                LOG.debug(
                        "Expected ListSerializer for ARRAY logical type but got: {}",
                        fullSerializer.getClass());
                return Optional.empty();

            case MAP:
                // MAP logical type → MAP state → extract value serializer
                if (fullSerializer instanceof MapSerializer) {
                    return Optional.of(((MapSerializer<?, ?>) fullSerializer).getValueSerializer());
                }
                LOG.debug(
                        "Expected MapSerializer for MAP logical type but got: {}",
                        fullSerializer.getClass());
                return Optional.empty();

            default:
                // Primitive logical type → VALUE state → use serializer as-is
                return Optional.of(fullSerializer);
        }
    }

    /**
     * Fallback inference using LogicalType when metadata extraction fails.
     *
     * @param rowField The row field to infer type for
     * @param context The inference context
     * @return The inferred TypeInformation
     */
    private TypeInformation<?> inferTypeFromLogicalType(
            RowType.RowField rowField, InferenceContext context) {

        LogicalType logicalType = rowField.getType();
        String columnName = rowField.getName();

        try {
            switch (context) {
                case KEY:
                    // Keys are always primitive
                    return TypeInformation.of(getPrimitiveClass(logicalType, columnName));

                case MAP_KEY:
                    // Extract key type from MAP logical type
                    if (logicalType instanceof MapType) {
                        LogicalType keyType = ((MapType) logicalType).getKeyType();
                        return TypeInformation.of(getPrimitiveClass(keyType, columnName));
                    }
                    throw new UnsupportedOperationException(
                            "MAP_KEY context requires MAP logical type, but got: " + logicalType);

                case VALUE:
                    return inferValueTypeFromLogicalType(logicalType, columnName);

                default:
                    throw new UnsupportedOperationException("Unknown context: " + context);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to infer type for context " + context, e);
        }
    }

    /**
     * Infers value type from LogicalType for VALUE context fallback.
     *
     * @param logicalType The LogicalType
     * @param columnName The column name for error messages
     * @return The inferred TypeInformation
     */
    private TypeInformation<?> inferValueTypeFromLogicalType(
            LogicalType logicalType, String columnName) throws ClassNotFoundException {

        switch (logicalType.getTypeRoot()) {
            case ARRAY:
                // ARRAY logical type → LIST state → return element type
                ArrayType arrayType = (ArrayType) logicalType;
                return TypeInformation.of(
                        getPrimitiveClass(arrayType.getElementType(), columnName));

            case MAP:
                // MAP logical type → MAP state → return value type
                MapType mapType = (MapType) logicalType;
                return TypeInformation.of(getPrimitiveClass(mapType.getValueType(), columnName));

            default:
                // Primitive logical type → VALUE state → return primitive type
                return TypeInformation.of(getPrimitiveClass(logicalType, columnName));
        }
    }

    /**
     * Maps LogicalType to primitive Java class.
     *
     * @param logicalType The LogicalType to map
     * @param columnName The column name for error messages
     * @return The corresponding Java class
     */
    private Class<?> getPrimitiveClass(LogicalType logicalType, String columnName)
            throws ClassNotFoundException {
        String className = inferTypeInfoClassFromLogicalType(columnName, logicalType);
        return Class.forName(className);
    }

    private String inferTypeInfoClassFromLogicalType(String columnName, LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return String.class.getName();

            case BOOLEAN:
                return Boolean.class.getName();

            case BINARY:
            case VARBINARY:
                return byte[].class.getName();

            case DECIMAL:
                return BigDecimal.class.getName();

            case TINYINT:
                return Byte.class.getName();

            case SMALLINT:
                return Short.class.getName();

            case INTEGER:
                return Integer.class.getName();

            case BIGINT:
                return Long.class.getName();

            case FLOAT:
                return Float.class.getName();

            case DOUBLE:
                return Double.class.getName();

            case DATE:
                return Integer.class.getName();

            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return Long.class.getName();

            case ARRAY:
                return inferTypeInfoClassFromLogicalType(
                        columnName, ((ArrayType) logicalType).getElementType());

            case MAP:
                return inferTypeInfoClassFromLogicalType(
                        columnName, ((MapType) logicalType).getValueType());

            case NULL:
                return null;

            case ROW:
            case MULTISET:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            case DESCRIPTOR:
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unable to infer state format for SQL type: %s in column: %s. "
                                        + "Please override the type with the following config parameter: %s.%s.%s",
                                logicalType, columnName, FIELDS, columnName, VALUE_CLASS));
        }
    }
}
