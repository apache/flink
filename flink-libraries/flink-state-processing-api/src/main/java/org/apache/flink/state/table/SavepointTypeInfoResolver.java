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
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

/** Resolver for TypeInformation from savepoint metadata and configuration. */
@Internal
class SavepointTypeInfoResolver {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointTypeInfoResolver.class);

    /** Determines which serializer of a state's metadata entry is being resolved. */
    enum InferenceContext {
        /** The key type of keyed state, or a {@code BroadcastState}'s (unwrapped) map key. */
        KEY,
        /** The key type of a keyed MAP state, wrapped inside its {@code MapSerializer}. */
        MAP_KEY,
        /** The value type, unwrapping a keyed LIST/MAP state's List/MapSerializer. */
        VALUE,
        /**
         * The value type of a non-keyed (operator) state — {@code ListState}/{@code
         * UnionState}/{@code BroadcastState} — whose value serializer is stored flat/unwrapped, so
         * unlike {@link #VALUE} no ARRAY/MAP unwrapping is applied.
         */
        FLAT_VALUE
    }

    private final Map<String, StateMetaInfoSnapshot> preloadedStateMetadata;
    private final SerializerConfig serializerConfig;
    @Nullable private final TypeSerializerSnapshot<?> preloadedKeySnapshot;

    public SavepointTypeInfoResolver(
            Map<String, StateMetaInfoSnapshot> preloadedStateMetadata,
            SerializerConfig serializerConfig,
            @Nullable TypeSerializerSnapshot<?> preloadedKeySnapshot) {
        this.preloadedStateMetadata = preloadedStateMetadata;
        this.serializerConfig = serializerConfig;
        this.preloadedKeySnapshot = preloadedKeySnapshot;
    }

    /**
     * Resolves the {@link TypeInformation} for a keyed state key: primitive types directly from the
     * {@link LogicalType}, complex ones (POJO, Avro) from the backend key serializer snapshot
     * preloaded from the savepoint metadata (every state in the same keyed-state backend shares
     * it).
     *
     * @throws IllegalArgumentException if the type cannot be inferred
     */
    public TypeInformation<?> resolveKeyType(RowType.RowField rowField) {
        LogicalType logicalType = rowField.getType();

        Class<?> primitiveClass = primitiveClass(logicalType);
        if (primitiveClass != null) {
            return TypeInformation.of(primitiveClass);
        }

        // A ROW-typed key (POJO or Avro) has no primitive class; restore its serializer from the
        // savepoint metadata instead.
        if (logicalType.is(LogicalTypeRoot.ROW) && preloadedKeySnapshot != null) {
            return ExternalTypeInfo.of(
                    TypeConversions.fromLogicalToDataType(logicalType),
                    SavepointFallbackSchemaLoader.buildFallbackSerializer(preloadedKeySnapshot));
        }

        throw new IllegalArgumentException(
                "Cannot resolve key TypeInformation for column '"
                        + rowField.getName()
                        + "' with type "
                        + logicalType.getTypeRoot()
                        + ".");
    }

    /**
     * Resolves the {@link TypeSerializer} for a MAP state's key, or {@code null} if {@code isMap}
     * is {@code false} (the field's state type has no map key to resolve).
     */
    @Nullable
    public TypeSerializer<?> resolveMapKeySerializer(RowType.RowField rowField, boolean isMap) {
        return isMap ? resolveSerializer(rowField, InferenceContext.MAP_KEY) : null;
    }

    /**
     * Resolves the {@link TypeSerializer} for a state's key stored directly under {@code
     * CommonSerializerKeys#KEY_SERIALIZER} (e.g. an operator {@code BroadcastState}'s map key,
     * which — unlike a keyed {@code MapState}'s key — is not wrapped inside a {@code
     * MapSerializer}).
     */
    public TypeSerializer<?> resolveKeySerializer(RowType.RowField rowField) {
        return resolveSerializer(rowField, InferenceContext.KEY);
    }

    /** Resolves the {@link TypeSerializer} for a state's value. */
    public TypeSerializer<?> resolveValueSerializer(RowType.RowField rowField) {
        return resolveSerializer(rowField, InferenceContext.VALUE);
    }

    /**
     * Resolves the {@link TypeSerializer} for a non-keyed (operator) state's value with no
     * ARRAY/MAP unwrapping, unlike {@link #resolveValueSerializer} which unwraps a keyed LIST/MAP
     * state's wrapping ListSerializer/MapSerializer.
     */
    public TypeSerializer<?> resolveFlatValueSerializer(RowType.RowField rowField) {
        return resolveSerializer(rowField, InferenceContext.FLAT_VALUE);
    }

    /**
     * Resolves the precise {@link StateDescriptor.Type} (e.g. {@code REDUCING}/{@code AGGREGATING})
     * a state was originally registered under, read from the {@code KEYED_STATE_TYPE} option in the
     * preloaded savepoint metadata. Returns {@code UNKNOWN} when the state (or the option) is not
     * present, in which case callers fall back to treating the state as plain VALUE/LIST/MAP.
     */
    public StateDescriptor.Type resolveStateKind(String stateName) {
        StateMetaInfoSnapshot stateMetaInfo = preloadedStateMetadata.get(stateName);
        if (stateMetaInfo == null) {
            return StateDescriptor.Type.UNKNOWN;
        }
        String kind =
                stateMetaInfo.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE);
        if (kind == null) {
            return StateDescriptor.Type.UNKNOWN;
        }
        try {
            return StateDescriptor.Type.valueOf(kind);
        } catch (IllegalArgumentException e) {
            return StateDescriptor.Type.UNKNOWN;
        }
    }

    /**
     * Resolves the {@link TypeSerializer} used for the namespace under which {@code stateName} is
     * registered (e.g. a window serializer), read directly from the preloaded savepoint metadata.
     *
     * <p>Unlike {@link #resolveValueSerializer}, there is no LogicalType-based fallback: the
     * namespace serializer is only ever needed for states already classified as namespaced, so the
     * metadata is known to contain it.
     */
    public TypeSerializer<?> resolveNamespaceSerializer(String stateName) {
        StateMetaInfoSnapshot stateMetaInfo = preloadedStateMetadata.get(stateName);
        if (stateMetaInfo == null) {
            throw new IllegalArgumentException(
                    "State '" + stateName + "' not found in preloaded metadata.");
        }
        TypeSerializerSnapshot<?> namespaceSnapshot =
                stateMetaInfo.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
        if (namespaceSnapshot == null) {
            throw new IllegalArgumentException(
                    "State '" + stateName + "' has no namespace serializer in metadata.");
        }
        return SavepointFallbackSchemaLoader.buildFallbackSerializer(namespaceSnapshot);
    }

    /**
     * Resolves the serializer for a table field, preferring the serializer the state was actually
     * written with (extracted from the preloaded savepoint metadata) and falling back to inference
     * from the table schema's {@link LogicalType}.
     *
     * @return the resolved serializer, or {@code null} for complex types (ROW/POJO) that cannot be
     *     inferred, which callers resolve from the savepoint header instead
     */
    @Nullable
    private TypeSerializer<?> resolveSerializer(
            RowType.RowField rowField, InferenceContext context) {
        try {
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

            TypeInformation<?> fallbackTypeInfo = inferTypeFromLogicalType(rowField, context);
            return fallbackTypeInfo == null
                    ? null
                    : fallbackTypeInfo.createSerializer(serializerConfig);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve serializer for field " + rowField.getName(), e);
        }
    }

    /**
     * Extracts the serializer the state was written with from the preloaded metadata. Performs no
     * I/O and no {@link TypeInformation} conversion, so it works with any serializer type (Avro,
     * custom types, etc.).
     */
    private Optional<TypeSerializer<?>> getSerializerFromMetadata(
            RowType.RowField rowField, InferenceContext context) {
        String stateName = rowField.getName();
        try {
            StateMetaInfoSnapshot stateMetaInfo = preloadedStateMetadata.get(stateName);
            if (stateMetaInfo == null) {
                LOG.debug("State '{}' not found in preloaded metadata", stateName);
                return Optional.empty();
            }

            TypeSerializerSnapshot<?> serializerSnapshot;
            if (context == InferenceContext.KEY) {
                serializerSnapshot =
                        stateMetaInfo.getTypeSerializerSnapshot(
                                StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER);
            } else {
                serializerSnapshot =
                        stateMetaInfo.getTypeSerializerSnapshot(
                                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
            }

            if (serializerSnapshot == null) {
                LOG.debug(
                        "No serializer snapshot found for state '{}' with context {}",
                        stateName,
                        context);
                return Optional.empty();
            }

            // Restore via the POJO-friendly path, so a missing POJO class does not fail the
            // restore.
            TypeSerializer<?> serializer =
                    SavepointFallbackSchemaLoader.buildFallbackSerializer(serializerSnapshot);

            switch (context) {
                case MAP_KEY:
                    // A keyed MAP state's key serializer lives inside its MapSerializer.
                    return serializer instanceof MapSerializer
                            ? Optional.of(((MapSerializer<?, ?>) serializer).getKeySerializer())
                            : Optional.empty();
                case VALUE:
                    return unwrapValueSerializer(serializer, rowField.getType());
                default:
                    return Optional.of(serializer);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to extract serializer from metadata for field '{}': {}",
                    stateName,
                    e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Unwraps the element/value serializer of a keyed LIST/MAP state (whose metadata serializer is
     * a List/MapSerializer); other logical types use the serializer as-is.
     */
    private Optional<TypeSerializer<?>> unwrapValueSerializer(
            TypeSerializer<?> fullSerializer, LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case ARRAY:
                if (fullSerializer instanceof ListSerializer) {
                    return Optional.of(((ListSerializer<?>) fullSerializer).getElementSerializer());
                }
                LOG.debug(
                        "Expected ListSerializer for ARRAY logical type but got: {}",
                        fullSerializer.getClass());
                return Optional.empty();

            case MAP:
                if (fullSerializer instanceof MapSerializer) {
                    return Optional.of(((MapSerializer<?, ?>) fullSerializer).getValueSerializer());
                }
                LOG.debug(
                        "Expected MapSerializer for MAP logical type but got: {}",
                        fullSerializer.getClass());
                return Optional.empty();

            default:
                return Optional.of(fullSerializer);
        }
    }

    /**
     * Infers the {@link TypeInformation} from the table schema's {@link LogicalType} when the state
     * is absent from the savepoint metadata. Returns {@code null} for complex value types, which
     * the caller resolves from the savepoint header instead.
     */
    @Nullable
    private TypeInformation<?> inferTypeFromLogicalType(
            RowType.RowField rowField, InferenceContext context) {
        LogicalType logicalType = rowField.getType();

        switch (context) {
            case KEY:
            case FLAT_VALUE:
                // Keys are always primitive here (complex ROW keys are handled by
                // resolveKeyType), and non-keyed state values are never ARRAY/MAP-wrapped.
                Class<?> flatClass = primitiveClass(logicalType);
                if (flatClass == null) {
                    throw new UnsupportedOperationException(
                            "Cannot infer a " + context + " type from logical type " + logicalType);
                }
                return TypeInformation.of(flatClass);

            case MAP_KEY:
                if (!(logicalType instanceof MapType)) {
                    throw new UnsupportedOperationException(
                            "MAP_KEY context requires MAP logical type, but got: " + logicalType);
                }
                return TypeInformation.of(primitiveClass(((MapType) logicalType).getKeyType()));

            case VALUE:
                // primitiveClass() already unwraps ARRAY element and MAP value types.
                Class<?> valueClass = primitiveClass(logicalType);
                return valueClass == null ? null : TypeInformation.of(valueClass);

            default:
                throw new UnsupportedOperationException("Unknown context: " + context);
        }
    }

    /**
     * Maps a {@link LogicalType} to its Java class, unwrapping ARRAY element and MAP value types,
     * or {@code null} for complex/unknown types (e.g. ROW/POJO) whose class cannot be inferred from
     * the schema alone.
     */
    @Nullable
    private static Class<?> primitiveClass(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return String.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DECIMAL:
                return BigDecimal.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
                return Integer.class;
            case BIGINT:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case ARRAY:
                return primitiveClass(((ArrayType) logicalType).getElementType());
            case MAP:
                return primitiveClass(((MapType) logicalType).getValueType());
            default:
                return null;
        }
    }
}
