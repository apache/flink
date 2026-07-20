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

package org.apache.flink.state.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.state.api.schema.KeyedStateSchemaInfo;
import org.apache.flink.state.api.schema.NonKeyedStateSchemaInfo;
import org.apache.flink.state.api.schema.OperatorStateSchemaInfo;
import org.apache.flink.state.api.schema.SerializerSnapshotToLogicalTypeConverter;
import org.apache.flink.state.api.schema.StateSchemaExtractor;
import org.apache.flink.state.api.schema.StateSchemaInfo;
import org.apache.flink.state.table.SavepointConnectorOptions;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManagerImpl;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * High-level utility for inspecting and reading keyed and non-keyed state from a checkpoint /
 * savepoint without requiring user POJO classes on the classpath.
 */
@Internal
public final class StateTableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StateTableUtils.class);

    private StateTableUtils() {}

    /**
     * Returns identifiers of all operators with at least one non-internal keyed or non-keyed state.
     */
    public static List<OperatorIdentifier> getOperatorIdentifiers(CheckpointMetadata metadata) {
        return metadata.getOperatorStates().stream()
                .filter(op -> hasNonInternalKeyedState(op) || hasNonInternalOperatorState(op))
                .map(
                        op ->
                                op.getOperatorUid()
                                        .map(OperatorIdentifier::forUid)
                                        .orElseGet(
                                                () ->
                                                        OperatorIdentifier.forUidHash(
                                                                op.getOperatorID().toHexString())))
                .collect(Collectors.toList());
    }

    private static boolean hasNonInternalKeyedState(OperatorState op) {
        try {
            List<StateSchemaInfo> schemas = StateSchemaExtractor.extractSchema(op);
            ClassifiedStates classified = classifyStates(op.getOperatorID().toHexString(), schemas);
            return !classified.voidNamespaceStates.isEmpty()
                    || !classified.windowNamespaceStates.isEmpty();
        } catch (Exception e) {
            LOG.error(
                    "Could not extract state schema for operator '{}': {}. Excluding from catalog.",
                    op.getOperatorID(),
                    e.getMessage());
            return false;
        }
    }

    private static boolean hasNonInternalOperatorState(OperatorState op) {
        try {
            List<OperatorStateSchemaInfo> schemas = StateSchemaExtractor.extractOperatorSchema(op);
            return schemas.stream().anyMatch(info -> !isInternalState(info.stateName));
        } catch (Exception e) {
            LOG.error(
                    "Could not extract non-keyed state schema for operator '{}': {}. "
                            + "Excluding from catalog.",
                    op.getOperatorID(),
                    e.getMessage());
            return false;
        }
    }

    /** Returns the names of all keyed states registered by the given operator. */
    public static List<String> getKeyedStates(
            CheckpointMetadata metadata, OperatorIdentifier operatorId) throws IOException {
        OperatorState opState = findOperatorState(metadata, operatorId);
        List<StateSchemaInfo> schemaInfos = StateSchemaExtractor.extractSchema(opState);
        ClassifiedStates classified = classifyStates(operatorId.toString(), schemaInfos);
        return classified.voidNamespaceStates.stream()
                .map(info -> info.stateName)
                .collect(Collectors.toList());
    }

    /**
     * Returns schema info for the plain per-key (void-namespace) states of the given operator — the
     * ones exposed by the {@code _keyed}/{@code _keyed_flat} tables. Extraction is lenient: field
     * names/types come from the serializer snapshot, so the user POJO class need not be on the
     * classpath.
     */
    public static KeyedStateSchemaInfo getKeyedStateSchema(
            CheckpointMetadata metadata, OperatorIdentifier operatorId) throws IOException {
        OperatorState opState = findOperatorState(metadata, operatorId);
        List<StateSchemaInfo> schemas = StateSchemaExtractor.extractSchema(opState);
        ClassifiedStates classified = classifyStates(operatorId.toString(), schemas);
        return buildKeyedStateSchemaInfo(schemas, classified.voidNamespaceStates, null);
    }

    /**
     * Like {@link #getKeyedStateSchema}, but for the namespaced (e.g. window-scoped) states exposed
     * by the {@code _windowed}/{@code _windowed_flat} tables. Empty if the operator has no
     * namespaced state; see {@link #classifyStates} for how multiple namespace types are handled.
     */
    public static KeyedStateSchemaInfo getWindowKeyedStateSchema(
            CheckpointMetadata metadata, OperatorIdentifier operatorId) throws IOException {
        OperatorState opState = findOperatorState(metadata, operatorId);
        List<StateSchemaInfo> schemas = StateSchemaExtractor.extractSchema(opState);
        ClassifiedStates classified = classifyStates(operatorId.toString(), schemas);
        return buildKeyedStateSchemaInfo(
                schemas, classified.windowNamespaceStates, classified.windowLogicalType);
    }

    private static KeyedStateSchemaInfo buildKeyedStateSchemaInfo(
            List<StateSchemaInfo> allSchemas,
            List<StateSchemaInfo> statesToInclude,
            @Nullable LogicalType windowLogicalType) {
        LogicalType keyType =
                allSchemas.isEmpty()
                        ? new VarBinaryType(true, VarBinaryType.MAX_LENGTH)
                        : SerializerSnapshotToLogicalTypeConverter.convert(
                                allSchemas.get(0).keySnapshot);

        LinkedHashMap<String, KeyedStateSchemaInfo.StateEntryInfo> stateSchemas =
                new LinkedHashMap<>();
        for (StateSchemaInfo info : statesToInclude) {
            SavepointConnectorOptions.StateType stateType;
            if (info.stateKind == StateDescriptor.Type.LIST) {
                stateType = SavepointConnectorOptions.StateType.LIST;
            } else if (info.stateKind == StateDescriptor.Type.MAP) {
                stateType = SavepointConnectorOptions.StateType.MAP;
            } else {
                stateType = SavepointConnectorOptions.StateType.VALUE;
            }

            try {
                LogicalType logicalType =
                        SerializerSnapshotToLogicalTypeConverter.convert(info.valueSnapshot);
                stateSchemas.put(
                        info.stateName,
                        new KeyedStateSchemaInfo.StateEntryInfo(
                                stateType, logicalType, windowLogicalType));
            } catch (Exception e) {
                logSchemaExtractionFailure("", info.stateName, info.valueSnapshot, e);
            }
        }

        return new KeyedStateSchemaInfo(keyType, stateSchemas);
    }

    /**
     * Logs that a state's schema extraction failed and it will be excluded from the table schema.
     */
    private static void logSchemaExtractionFailure(
            String label,
            String stateName,
            @Nullable TypeSerializerSnapshot<?> valueSnapshot,
            Exception e) {
        LOG.error(
                "Cannot extract schema for {}state '{}' (serializer type: {}): {}. "
                        + "This state will be excluded from the table schema. "
                        + "Use explicit connector options to include it.",
                label,
                stateName,
                valueSnapshot == null ? "null" : valueSnapshot.getClass().getSimpleName(),
                e.getMessage());
    }

    /**
     * Builds a {@link CatalogTable} for all keyed states of an operator: one {@code "state_key"}
     * column plus one column per keyed state. Pre-populates {@link
     * SavepointConnectorOptions#STATE_BACKEND_TYPE} when it can be unambiguously determined from
     * the checkpoint metadata.
     */
    public static CatalogTable getStateCatalogTable(
            CheckpointMetadata metadata,
            KeyedStateSchemaInfo schemaInfo,
            String statePath,
            OperatorIdentifier operatorIdentifier) {
        return buildKeyedCatalogTable(metadata, schemaInfo, statePath, operatorIdentifier, null);
    }

    /**
     * Builds a {@link CatalogTable} exposing a single keyed LIST or MAP state flattened into one
     * row per list element / map entry, rather than one row per key. Value column names are fixed
     * rather than the state's own name to avoid collisions with reserved columns:
     *
     * <ul>
     *   <li>LIST: {@code (state_key, list_index, list_value)}, PK {@code (state_key, list_index)}
     *   <li>MAP: {@code (state_key, map_key, map_value)}, PK {@code (state_key, map_key)}
     * </ul>
     */
    public static CatalogTable getFlattenedStateCatalogTable(
            CheckpointMetadata metadata,
            KeyedStateSchemaInfo schemaInfo,
            String stateName,
            String statePath,
            OperatorIdentifier operatorIdentifier) {
        return buildFlattenedKeyedCatalogTable(
                metadata, schemaInfo, stateName, statePath, operatorIdentifier, false);
    }

    /**
     * Builds a {@link CatalogTable} representing all namespaced (e.g. window-scoped) states of an
     * operator, as returned by {@link #getWindowKeyedStateSchema}: one row per {@code (state_key,
     * state_window)} pair, plus one column per namespaced VALUE-shaped state.
     */
    public static CatalogTable getWindowStateCatalogTable(
            CheckpointMetadata metadata,
            KeyedStateSchemaInfo schemaInfo,
            String statePath,
            OperatorIdentifier operatorIdentifier) {

        LogicalType windowType =
                schemaInfo.stateSchemas.values().stream()
                        .map(entry -> entry.windowLogicalType)
                        .filter(java.util.Objects::nonNull)
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "No namespaced state found for operator '"
                                                        + operatorIdentifier
                                                        + "'."));

        return buildKeyedCatalogTable(
                metadata, schemaInfo, statePath, operatorIdentifier, windowType);
    }

    /**
     * Builds a {@link CatalogTable} for either the plain keyed table ({@code windowType == null})
     * or the namespaced/window table (see {@link #getWindowStateCatalogTable}).
     */
    private static CatalogTable buildKeyedCatalogTable(
            CheckpointMetadata metadata,
            KeyedStateSchemaInfo schemaInfo,
            String statePath,
            OperatorIdentifier operatorIdentifier,
            @Nullable LogicalType windowType) {

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column(
                "state_key", LogicalTypeDataTypeConverter.toDataType(schemaInfo.keyType).notNull());
        if (windowType != null) {
            schemaBuilder.column(
                    "state_window", LogicalTypeDataTypeConverter.toDataType(windowType).notNull());
        }

        for (Map.Entry<String, KeyedStateSchemaInfo.StateEntryInfo> entry :
                schemaInfo.stateSchemas.entrySet()) {
            schemaBuilder.column(entry.getKey(), stateValueColumnDataType(entry.getValue()));
        }
        if (windowType == null) {
            schemaBuilder.primaryKeyNamed("PK_state_key", "state_key");
        } else {
            schemaBuilder.primaryKeyNamed("PK_state_key_state_window", "state_key", "state_window");
        }
        Schema schema = schemaBuilder.build();

        Map<String, String> options =
                buildBaseConnectorOptions(
                        statePath,
                        operatorIdentifier,
                        windowType == null
                                ? SavepointConnectorOptions.StateReaderMode.KEYED
                                : SavepointConnectorOptions.StateReaderMode.WINDOWED);
        withStateBackendType(options, metadata, operatorIdentifier);

        return CatalogTable.newBuilder().schema(schema).options(options).build();
    }

    /**
     * Like {@link #getFlattenedStateCatalogTable}, but for a namespaced (window-scoped) LIST/MAP
     * state: {@code (state_key, state_window, list_index, list_value)} for LIST, or {@code
     * (state_key, state_window, map_key, map_value)} for MAP.
     */
    public static CatalogTable getFlattenedWindowStateCatalogTable(
            CheckpointMetadata metadata,
            KeyedStateSchemaInfo schemaInfo,
            String stateName,
            String statePath,
            OperatorIdentifier operatorIdentifier) {
        return buildFlattenedKeyedCatalogTable(
                metadata, schemaInfo, stateName, statePath, operatorIdentifier, true);
    }

    /**
     * Builds a {@link CatalogTable} exposing a single LIST or MAP state flattened into one row per
     * list element / map entry, either plain-keyed ({@code windowed == false}, see {@link
     * #getFlattenedStateCatalogTable}) or namespaced ({@code windowed == true}, see {@link
     * #getFlattenedWindowStateCatalogTable}).
     */
    private static CatalogTable buildFlattenedKeyedCatalogTable(
            CheckpointMetadata metadata,
            KeyedStateSchemaInfo schemaInfo,
            String stateName,
            String statePath,
            OperatorIdentifier operatorIdentifier,
            boolean windowed) {

        KeyedStateSchemaInfo.StateEntryInfo entryInfo = schemaInfo.stateSchemas.get(stateName);
        if (entryInfo == null) {
            throw new IllegalArgumentException(
                    "State '"
                            + stateName
                            + "' not found for operator '"
                            + operatorIdentifier
                            + "'.");
        }
        if (entryInfo.stateType != SavepointConnectorOptions.StateType.LIST
                && entryInfo.stateType != SavepointConnectorOptions.StateType.MAP) {
            throw new IllegalArgumentException(
                    "Flattened state tables are only supported for LIST and MAP states, but '"
                            + stateName
                            + "' is "
                            + entryInfo.stateType
                            + ".");
        }
        if (windowed && entryInfo.windowLogicalType == null) {
            throw new IllegalArgumentException(
                    "State '"
                            + stateName
                            + "' is not a namespaced state for operator '"
                            + operatorIdentifier
                            + "'.");
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column(
                "state_key", LogicalTypeDataTypeConverter.toDataType(schemaInfo.keyType).notNull());
        if (windowed) {
            schemaBuilder.column(
                    "state_window",
                    LogicalTypeDataTypeConverter.toDataType(entryInfo.windowLogicalType).notNull());
        }

        String subKeyColumnName = addFlattenedValueColumns(schemaBuilder, entryInfo);
        if (!windowed) {
            schemaBuilder.primaryKeyNamed(
                    "PK_state_key_" + subKeyColumnName, "state_key", subKeyColumnName);
        }
        Schema schema = schemaBuilder.build();

        Map<String, String> options =
                buildBaseConnectorOptions(
                        statePath,
                        operatorIdentifier,
                        windowed
                                ? SavepointConnectorOptions.StateReaderMode.WINDOWED_FLAT
                                : SavepointConnectorOptions.StateReaderMode.KEYED_FLAT);
        options.put(SavepointConnectorOptions.FLATTENED_STATE_NAME.key(), stateName);
        withStateBackendType(options, metadata, operatorIdentifier);

        return CatalogTable.newBuilder().schema(schema).options(options).build();
    }

    /**
     * Adds the LIST- or MAP-shaped sub-key and value columns (e.g. {@code (list_index, list_value)}
     * or {@code (map_key, map_value)}) for a flattened state table, and returns the sub-key
     * column's name.
     */
    private static String addFlattenedValueColumns(
            Schema.Builder schemaBuilder, KeyedStateSchemaInfo.StateEntryInfo entryInfo) {
        LogicalType valueType;
        String subKeyColumnName;
        String valueColumnName;
        if (entryInfo.stateType == SavepointConnectorOptions.StateType.LIST) {
            valueType = ((ArrayType) entryInfo.logicalType).getElementType();
            subKeyColumnName = "list_index";
            valueColumnName = "list_value";
            schemaBuilder.column(
                    subKeyColumnName,
                    LogicalTypeDataTypeConverter.toDataType(new BigIntType(false)));
        } else {
            MapType mapType = (MapType) entryInfo.logicalType;
            valueType = mapType.getValueType();
            subKeyColumnName = "map_key";
            valueColumnName = "map_value";
            schemaBuilder.column(
                    subKeyColumnName,
                    LogicalTypeDataTypeConverter.toDataType(mapType.getKeyType()).notNull());
        }
        schemaBuilder.column(valueColumnName, LogicalTypeDataTypeConverter.toDataType(valueType));
        return subKeyColumnName;
    }

    /**
     * Returns schema info for the non-keyed (operator) states of the given operator — {@code
     * ListState}, {@code UnionState}, {@code BroadcastState} — exposed by the {@code _list}/{@code
     * _union}/{@code _broadcast} tables. Extraction is lenient (no user POJO class required on the
     * classpath); states whose schema can't be determined are excluded with a logged error.
     */
    public static NonKeyedStateSchemaInfo getNonKeyedStateSchema(
            CheckpointMetadata metadata, OperatorIdentifier operatorId) throws IOException {
        OperatorState opState = findOperatorState(metadata, operatorId);
        List<OperatorStateSchemaInfo> schemas = StateSchemaExtractor.extractOperatorSchema(opState);

        LinkedHashMap<String, NonKeyedStateSchemaInfo.StateEntryInfo> stateSchemas =
                new LinkedHashMap<>();
        for (OperatorStateSchemaInfo info : schemas) {
            if (isInternalState(info.stateName)) {
                continue;
            }
            try {
                LogicalType valueLogicalType =
                        SerializerSnapshotToLogicalTypeConverter.convert(info.valueSnapshot);
                LogicalType mapKeyLogicalType =
                        info.keySnapshot == null
                                ? null
                                : SerializerSnapshotToLogicalTypeConverter.convert(
                                        info.keySnapshot);
                stateSchemas.put(
                        info.stateName,
                        new NonKeyedStateSchemaInfo.StateEntryInfo(
                                info.kind, valueLogicalType, mapKeyLogicalType));
            } catch (Exception e) {
                logSchemaExtractionFailure("non-keyed ", info.stateName, info.valueSnapshot, e);
            }
        }

        return new NonKeyedStateSchemaInfo(stateSchemas);
    }

    /**
     * Builds a {@link CatalogTable} exposing a single {@code ListState} or {@code UnionState}, with
     * one row per list element: a ROW-typed value contributes one column per field, a scalar value
     * gets a single column named after the state. No ordering column and no primary key, since no
     * column is guaranteed unique across rows. Unlike the keyed table builders, this does not set
     * {@link SavepointConnectorOptions#STATE_BACKEND_TYPE} since non-keyed state isn't part of a
     * state backend.
     */
    public static CatalogTable getOperatorStateCatalogTable(
            NonKeyedStateSchemaInfo schemaInfo,
            String stateName,
            String statePath,
            OperatorIdentifier operatorIdentifier) {

        NonKeyedStateSchemaInfo.StateEntryInfo entryInfo =
                findNonKeyedStateEntry(schemaInfo, stateName, operatorIdentifier);
        if (entryInfo.kind != SavepointConnectorOptions.StateReaderMode.LIST
                && entryInfo.kind != SavepointConnectorOptions.StateReaderMode.UNION) {
            throw new IllegalArgumentException(
                    "Operator state tables are only supported for LIST and UNION states, but '"
                            + stateName
                            + "' is "
                            + entryInfo.kind
                            + ".");
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        if (entryInfo.valueLogicalType instanceof RowType) {
            for (RowType.RowField field : ((RowType) entryInfo.valueLogicalType).getFields()) {
                schemaBuilder.column(
                        field.getName(), LogicalTypeDataTypeConverter.toDataType(field.getType()));
            }
        } else {
            schemaBuilder.column(
                    stateName, LogicalTypeDataTypeConverter.toDataType(entryInfo.valueLogicalType));
        }

        Map<String, String> options =
                buildBaseConnectorOptions(statePath, operatorIdentifier, entryInfo.kind);
        options.put(SavepointConnectorOptions.FLATTENED_STATE_NAME.key(), stateName);

        return CatalogTable.newBuilder().schema(schemaBuilder.build()).options(options).build();
    }

    /**
     * Builds a {@link CatalogTable} exposing a single {@code BroadcastState}, one row per map
     * entry: {@code (map_key NOT NULL, map_value)} with PK {@code map_key}. Column names are fixed
     * rather than the state's own name to avoid collisions with reserved columns. Like {@link
     * #getOperatorStateCatalogTable}, this does not set {@link
     * SavepointConnectorOptions#STATE_BACKEND_TYPE}.
     */
    public static CatalogTable getBroadcastStateCatalogTable(
            NonKeyedStateSchemaInfo schemaInfo,
            String stateName,
            String statePath,
            OperatorIdentifier operatorIdentifier) {

        NonKeyedStateSchemaInfo.StateEntryInfo entryInfo =
                findNonKeyedStateEntry(schemaInfo, stateName, operatorIdentifier);
        if (entryInfo.kind != SavepointConnectorOptions.StateReaderMode.BROADCAST) {
            throw new IllegalArgumentException(
                    "Broadcast state tables are only supported for BROADCAST states, but '"
                            + stateName
                            + "' is "
                            + entryInfo.kind
                            + ".");
        }

        Schema schema =
                Schema.newBuilder()
                        .column(
                                "map_key",
                                LogicalTypeDataTypeConverter.toDataType(entryInfo.mapKeyLogicalType)
                                        .notNull())
                        .column(
                                "map_value",
                                LogicalTypeDataTypeConverter.toDataType(entryInfo.valueLogicalType))
                        .primaryKeyNamed("PK_map_key", "map_key")
                        .build();

        Map<String, String> options =
                buildBaseConnectorOptions(
                        statePath,
                        operatorIdentifier,
                        SavepointConnectorOptions.StateReaderMode.BROADCAST);
        options.put(SavepointConnectorOptions.FLATTENED_STATE_NAME.key(), stateName);

        return CatalogTable.newBuilder().schema(schema).options(options).build();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Looks up a single non-keyed state entry, failing rather than returning {@code null} when the
     * operator has no such state.
     */
    private static NonKeyedStateSchemaInfo.StateEntryInfo findNonKeyedStateEntry(
            NonKeyedStateSchemaInfo schemaInfo,
            String stateName,
            OperatorIdentifier operatorIdentifier) {
        NonKeyedStateSchemaInfo.StateEntryInfo entryInfo = schemaInfo.stateSchemas.get(stateName);
        if (entryInfo == null) {
            throw new IllegalArgumentException(
                    "State '"
                            + stateName
                            + "' not found for operator '"
                            + operatorIdentifier
                            + "'.");
        }
        return entryInfo;
    }

    /**
     * Resolves the value column's {@link org.apache.flink.table.types.DataType}, forcing it
     * nullable for VALUE-shaped state: unlike LIST/MAP, a {@code ValueState} can legitimately hold
     * no value (never written, or cleared by a trigger), so a read may return {@code null}.
     */
    private static DataType stateValueColumnDataType(
            KeyedStateSchemaInfo.StateEntryInfo entryInfo) {
        DataType dataType = LogicalTypeDataTypeConverter.toDataType(entryInfo.logicalType);
        return entryInfo.stateType == SavepointConnectorOptions.StateType.VALUE
                ? dataType.nullable()
                : dataType;
    }

    private static OperatorState findOperatorState(
            CheckpointMetadata metadata, OperatorIdentifier operatorId) {
        for (OperatorState op : metadata.getOperatorStates()) {
            if (op.getOperatorID().equals(operatorId.getOperatorId())) {
                return op;
            }
        }
        throw new IllegalArgumentException(
                "Operator '" + operatorId + "' not found in checkpoint metadata.");
    }

    /**
     * Returns the base connector options shared by every savepoint-backed {@link CatalogTable}.
     * {@code readerMode} is always set explicitly rather than relying on the option's default, so
     * every table's options unambiguously reflect its schema.
     */
    private static Map<String, String> buildBaseConnectorOptions(
            String statePath,
            OperatorIdentifier operatorIdentifier,
            SavepointConnectorOptions.StateReaderMode readerMode) {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "savepoint");
        options.put(SavepointConnectorOptions.STATE_PATH.key(), statePath);
        options.put(SavepointConnectorOptions.STATE_READER_MODE.key(), readerMode.toString());
        operatorIdentifier
                .getUid()
                .ifPresentOrElse(
                        uid -> options.put(SavepointConnectorOptions.OPERATOR_UID.key(), uid),
                        () ->
                                options.put(
                                        SavepointConnectorOptions.OPERATOR_UID_HASH.key(),
                                        operatorIdentifier.getOperatorId().toHexString()));
        return options;
    }

    /**
     * Adds {@link SavepointConnectorOptions#STATE_BACKEND_TYPE} when it can be unambiguously
     * determined. Only meaningful for keyed state tables — non-keyed state isn't stored in a state
     * backend.
     */
    private static void withStateBackendType(
            Map<String, String> options,
            CheckpointMetadata metadata,
            OperatorIdentifier operatorIdentifier) {
        OperatorState opState = findOperatorState(metadata, operatorIdentifier);
        detectStateBackendType(opState)
                .ifPresent(
                        type ->
                                options.put(
                                        SavepointConnectorOptions.STATE_BACKEND_TYPE.key(), type));
    }

    /**
     * Attempts to determine the state backend that produced the operator's keyed state, from the
     * concrete {@link KeyedStateHandle} subtype: heap/HashMap backends produce {@link
     * KeyGroupsStateHandle}, RocksDB/ForSt produce {@link IncrementalKeyedStateHandle}.
     * Canonical-format savepoints rewrite state into the backend-agnostic {@link
     * KeyGroupsSavepointStateHandle}, in which case the backend can't be told and an empty result
     * is returned.
     */
    static Optional<String> detectStateBackendType(OperatorState opState) {
        Set<String> detectedTypes = new HashSet<>();
        for (OperatorSubtaskState subtaskState : opState.getStates()) {
            collectStateBackendTypes(subtaskState.getManagedKeyedState(), detectedTypes);
            collectStateBackendTypes(subtaskState.getRawKeyedState(), detectedTypes);
        }
        if (detectedTypes.size() != 1) {
            if (detectedTypes.size() > 1) {
                LOG.warn(
                        "Operator '{}' has keyed state handles from multiple state backends {}; "
                                + "not setting '{}'.",
                        opState.getOperatorID(),
                        detectedTypes,
                        SavepointConnectorOptions.STATE_BACKEND_TYPE.key());
            }
            return Optional.empty();
        }
        return Optional.of(detectedTypes.iterator().next());
    }

    private static void collectStateBackendTypes(
            Iterable<KeyedStateHandle> handles, Set<String> detectedTypes) {
        for (KeyedStateHandle handle : handles) {
            if (handle instanceof ChangelogStateBackendHandle) {
                collectStateBackendTypes(
                        ((ChangelogStateBackendHandle) handle).getMaterializedStateHandles(),
                        detectedTypes);
            } else if (handle instanceof IncrementalKeyedStateHandle) {
                detectedTypes.add(StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME);
            } else if (handle instanceof KeyGroupsSavepointStateHandle) {
                // backend-agnostic format; origin can't be told apart from the handle
            } else if (handle instanceof KeyGroupsStateHandle) {
                detectedTypes.add(StateBackendLoader.HASHMAP_STATE_BACKEND_NAME);
            } else {
                LOG.warn("Unknown handle type '{}'.", handle.getClass().getSimpleName());
            }
        }
    }

    /** Returns {@code true} for Flink-internal states that are not user-registered states. */
    private static boolean isInternalState(String stateName) {
        return stateName.startsWith(InternalTimeServiceManagerImpl.TIMER_STATE_PREFIX + "/")
                || stateName.equals(WindowOperator.MERGING_WINDOW_SET_STATE_NAME);
    }

    /**
     * Returns {@code true} for plain per-key state (registered with {@code VoidNamespace}), {@code
     * false} if scoped by another namespace (e.g. a window). A missing snapshot (older savepoint
     * format) is treated as void.
     */
    private static boolean isVoidNamespace(TypeSerializerSnapshot<?> namespaceSnapshot) {
        return namespaceSnapshot == null
                || namespaceSnapshot
                        instanceof VoidNamespaceSerializer.VoidNamespaceSerializerSnapshot;
    }

    /**
     * The result of {@link #classifyStates}: user-registered states of an operator, partitioned
     * into plain per-key (void-namespace) states and namespaced (e.g. window-scoped) states.
     */
    private static final class ClassifiedStates {
        final List<StateSchemaInfo> voidNamespaceStates;
        final List<StateSchemaInfo> windowNamespaceStates;
        @Nullable final LogicalType windowLogicalType;

        ClassifiedStates(
                List<StateSchemaInfo> voidNamespaceStates,
                List<StateSchemaInfo> windowNamespaceStates,
                @Nullable LogicalType windowLogicalType) {
            this.voidNamespaceStates = voidNamespaceStates;
            this.windowNamespaceStates = windowNamespaceStates;
            this.windowLogicalType = windowLogicalType;
        }
    }

    /**
     * Partitions user-registered states into plain per-key (void-namespace) and namespaced states,
     * resolving the namespaced states' shared {@link LogicalType}. An operator can register more
     * than one namespace type only via hand-rolled state access; when that happens, the first
     * resolvable namespace type is kept and the rest excluded with a warning.
     */
    private static ClassifiedStates classifyStates(
            String operatorLabel, List<StateSchemaInfo> schemas) {
        List<StateSchemaInfo> voidStates = new ArrayList<>();
        Map<String, List<StateSchemaInfo>> namespacedGroups = new LinkedHashMap<>();
        for (StateSchemaInfo info : schemas) {
            if (isInternalState(info.stateName)) {
                continue;
            }
            if (isVoidNamespace(info.namespaceSnapshot)) {
                voidStates.add(info);
            } else {
                namespacedGroups
                        .computeIfAbsent(
                                info.namespaceSnapshot.getClass().getName(), k -> new ArrayList<>())
                        .add(info);
            }
        }

        List<StateSchemaInfo> chosenGroup = Collections.emptyList();
        LogicalType chosenNamespaceType = null;
        for (Map.Entry<String, List<StateSchemaInfo>> entry : namespacedGroups.entrySet()) {
            if (chosenNamespaceType != null) {
                logExcludedNamespaceGroup(
                        operatorLabel,
                        entry.getKey(),
                        entry.getValue(),
                        "an operator has states registered under more than one namespace type");
                continue;
            }

            TypeSerializerSnapshot<?> representative = entry.getValue().get(0).namespaceSnapshot;
            try {
                chosenNamespaceType =
                        SerializerSnapshotToLogicalTypeConverter.convert(representative);
            } catch (UnsupportedOperationException e) {
                logExcludedNamespaceGroup(
                        operatorLabel,
                        entry.getKey(),
                        entry.getValue(),
                        "cannot extract schema for this namespace type: " + e.getMessage());
                continue;
            }
            chosenGroup = entry.getValue();
        }

        return new ClassifiedStates(voidStates, chosenGroup, chosenNamespaceType);
    }

    private static void logExcludedNamespaceGroup(
            String operatorLabel,
            String namespaceClassName,
            List<StateSchemaInfo> excluded,
            String reason) {
        LOG.warn(
                "Excluding namespace type '{}' on operator '{}' from the catalog: {}. States: {}.",
                namespaceClassName,
                operatorLabel,
                reason,
                excluded.stream().map(i -> i.stateName).collect(Collectors.toList()));
    }
}
