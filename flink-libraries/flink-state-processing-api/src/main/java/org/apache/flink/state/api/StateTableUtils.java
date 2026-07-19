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
import org.apache.flink.state.api.schema.SerializerSnapshotToLogicalTypeConverter;
import org.apache.flink.state.api.schema.StateSchemaExtractor;
import org.apache.flink.state.api.schema.StateSchemaInfo;
import org.apache.flink.state.table.SavepointConnectorOptions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
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
 * High-level utility for inspecting and reading keyed state from a checkpoint / savepoint without
 * requiring user POJO classes on the classpath.
 */
@Internal
public final class StateTableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StateTableUtils.class);

    private StateTableUtils() {}

    /**
     * Returns the {@link OperatorIdentifier}s of all operators present in the given checkpoint
     * metadata that have at least one non-internal keyed state.
     *
     * @param metadata the checkpoint metadata to inspect
     * @return list of operator identifiers; never null, may be empty
     */
    public static List<OperatorIdentifier> getOperatorIdentifiers(CheckpointMetadata metadata) {
        return metadata.getOperatorStates().stream()
                .filter(StateTableUtils::hasNonInternalKeyedState)
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
            LOG.warn(
                    "Could not extract state schema for operator '{}': {}. Excluding from catalog.",
                    op.getOperatorID(),
                    e.getMessage());
            return false;
        }
    }

    /**
     * Returns the names of all keyed states registered by the given operator.
     *
     * @param metadata the checkpoint metadata to inspect
     * @param operatorId identifies the operator
     * @param classLoader the class loader used when reading serializer snapshots
     * @return list of state names; never null, may be empty
     * @throws IOException if the state header cannot be read
     */
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
     * Returns the {@link KeyedStateSchemaInfo} for the plain per-key (void-namespace) states of the
     * given operator — the ones exposed by the {@code _keyed}/{@code _keyed_flat} tables.
     *
     * <p>Schema extraction is lenient: POJO field names and types are derived from the serializer
     * snapshot and do not require the user POJO class to be on the classpath.
     *
     * @param metadata the checkpoint metadata to inspect
     * @param operatorId identifies the operator
     * @return schema information covering the key type and all registered state entries
     * @throws IOException if the state header cannot be read
     */
    public static KeyedStateSchemaInfo getKeyedStateSchema(
            CheckpointMetadata metadata, OperatorIdentifier operatorId) throws IOException {
        OperatorState opState = findOperatorState(metadata, operatorId);
        List<StateSchemaInfo> schemas = StateSchemaExtractor.extractSchema(opState);
        ClassifiedStates classified = classifyStates(operatorId.toString(), schemas);
        return buildKeyedStateSchemaInfo(schemas, classified.voidNamespaceStates, null);
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
     * Logs that a single state's schema could not be extracted and will therefore be excluded from
     * the table schema, shared by {@link #buildKeyedStateSchemaInfo}.
     *
     * @param label a prefix inserted before "state" in the log message (e.g. {@code "non-keyed "}
     *     or {@code ""}), distinguishing which caller excluded the state
     */
    private static void logSchemaExtractionFailure(
            String label,
            String stateName,
            @Nullable TypeSerializerSnapshot<?> valueSnapshot,
            Exception e) {
        LOG.warn(
                "Cannot extract schema for {}state '{}' (serializer type: {}): {}. "
                        + "This state will be excluded from the table schema. "
                        + "Use explicit connector options to include it.",
                label,
                stateName,
                valueSnapshot == null ? "null" : valueSnapshot.getClass().getSimpleName(),
                e.getMessage());
    }

    /**
     * Builds a {@link CatalogTable} representing all keyed states of an operator.
     *
     * <p>The resulting table has one column named {@code "state_key"} for the key and one column
     * per keyed state. The connector options are pre-populated so the table can be registered
     * directly in a {@link org.apache.flink.table.catalog.CatalogManager}.
     *
     * <p>When the state backend that produced the operator's keyed state can be unambiguously
     * determined from the checkpoint metadata, {@link SavepointConnectorOptions#STATE_BACKEND_TYPE}
     * is pre-populated as well, so callers don't need to specify it themselves.
     *
     * @param metadata the checkpoint metadata the operator belongs to
     * @param schemaInfo the schema information returned by {@link #getKeyedStateSchema}
     * @param statePath the path to the savepoint / checkpoint
     * @param operatorIdentifier identifies the operator whose state to read
     * @return a {@link CatalogTable} ready for registration
     */
    public static CatalogTable getStateCatalogTable(
            CheckpointMetadata metadata,
            KeyedStateSchemaInfo schemaInfo,
            String statePath,
            OperatorIdentifier operatorIdentifier) {
        return buildKeyedCatalogTable(metadata, schemaInfo, statePath, operatorIdentifier, null);
    }

    /**
     * Builds a {@link CatalogTable} representing all keyed states of an operator, or, when {@code
     * windowType} is non-null, all namespaced (e.g. window-scoped) states of an operator.
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
        }
        Schema schema = schemaBuilder.build();

        Map<String, String> options = buildBaseConnectorOptions(statePath, operatorIdentifier);
        withStateBackendType(options, metadata, operatorIdentifier);

        return CatalogTable.newBuilder().schema(schema).options(options).build();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Resolves the SQL column {@link org.apache.flink.table.types.DataType} for a single state's
     * value column, forcing it nullable for VALUE-shaped state: unlike LIST/MAP (which always have
     * a value, possibly empty), a {@code ValueState}/{@code ReducingState}/{@code AggregatingState}
     * can legitimately hold no value (e.g. never written, or cleared by a trigger such as {@code
     * CountTrigger}), in which case a read returns {@code null}.
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
     * Returns the base connector options ({@link FactoryUtil#CONNECTOR}, {@link
     * SavepointConnectorOptions#STATE_PATH}, and the operator identifier option) shared by every
     * savepoint-backed {@link CatalogTable}.
     */
    private static Map<String, String> buildBaseConnectorOptions(
            String statePath, OperatorIdentifier operatorIdentifier) {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "savepoint");
        options.put(SavepointConnectorOptions.STATE_PATH.key(), statePath);
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
     * Adds {@link SavepointConnectorOptions#STATE_BACKEND_TYPE} to {@code options} when it can be
     * unambiguously determined from the checkpoint metadata. Only meaningful for keyed state
     * tables: non-keyed (list/union/broadcast) state isn't stored in a state backend, so callers
     * for those table kinds must not call this.
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
     * Attempts to determine the state backend (shortcut name, see {@link
     * StateBackendLoader#HASHMAP_STATE_BACKEND_NAME} / {@link
     * StateBackendLoader#ROCKSDB_STATE_BACKEND_NAME}) that produced the operator's keyed state, by
     * inspecting the concrete {@link KeyedStateHandle} subtype found in the checkpoint metadata:
     * heap/HashMap backends produce {@link KeyGroupsStateHandle}, RocksDB/ForSt backends produce
     * {@link IncrementalKeyedStateHandle}.
     *
     * <p>Canonical-format savepoints rewrite keyed state into the backend-agnostic {@link
     * KeyGroupsSavepointStateHandle}, in which case the originating backend can no longer be
     * determined from the handle alone; an empty result is returned rather than guessing.
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
                // Canonical-format savepoints rewrite keyed state into a backend-agnostic
                // format; the originating backend can no longer be told apart from the handle.
            } else if (handle instanceof KeyGroupsStateHandle) {
                detectedTypes.add(StateBackendLoader.HASHMAP_STATE_BACKEND_NAME);
            } else {
                LOG.warn("Unknown handle type '{}'.", handle.getClass().getSimpleName());
            }
        }
    }

    /** Returns {@code true} for Flink-internal states that are not user-registered states. */
    private static boolean isInternalState(String stateName) {
        // Flink's built-in timer states use this prefix.
        // "merging-window-set" is WindowOperator's internal session-window merging bookkeeping
        // state, not a user-registered state.
        return stateName.startsWith("_timer_state/") || stateName.equals("merging-window-set");
    }

    /**
     * Returns {@code true} if a state is plain per-key state (registered with {@code
     * VoidNamespace}) and {@code false} if it is scoped by some other namespace (e.g. a window).
     *
     * <p>A missing namespace snapshot (e.g. from an older savepoint format) is treated as void,
     * matching pre-existing behavior.
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
     * Partitions the user-registered states of a single operator into plain per-key
     * (void-namespace) states and namespaced states, resolving the namespaced states' shared {@link
     * LogicalType} along the way.
     *
     * <p>An operator may register states under more than one distinct namespace <em>type</em> only
     * via hand-rolled state access (no built-in windowing API does this); when that happens, the
     * first namespace type whose schema can be determined is kept, and every other group is
     * excluded with a logged warning.
     *
     * @param operatorLabel a human-readable operator identifier, used only for log messages
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
            } catch (Exception e) {
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
