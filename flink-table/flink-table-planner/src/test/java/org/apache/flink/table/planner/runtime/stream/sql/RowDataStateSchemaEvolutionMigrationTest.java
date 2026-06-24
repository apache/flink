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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RunnableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drives the full RowData state-migration stack end-to-end against the RocksDB keyed state backend:
 * register a {@code ValueState<RowData>} / {@code MapState<RowData, RowData>} with an old-schema
 * name-retaining serializer, snapshot, then restore with an evolved new-schema name-retaining
 * serializer. The restore exercises {@code TtlAwareSerializer.migrateValueFromPriorSerializer} ->
 * {@code RowDataSerializerSnapshot.migrate}.
 *
 * <p>This is the deterministic proof for the RowData state schema-evolution feature: when the value
 * serializer retains field names, an added-nullable-field / reordered evolution restores with old
 * fields preserved by name and added fields null; a leaf type change is rejected.
 */
class RowDataStateSchemaEvolutionMigrationTest {

    // old schema: (id INT NOT NULL, name VARCHAR)
    private static final RowType OLD_TYPE =
            RowType.of(
                    new LogicalType[] {new IntType(false), new VarCharType(VarCharType.MAX_LENGTH)},
                    new String[] {"id", "name"});

    // new schema: name "name" reordered before "id", with an added nullable field "score BIGINT"
    private static final RowType NEW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new IntType(false),
                        new BigIntType(true)
                    },
                    new String[] {"name", "id", "score"});

    // a leaf type change: id INT -> BIGINT (incompatible)
    private static final RowType TYPE_CHANGED_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new BigIntType(false), new VarCharType(VarCharType.MAX_LENGTH)
                    },
                    new String[] {"id", "name"});

    private MockEnvironment env;

    @BeforeEach
    void before() throws Exception {
        env = MockEnvironment.builder().build();
        env.setCheckpointStorageAccess(
                new JobManagerCheckpointStorage().createCheckpointStorage(new JobID()));
    }

    @AfterEach
    void after() throws Exception {
        if (env != null) {
            env.close();
        }
    }

    @Test
    void testValueStateMigrationPreservesFieldsByName() throws Exception {
        RowData migrated = migrateValueState(stateTtlConfig(), oldValue());
        assertEvolved(migrated);
    }

    @Test
    void testValueStateMigrationWithoutTtl() throws Exception {
        RowData migrated = migrateValueState(StateTtlConfig.DISABLED, oldValue());
        assertEvolved(migrated);
    }

    @Test
    void testMapValueMigrationPreservesFieldsByName() throws Exception {
        RowData migrated = migrateMapValue(stateTtlConfig(), oldValue());
        assertEvolved(migrated);
    }

    @Test
    void testLeafTypeChangeIsRejected() {
        assertThatThrownBy(
                        () ->
                                runValueStateUpgrade(
                                        stateTtlConfig(),
                                        descriptor("state", stateTtlConfig(), OLD_TYPE),
                                        descriptor("state", stateTtlConfig(), TYPE_CHANGED_TYPE),
                                        oldValue()))
                .satisfiesAnyOf(
                        e -> assertThat(e).isInstanceOf(StateMigrationException.class),
                        e -> assertThat(e).hasCauseInstanceOf(StateMigrationException.class));
    }

    /**
     * Proves the gate through a public, observable contract: with schema evolution enabled the
     * value serializer retains field names, so a name-based (added-nullable / reordered) evolution
     * resolves to {@code compatibleAfterMigration}; with it disabled the names-less serializer
     * keeps today's behavior and the same evolution resolves to {@code incompatible}.
     */
    @Test
    void testEvolutionFlagControlsSchemaEvolutionCompatibility() {
        InternalTypeInfo<RowData> oldRecordType = InternalTypeInfo.of(OLD_TYPE);
        InternalTypeInfo<RowData> newRecordType = InternalTypeInfo.of(NEW_TYPE);

        // evolution enabled: names retained on both sides -> compatibleAfterMigration
        RowDataSerializer evolutionOnNew = RowDataSerializer.withFieldNames(NEW_TYPE);
        TypeSerializerSnapshot<RowData> evolutionOnOld =
                RowDataSerializer.withFieldNames(OLD_TYPE).snapshotConfiguration();
        assertThat(
                        evolutionOnNew
                                .snapshotConfiguration()
                                .resolveSchemaCompatibility(evolutionOnOld)
                                .isCompatibleAfterMigration())
                .isTrue();

        // evolution disabled: names-less serializers (the existing TypeInformation path) -> today's
        // behavior, a differing layout is incompatible
        RowDataSerializer evolutionOffNew =
                (RowDataSerializer)
                        newRecordType.createSerializer(
                                env.getExecutionConfig().getSerializerConfig());
        TypeSerializerSnapshot<RowData> evolutionOffOld =
                oldRecordType
                        .createSerializer(env.getExecutionConfig().getSerializerConfig())
                        .snapshotConfiguration();
        assertThat(
                        evolutionOffNew
                                .snapshotConfiguration()
                                .resolveSchemaCompatibility(evolutionOffOld)
                                .isIncompatible())
                .isTrue();
    }

    // ------------------------------------------------------------------------------------

    private static GenericRowData oldValue() {
        GenericRowData value = new GenericRowData(2);
        value.setRowKind(RowKind.UPDATE_AFTER);
        value.setField(0, 42);
        value.setField(1, StringData.fromString("alice"));
        return value;
    }

    private static void assertEvolved(RowData migrated) {
        assertThat(migrated.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
        // new order: name, id, score
        assertThat(migrated.getString(0).toString()).isEqualTo("alice");
        assertThat(migrated.getInt(1)).isEqualTo(42);
        assertThat(migrated.isNullAt(2)).isTrue();
    }

    private static StateTtlConfig stateTtlConfig() {
        return StateTtlConfig.newBuilder(Duration.ofHours(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
    }

    private static ValueStateDescriptor<RowData> descriptor(
            String name, StateTtlConfig ttlConfig, RowType rowType) {
        ValueStateDescriptor<RowData> descriptor =
                new ValueStateDescriptor<>(name, RowDataSerializer.withFieldNames(rowType));
        if (ttlConfig.isEnabled()) {
            descriptor.enableTimeToLive(ttlConfig);
        }
        return descriptor;
    }

    private RowData migrateValueState(StateTtlConfig ttlConfig, RowData oldValue) throws Exception {
        return runValueStateUpgrade(
                ttlConfig,
                descriptor("state", ttlConfig, OLD_TYPE),
                descriptor("state", ttlConfig, NEW_TYPE),
                oldValue);
    }

    private RowData runValueStateUpgrade(
            StateTtlConfig ttlConfig,
            ValueStateDescriptor<RowData> oldDescriptor,
            ValueStateDescriptor<RowData> newDescriptor,
            RowData oldValue)
            throws Exception {
        SharedStateRegistry registry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend = createBackend(Collections.emptyList());
        KeyedStateHandle snapshot;
        try {
            ValueState<RowData> state = partitionedValueState(backend, oldDescriptor);
            backend.setCurrentKey(1);
            state.update(oldValue);
            snapshot = snapshot(backend, registry);
        } finally {
            backend.dispose();
        }

        backend = createBackend(Collections.singletonList(snapshot));
        try {
            ValueState<RowData> state = partitionedValueState(backend, newDescriptor);
            backend.setCurrentKey(1);
            return state.value();
        } finally {
            backend.dispose();
        }
    }

    private RowData migrateMapValue(StateTtlConfig ttlConfig, RowData oldValue) throws Exception {
        RowData uniqueKey = GenericRowData.of(7);
        RowType keyType = RowType.of(new IntType(false));

        SharedStateRegistry registry = new SharedStateRegistryImpl();
        CheckpointableKeyedStateBackend<Integer> backend = createBackend(Collections.emptyList());
        KeyedStateHandle snapshot;
        try {
            MapState<RowData, RowData> state =
                    partitionedMapState(backend, mapDescriptor(ttlConfig, keyType, OLD_TYPE));
            backend.setCurrentKey(1);
            state.put(uniqueKey, oldValue);
            snapshot = snapshot(backend, registry);
        } finally {
            backend.dispose();
        }

        backend = createBackend(Collections.singletonList(snapshot));
        try {
            MapState<RowData, RowData> state =
                    partitionedMapState(backend, mapDescriptor(ttlConfig, keyType, NEW_TYPE));
            backend.setCurrentKey(1);
            return state.get(uniqueKey);
        } finally {
            backend.dispose();
        }
    }

    private static MapStateDescriptor<RowData, RowData> mapDescriptor(
            StateTtlConfig ttlConfig, RowType keyType, RowType valueType) {
        MapStateDescriptor<RowData, RowData> descriptor =
                new MapStateDescriptor<>(
                        "state",
                        new RowDataSerializer(keyType),
                        RowDataSerializer.withFieldNames(valueType));
        if (ttlConfig.isEnabled()) {
            descriptor.enableTimeToLive(ttlConfig);
        }
        return descriptor;
    }

    private static ValueState<RowData> partitionedValueState(
            CheckpointableKeyedStateBackend<Integer> backend,
            ValueStateDescriptor<RowData> descriptor)
            throws Exception {
        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
    }

    private static MapState<RowData, RowData> partitionedMapState(
            CheckpointableKeyedStateBackend<Integer> backend,
            MapStateDescriptor<RowData, RowData> descriptor)
            throws Exception {
        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
    }

    private CheckpointableKeyedStateBackend<Integer> createBackend(
            List<KeyedStateHandle> restoreState) throws Exception {
        return new EmbeddedRocksDBStateBackend()
                .createKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                new JobID(),
                                "test_op",
                                IntSerializer.INSTANCE,
                                10,
                                new KeyGroupRange(0, 9),
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                env.getMetricGroup(),
                                restoreState,
                                new CloseableRegistry()));
    }

    private KeyedStateHandle snapshot(
            CheckpointableKeyedStateBackend<Integer> backend, SharedStateRegistry registry)
            throws Exception {
        CheckpointStreamFactory streamFactory =
                new JobManagerCheckpointStorage()
                        .createCheckpointStorage(new JobID())
                        .resolveCheckpointStorageLocation(
                                1L, CheckpointStorageLocationReference.getDefault());
        RunnableFuture<SnapshotResult<KeyedStateHandle>> future =
                backend.snapshot(
                        1L,
                        2L,
                        streamFactory,
                        CheckpointOptions.forCheckpointWithDefaultLocation());
        if (!future.isDone()) {
            future.run();
        }
        SnapshotResult<KeyedStateHandle> result = future.get();
        KeyedStateHandle handle = result.getJobManagerOwnedSnapshot();
        if (handle != null) {
            handle.registerSharedStates(registry, 0L);
        }
        return handle;
    }
}
