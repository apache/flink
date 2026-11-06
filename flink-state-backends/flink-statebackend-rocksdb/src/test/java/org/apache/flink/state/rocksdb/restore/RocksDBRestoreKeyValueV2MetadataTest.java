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

package org.apache.flink.state.rocksdb.restore;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageAccess;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.v2.RegisteredKeyAndUserKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests restoring incremental checkpoints containing {@link
 * StateMetaInfoSnapshot.BackendStateType#KEY_VALUE_V2} state metadata into {@link
 * RocksDBKeyedStateBackend} across single-handle, temporary-DB rescale, and ingest-DB rescale
 * restore paths.
 */
class RocksDBRestoreKeyValueV2MetadataTest {

    private static final int NUM_KEY_GROUPS = 10;
    private static final KeyGroupRange FULL_RANGE = new KeyGroupRange(0, NUM_KEY_GROUPS - 1);
    private static final KeyGroupRange LEFT_RANGE = new KeyGroupRange(0, 4);
    private static final KeyGroupRange RIGHT_RANGE = new KeyGroupRange(5, NUM_KEY_GROUPS - 1);

    private static final String KEY_LEFT_1 = "8";
    private static final String KEY_LEFT_2 = "5";
    private static final String KEY_RIGHT_1 = "7";
    private static final String KEY_RIGHT_2 = "1";

    private static final ValueStateDescriptor<Integer> VALUE_DESC =
            new ValueStateDescriptor<>("valueState", IntSerializer.INSTANCE);
    private static final ListStateDescriptor<Integer> LIST_DESC =
            new ListStateDescriptor<>("listState", IntSerializer.INSTANCE);
    private static final MapStateDescriptor<Integer, String> MAP_DESC =
            new MapStateDescriptor<>("mapState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);
    private static final ValueStateDescriptor<Integer> UNACCESSED_VALUE_DESC =
            new ValueStateDescriptor<>("unaccessedState", IntSerializer.INSTANCE);

    enum RestoreMode {
        SINGLE_HANDLE(false, false),
        RESCALE_TEMP_DB(true, false),
        RESCALE_INGEST_DB(true, true);

        final boolean multiHandle;
        final boolean useIngestDbRestoreMode;

        RestoreMode(boolean multiHandle, boolean useIngestDbRestoreMode) {
            this.multiHandle = multiHandle;
            this.useIngestDbRestoreMode = useIngestDbRestoreMode;
        }
    }

    @TempDir private File tempDir;

    private SharedStateRegistry sharedStateRegistry;

    @BeforeEach
    void setUp() throws IOException {
        FileSystem.initialize(new Configuration(), null);
        sharedStateRegistry = new SharedStateRegistryImpl();
        assertThat(KeyGroupRangeAssignment.assignToKeyGroup(KEY_LEFT_1, NUM_KEY_GROUPS))
                .isBetween(LEFT_RANGE.getStartKeyGroup(), LEFT_RANGE.getEndKeyGroup());
        assertThat(KeyGroupRangeAssignment.assignToKeyGroup(KEY_LEFT_2, NUM_KEY_GROUPS))
                .isBetween(LEFT_RANGE.getStartKeyGroup(), LEFT_RANGE.getEndKeyGroup());
        assertThat(KeyGroupRangeAssignment.assignToKeyGroup(KEY_RIGHT_1, NUM_KEY_GROUPS))
                .isBetween(RIGHT_RANGE.getStartKeyGroup(), RIGHT_RANGE.getEndKeyGroup());
        assertThat(KeyGroupRangeAssignment.assignToKeyGroup(KEY_RIGHT_2, NUM_KEY_GROUPS))
                .isBetween(RIGHT_RANGE.getStartKeyGroup(), RIGHT_RANGE.getEndKeyGroup());
    }

    @ParameterizedTest
    @EnumSource(RestoreMode.class)
    void testRestoreKeyValueV2MetadataInRocksDB(RestoreMode mode) throws Exception {
        Collection<KeyedStateHandle> handlesWithV2Metadata =
                mode.multiHandle
                        ? Arrays.asList(
                                snapshotWithKeyValueV2Metadata(
                                        "source-left",
                                        LEFT_RANGE,
                                        Arrays.asList(KEY_LEFT_1, KEY_LEFT_2)),
                                snapshotWithKeyValueV2Metadata(
                                        "source-right",
                                        RIGHT_RANGE,
                                        Arrays.asList(KEY_RIGHT_1, KEY_RIGHT_2)))
                        : Collections.singletonList(
                                snapshotWithKeyValueV2Metadata(
                                        "source-single",
                                        FULL_RANGE,
                                        Arrays.asList(
                                                KEY_LEFT_1, KEY_LEFT_2, KEY_RIGHT_1, KEY_RIGHT_2)));

        List<String> allKeys = Arrays.asList(KEY_LEFT_1, KEY_LEFT_2, KEY_RIGHT_1, KEY_RIGHT_2);
        try (MockEnvironment restoreEnv = newEnvironment("rocks-restore");
                AbstractKeyedStateBackend<String> restoredBackend =
                        createRocksBackend(
                                restoreEnv,
                                FULL_RANGE,
                                handlesWithV2Metadata,
                                mode.useIngestDbRestoreMode)) {
            try {
                verifyAndMutateRestoredState(restoredBackend, allKeys);

                // Verify getKeys also works on an unaccessed restored KEY_VALUE_V2 state before
                // tryRegisterKvStateInformation re-registers it.
                try (Stream<String> unaccessedKeys =
                        restoredBackend.getKeys(UNACCESSED_VALUE_DESC.getName(), 1)) {
                    assertThat(unaccessedKeys.collect(Collectors.toSet()))
                            .containsExactlyInAnyOrderElementsOf(allKeys);
                }
            } finally {
                restoredBackend.dispose();
            }
        }
    }

    private KeyedStateHandle snapshotWithKeyValueV2Metadata(
            String name, KeyGroupRange range, List<String> keys) throws Exception {
        try (MockEnvironment env = newEnvironment(name);
                AbstractKeyedStateBackend<String> backend =
                        createRocksBackend(env, range, Collections.emptyList(), false)) {
            try {
                InternalValueState<String, Integer, Integer> valueState =
                        backend.createOrUpdateInternalState(IntSerializer.INSTANCE, VALUE_DESC);
                InternalListState<String, Integer, Integer> listState =
                        backend.createOrUpdateInternalState(IntSerializer.INSTANCE, LIST_DESC);
                InternalMapState<String, Integer, Integer, String> mapState =
                        backend.createOrUpdateInternalState(IntSerializer.INSTANCE, MAP_DESC);
                InternalValueState<String, Integer, Integer> unaccessedState =
                        backend.createOrUpdateInternalState(
                                IntSerializer.INSTANCE, UNACCESSED_VALUE_DESC);

                for (String key : keys) {
                    backend.setCurrentKey(key);
                    for (int ns : new int[] {1, 2}) {
                        int base = keySeed(key, ns);
                        valueState.setCurrentNamespace(ns);
                        listState.setCurrentNamespace(ns);
                        mapState.setCurrentNamespace(ns);
                        unaccessedState.setCurrentNamespace(ns);

                        valueState.update(base);
                        listState.update(Arrays.asList(base + 10, base + 20));
                        mapState.put(1, "val-" + key + "-" + ns);
                        mapState.put(2, "second-" + key + "-" + ns);
                        unaccessedState.update(base + 100);
                    }
                }

                IncrementalRemoteKeyedStateHandle rawHandle =
                        (IncrementalRemoteKeyedStateHandle) snapshotBackend(backend, env, 1L);
                return rewriteMetadataToKeyValueV2(rawHandle);
            } finally {
                backend.dispose();
            }
        }
    }

    private IncrementalRemoteKeyedStateHandle rewriteMetadataToKeyValueV2(
            IncrementalRemoteKeyedStateHandle handle) throws IOException {
        List<StateMetaInfoSnapshot> v2MetaSnapshots =
                Arrays.asList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        VALUE_DESC.getName(),
                                        org.apache.flink.api.common.state.v2.StateDescriptor.Type
                                                .VALUE,
                                        IntSerializer.INSTANCE,
                                        IntSerializer.INSTANCE)
                                .snapshot(),
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        LIST_DESC.getName(),
                                        org.apache.flink.api.common.state.v2.StateDescriptor.Type
                                                .LIST,
                                        IntSerializer.INSTANCE,
                                        IntSerializer.INSTANCE)
                                .snapshot(),
                        new RegisteredKeyAndUserKeyValueStateBackendMetaInfo<>(
                                        MAP_DESC.getName(),
                                        org.apache.flink.api.common.state.v2.StateDescriptor.Type
                                                .MAP,
                                        IntSerializer.INSTANCE,
                                        StringSerializer.INSTANCE,
                                        IntSerializer.INSTANCE)
                                .snapshot(),
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        UNACCESSED_VALUE_DESC.getName(),
                                        org.apache.flink.api.common.state.v2.StateDescriptor.Type
                                                .VALUE,
                                        IntSerializer.INSTANCE,
                                        IntSerializer.INSTANCE)
                                .snapshot());

        KeyedBackendSerializationProxy<String> proxy =
                new KeyedBackendSerializationProxy<>(
                        StringSerializer.INSTANCE, v2MetaSnapshots, true);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        proxy.write(new DataOutputViewStreamWrapper(out));
        ByteStreamStateHandle v2MetaHandle =
                new ByteStreamStateHandle("v2-metadata", out.toByteArray());

        IncrementalRemoteKeyedStateHandle rewritten =
                new IncrementalRemoteKeyedStateHandle(
                        handle.getBackendIdentifier(),
                        handle.getKeyGroupRange(),
                        handle.getCheckpointId(),
                        handle.getSharedState(),
                        handle.getPrivateState(),
                        v2MetaHandle,
                        handle.getCheckpointedSize());
        rewritten.registerSharedStates(sharedStateRegistry, handle.getCheckpointId());
        return rewritten;
    }

    private void verifyAndMutateRestoredState(
            AbstractKeyedStateBackend<String> rocksBackend, List<String> keys) throws Exception {
        InternalMapState<String, Integer, Integer, String> mapState =
                rocksBackend.createOrUpdateInternalState(IntSerializer.INSTANCE, MAP_DESC);
        InternalValueState<String, Integer, Integer> valueState =
                rocksBackend.createOrUpdateInternalState(IntSerializer.INSTANCE, VALUE_DESC);
        InternalListState<String, Integer, Integer> listState =
                rocksBackend.createOrUpdateInternalState(IntSerializer.INSTANCE, LIST_DESC);

        for (String key : keys) {
            rocksBackend.setCurrentKey(key);
            for (int ns : new int[] {1, 2}) {
                int base = keySeed(key, ns);
                valueState.setCurrentNamespace(ns);
                listState.setCurrentNamespace(ns);
                mapState.setCurrentNamespace(ns);

                assertThat(valueState.value()).isEqualTo(base);
                assertThat(listState.get()).containsExactly(base + 10, base + 20);
                assertThat(mapState.contains(1)).isTrue();
                assertThat(mapState.get(1)).isEqualTo("val-" + key + "-" + ns);
                assertThat(mapState.contains(2)).isTrue();
                assertThat(mapState.get(2)).isEqualTo("second-" + key + "-" + ns);

                Map<Integer, String> entries = new HashMap<>();
                for (Map.Entry<Integer, String> entry : mapState.entries()) {
                    entries.put(entry.getKey(), entry.getValue());
                }
                Map<Integer, String> expected = new HashMap<>();
                expected.put(1, "val-" + key + "-" + ns);
                expected.put(2, "second-" + key + "-" + ns);
                assertThat(entries).isEqualTo(expected);
            }
        }
    }

    private static int keySeed(String key, int namespace) {
        return Integer.parseInt(key) * 100 + namespace;
    }

    private MockEnvironment newEnvironment(String subDirName) throws IOException {
        File dir = new File(tempDir, subDirName);
        assertThat(dir.mkdirs()).isTrue();
        MockEnvironment env =
                MockEnvironment.builder()
                        .setUserCodeClassLoader(getClass().getClassLoader())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(new Configuration(), dir))
                        .build();
        env.setCheckpointStorageAccess(
                new FsCheckpointStorageAccess(
                        new Path(new File(dir, "checkpoints").getPath()),
                        null,
                        env.getJobID(),
                        1024,
                        4096));
        return env;
    }

    private AbstractKeyedStateBackend<String> createRocksBackend(
            MockEnvironment env,
            KeyGroupRange range,
            Collection<KeyedStateHandle> stateHandles,
            boolean useIngestDbRestoreMode)
            throws IOException {
        Configuration config = new Configuration();
        config.set(RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE, useIngestDbRestoreMode);
        EmbeddedRocksDBStateBackend rocksStateBackend =
                new EmbeddedRocksDBStateBackend(true)
                        .configure(config, getClass().getClassLoader());
        return rocksStateBackend.createKeyedStateBackend(
                new KeyedStateBackendParametersImpl<>(
                        env,
                        env.getJobID(),
                        "test_op",
                        StringSerializer.INSTANCE,
                        NUM_KEY_GROUPS,
                        range,
                        env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        (name, value) -> {},
                        stateHandles,
                        new CloseableRegistry(),
                        1.0));
    }

    private KeyedStateHandle snapshotBackend(
            AbstractKeyedStateBackend<String> backend, MockEnvironment env, long checkpointId)
            throws Exception {
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                backend.snapshot(
                        checkpointId,
                        System.currentTimeMillis(),
                        env.getCheckpointStorageAccess()
                                .resolveCheckpointStorageLocation(
                                        checkpointId,
                                        CheckpointStorageLocationReference.getDefault()),
                        CheckpointOptions.forCheckpointWithDefaultLocation());
        if (!snapshot.isDone()) {
            snapshot.run();
        }
        SnapshotResult<KeyedStateHandle> result = snapshot.get();
        KeyedStateHandle handle = result.getJobManagerOwnedSnapshot();
        if (handle != null) {
            handle.registerSharedStates(sharedStateRegistry, checkpointId);
        }
        return handle;
    }
}
