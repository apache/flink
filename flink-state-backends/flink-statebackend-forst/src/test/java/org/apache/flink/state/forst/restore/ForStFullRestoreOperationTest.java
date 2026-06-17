/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.restore;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageAccess;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.state.forst.ForStOptions;
import org.apache.flink.state.forst.ForStStateBackend;
import org.apache.flink.state.forst.ForStStateBackendConfigTest;
import org.apache.flink.state.forst.sync.ForStSyncKeyedStateBackend;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ForStFullRestoreOperation}, which handles restoring a ForSt state backend from
 * a full (canonical) savepoint produced by another backend (e.g., HashMapStateBackend).
 *
 * <p>ForSt's own snapshot strategies always produce {@link
 * org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle}. The full restore path is only
 * reached when restoring from a canonical savepoint that contains a {@link
 * org.apache.flink.runtime.state.KeyGroupsStateHandle}, typically produced by the heap backend.
 */
class ForStFullRestoreOperationTest {

    @TempDir private File tempFolder;

    private MockEnvironment env;
    private CheckpointStreamFactory streamFactory;

    @BeforeEach
    void setup() throws Exception {
        FileSystem.initialize(new Configuration(), null);

        env =
                MockEnvironment.builder()
                        .setUserCodeClassLoader(
                                ForStStateBackendConfigTest.class.getClassLoader())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(new Configuration(), tempFolder))
                        .build();

        CheckpointStorageAccess storageAccess =
                new FsCheckpointStorageAccess(
                        new Path(tempFolder.getPath(), "checkpoints"),
                        null,
                        env.getJobID(),
                        1024,
                        4096);
        env.setCheckpointStorageAccess(storageAccess);

        streamFactory =
                storageAccess.resolveCheckpointStorageLocation(
                        1L, CheckpointStorageLocationReference.getDefault());
    }

    /**
     * Writes state into a HashMapStateBackend (produces KeyGroupsStateHandle), then restores it
     * into a ForSt sync backend configured with PriorityQueueStateType.ForStDB. This exercises
     * ForStFullRestoreOperation end-to-end.
     */
    @Test
    void testRestoreValueStateFromFullSnapshot() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        // --- write state using heap backend (produces KeyGroupsStateHandle) ---
        HashMapStateBackend heapBackend = new HashMapStateBackend();
        org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<Integer> heapKeyed =
                heapBackend.createKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                new JobID(),
                                "test_op",
                                IntSerializer.INSTANCE,
                                10,
                                new KeyGroupRange(0, 9),
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                (name, value) -> {},
                                Collections.emptyList(),
                                new CloseableRegistry(),
                                1.0d));

        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("test-value", StringSerializer.INSTANCE);
        ValueState<String> state =
                heapKeyed.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        heapKeyed.setCurrentKey(1);
        state.update("hello");
        heapKeyed.setCurrentKey(2);
        state.update("world");
        heapKeyed.setCurrentKey(3);
        state.update("flink");

        RunnableFuture<org.apache.flink.runtime.state.SnapshotResult<KeyedStateHandle>> future =
                heapKeyed.snapshot(
                        1L, 1L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
        if (!future.isDone()) {
            future.run();
        }
        org.apache.flink.runtime.state.SnapshotResult<KeyedStateHandle> snapshotResult =
                future.get();
        KeyedStateHandle handle = snapshotResult.getJobManagerOwnedSnapshot();
        assertThat(handle).isInstanceOf(org.apache.flink.runtime.state.KeyGroupsStateHandle.class);
        handle.registerSharedStates(sharedStateRegistry, 1L);
        heapKeyed.dispose();

        // --- restore into ForSt sync backend with ForStDB timer type (hits
        // ForStFullRestoreOperation) ---
        ForStStateBackend forStBackend = new ForStStateBackend();
        Configuration config = new Configuration();
        config.set(ForStOptions.PRIMARY_DIRECTORY, tempFolder.toURI().toString());
        config.set(
                ForStOptions.TIMER_SERVICE_FACTORY,
                ForStStateBackend.PriorityQueueStateType.ForStDB);
        forStBackend =
                forStBackend.configure(config, Thread.currentThread().getContextClassLoader());

        env.setCheckpointStorageAccess(
                new JobManagerCheckpointStorage().createCheckpointStorage(new JobID()));

        ForStSyncKeyedStateBackend<Integer> forStKeyed =
                (ForStSyncKeyedStateBackend<Integer>)
                        forStBackend.createKeyedStateBackend(
                                new KeyedStateBackendParametersImpl<>(
                                        env,
                                        new JobID(),
                                        "test_op",
                                        IntSerializer.INSTANCE,
                                        10,
                                        new KeyGroupRange(0, 9),
                                        env.getTaskKvStateRegistry(),
                                        TtlTimeProvider.DEFAULT,
                                        new UnregisteredMetricsGroup(),
                                        (name, value) -> {},
                                        Collections.singletonList(handle),
                                        new CloseableRegistry(),
                                        1.0d));

        ValueState<String> restoredState =
                forStKeyed.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        forStKeyed.setCurrentKey(1);
        assertThat(restoredState.value()).isEqualTo("hello");
        forStKeyed.setCurrentKey(2);
        assertThat(restoredState.value()).isEqualTo("world");
        forStKeyed.setCurrentKey(3);
        assertThat(restoredState.value()).isEqualTo("flink");

        forStKeyed.dispose();
        handle.discardState();
    }

    /**
     * Verifies that state spanning multiple key groups is correctly distributed and readable after
     * restoring via ForStFullRestoreOperation.
     */
    @Test
    void testRestoreAcrossMultipleKeyGroups() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        HashMapStateBackend heapBackend = new HashMapStateBackend();
        org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<Integer> heapKeyed =
                heapBackend.createKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                new JobID(),
                                "test_op",
                                IntSerializer.INSTANCE,
                                10,
                                new KeyGroupRange(0, 9),
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                (name, value) -> {},
                                Collections.emptyList(),
                                new CloseableRegistry(),
                                1.0d));

        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("counter", IntSerializer.INSTANCE);
        ValueState<Integer> state =
                heapKeyed.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        // write keys that spread across multiple key groups
        for (int key = 0; key < 10; key++) {
            heapKeyed.setCurrentKey(key);
            state.update(key * 100);
        }

        RunnableFuture<org.apache.flink.runtime.state.SnapshotResult<KeyedStateHandle>> future =
                heapKeyed.snapshot(
                        1L, 1L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
        if (!future.isDone()) {
            future.run();
        }
        KeyedStateHandle handle = future.get().getJobManagerOwnedSnapshot();
        assertThat(handle).isInstanceOf(org.apache.flink.runtime.state.KeyGroupsStateHandle.class);
        handle.registerSharedStates(sharedStateRegistry, 1L);
        heapKeyed.dispose();

        ForStStateBackend forStBackend = new ForStStateBackend();
        Configuration config = new Configuration();
        config.set(ForStOptions.PRIMARY_DIRECTORY, tempFolder.toURI().toString());
        config.set(
                ForStOptions.TIMER_SERVICE_FACTORY,
                ForStStateBackend.PriorityQueueStateType.ForStDB);
        forStBackend =
                forStBackend.configure(config, Thread.currentThread().getContextClassLoader());

        env.setCheckpointStorageAccess(
                new JobManagerCheckpointStorage().createCheckpointStorage(new JobID()));

        ForStSyncKeyedStateBackend<Integer> forStKeyed =
                (ForStSyncKeyedStateBackend<Integer>)
                        forStBackend.createKeyedStateBackend(
                                new KeyedStateBackendParametersImpl<>(
                                        env,
                                        new JobID(),
                                        "test_op",
                                        IntSerializer.INSTANCE,
                                        10,
                                        new KeyGroupRange(0, 9),
                                        env.getTaskKvStateRegistry(),
                                        TtlTimeProvider.DEFAULT,
                                        new UnregisteredMetricsGroup(),
                                        (name, value) -> {},
                                        Collections.singletonList(handle),
                                        new CloseableRegistry(),
                                        1.0d));

        ValueState<Integer> restoredState =
                forStKeyed.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        for (int key = 0; key < 10; key++) {
            forStKeyed.setCurrentKey(key);
            assertThat(restoredState.value()).isEqualTo(key * 100);
        }

        forStKeyed.dispose();
        handle.discardState();
    }
}
