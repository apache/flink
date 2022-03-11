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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.changelog.fs.ChangelogStorageMetricGroup;
import org.apache.flink.changelog.fs.FsStateChangelogStorage;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.Executors;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.state.StateBackendTestBase.runSnapshot;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test Utilities for Changelog StateBackend. */
public class ChangelogStateBackendTestUtils {

    public static <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            StateBackend stateBackend,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        return createKeyedBackend(
                stateBackend, keySerializer, numberOfKeyGroups, keyGroupRange, null, env);
    }

    public static <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            StateBackend stateBackend,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            KeyedStateHandle state,
            Environment env)
            throws Exception {

        return stateBackend.createKeyedStateBackend(
                env,
                new JobID(),
                "test_op",
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                env.getTaskKvStateRegistry(),
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                state == null ? Collections.emptyList() : Collections.singletonList(state),
                new CloseableRegistry());
    }

    public static CheckpointableKeyedStateBackend<Integer> createKeyedBackend(
            StateBackend stateBackend, Environment env) throws Exception {

        return createKeyedBackend(
                stateBackend, IntSerializer.INSTANCE, 10, new KeyGroupRange(0, 9), env);
    }

    private static CheckpointableKeyedStateBackend<Integer> restoreKeyedBackend(
            StateBackend stateBackend, KeyedStateHandle state, Environment env) throws Exception {

        return createKeyedBackend(
                stateBackend, IntSerializer.INSTANCE, 10, new KeyGroupRange(0, 9), state, env);
    }

    public static TestTaskStateManager createTaskStateManager(File changelogStoragePath)
            throws IOException {
        return TestTaskStateManager.builder()
                .setStateChangelogStorage(
                        new FsStateChangelogStorage(
                                Path.fromLocalFile(changelogStoragePath),
                                false,
                                1024,
                                new ChangelogStorageMetricGroup(
                                        UnregisteredMetricGroups
                                                .createUnregisteredTaskManagerJobMetricGroup())))
                .build();
    }

    public static void testMaterializedRestore(
            StateBackend stateBackend,
            StateTtlConfig stateTtlConfig,
            Environment env,
            CheckpointStreamFactory streamFactory)
            throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        TypeInformation<StateBackendTestBase.TestPojo> pojoType =
                new GenericTypeInfo<>(StateBackendTestBase.TestPojo.class);
        ValueStateDescriptor<StateBackendTestBase.TestPojo> kvId =
                new ValueStateDescriptor<>("id", pojoType);
        if (stateTtlConfig.isEnabled()) {
            kvId.enableTimeToLive(stateTtlConfig);
        }

        ChangelogKeyedStateBackend<Integer> keyedBackend =
                (ChangelogKeyedStateBackend<Integer>) createKeyedBackend(stateBackend, env);

        CompletableFuture<Void> asyncComplete = new CompletableFuture<>();
        PeriodicMaterializationManager periodicMaterializationManager =
                periodicMaterializationManager(keyedBackend, asyncComplete);

        try {
            ValueState<StateBackendTestBase.TestPojo> state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            keyedBackend.setCurrentKey(1);
            state.update(new StateBackendTestBase.TestPojo("u1", 1));

            keyedBackend.setCurrentKey(2);
            state.update(new StateBackendTestBase.TestPojo("u2", 2));

            materialize(keyedBackend, periodicMaterializationManager);

            keyedBackend.setCurrentKey(2);
            state.update(new StateBackendTestBase.TestPojo("u2", 22));

            keyedBackend.setCurrentKey(3);
            state.update(new StateBackendTestBase.TestPojo("u3", 3));

            materialize(keyedBackend, periodicMaterializationManager);

            keyedBackend.setCurrentKey(4);
            state.update(new StateBackendTestBase.TestPojo("u4", 4));

            keyedBackend.setCurrentKey(2);
            state.update(new StateBackendTestBase.TestPojo("u2", 222));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            keyedBackend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();

            // make sure the asycn phase completes successfully
            if (asyncComplete.isCompletedExceptionally()) {
                asyncComplete.get();
            }

            // ============================ restore snapshot ===============================

            env.getExecutionConfig().registerKryoType(StateBackendTestBase.TestPojo.class);

            keyedBackend =
                    (ChangelogKeyedStateBackend<Integer>)
                            restoreKeyedBackend(stateBackend, snapshot, env);
            snapshot.discardState();

            state =
                    keyedBackend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            keyedBackend.setCurrentKey(1);
            assertEquals(new StateBackendTestBase.TestPojo("u1", 1), state.value());

            keyedBackend.setCurrentKey(2);
            assertEquals(new StateBackendTestBase.TestPojo("u2", 222), state.value());

            keyedBackend.setCurrentKey(3);
            assertEquals(new StateBackendTestBase.TestPojo("u3", 3), state.value());
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    /**
     * Explicitly trigger materialization. Materialization is expected to complete before returning
     * from this method by the use of direct executor when constructing materializer.
     * Automatic/periodic triggering is disabled by NOT starting the periodicMaterializationManager.
     *
     * <p>Additionally, verify changelog truncation happened upon completion.
     */
    private static void materialize(
            ChangelogKeyedStateBackend<Integer> keyedBackend,
            PeriodicMaterializationManager periodicMaterializationManager) {
        StateChangelogWriter<? extends ChangelogStateHandle> writer =
                keyedBackend.getChangelogWriter();
        SequenceNumber sqn = writer.nextSequenceNumber();
        periodicMaterializationManager.triggerMaterialization();
        assertTrue(
                "Materialization didn't truncate the changelog",
                sqn.compareTo(writer.getLowestSequenceNumber()) <= 0);
    }

    public static void testMaterializedRestoreForPriorityQueue(
            StateBackend stateBackend, Environment env, CheckpointStreamFactory streamFactory)
            throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        String fieldName = "key-grouped-priority-queue";
        ChangelogKeyedStateBackend<Integer> keyedBackend =
                (ChangelogKeyedStateBackend<Integer>) createKeyedBackend(stateBackend, env);

        CompletableFuture<Void> asyncComplete = new CompletableFuture<>();
        PeriodicMaterializationManager periodicMaterializationManager =
                periodicMaterializationManager(keyedBackend, asyncComplete);

        try {
            KeyGroupedInternalPriorityQueue<TestType> priorityQueue =
                    keyedBackend.create(fieldName, new TestType.V1TestTypeSerializer());

            TestType elementA100 = new TestType("a", 100);
            TestType elementA10 = new TestType("a", 10);
            TestType elementA20 = new TestType("a", 20);

            assertTrue(priorityQueue.add(elementA100));
            assertTrue(priorityQueue.add(elementA10));
            assertFalse(priorityQueue.add(elementA20));
            assertFalse(priorityQueue.add(elementA10));

            List<TestType> actualList = new ArrayList<>();
            try (CloseableIterator<TestType> iterator = priorityQueue.iterator()) {
                iterator.forEachRemaining(actualList::add);
            }

            assertThat(actualList, containsInAnyOrder(elementA100, elementA10, elementA20));

            materialize(keyedBackend, periodicMaterializationManager);

            TestType elementB9 = new TestType("b", 9);
            assertTrue(priorityQueue.add(elementB9));

            materialize(keyedBackend, periodicMaterializationManager);

            TestType elementC9 = new TestType("c", 9);
            TestType elementC8 = new TestType("c", 8);
            assertFalse(priorityQueue.add(elementC9));
            assertTrue(priorityQueue.add(elementC8));

            KeyedStateHandle snapshot =
                    runSnapshot(
                            keyedBackend.snapshot(
                                    682375462378L,
                                    2,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();

            // make sure the asycn phase completes successfully
            if (asyncComplete.isCompletedExceptionally()) {
                asyncComplete.get();
            }

            // ============================ restore snapshot ===============================

            keyedBackend =
                    (ChangelogKeyedStateBackend<Integer>)
                            restoreKeyedBackend(stateBackend, snapshot, env);
            snapshot.discardState();

            KeyGroupedInternalPriorityQueue<TestType> priorityQueueRestored =
                    keyedBackend.create(fieldName, new TestType.V1TestTypeSerializer());

            List<TestType> actualListRestore = new ArrayList<>();
            try (CloseableIterator<TestType> iterator = priorityQueueRestored.iterator()) {
                iterator.forEachRemaining(actualListRestore::add);
            }

            assertThat(
                    actualListRestore,
                    containsInAnyOrder(
                            elementA100, elementA10, elementA20, elementB9, elementC9, elementC8));

            assertFalse(priorityQueueRestored.add(new TestType("d", 11)));
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
        }
    }

    private static PeriodicMaterializationManager periodicMaterializationManager(
            ChangelogKeyedStateBackend<Integer> keyedBackend,
            CompletableFuture<Void> asyncComplete) {
        return new PeriodicMaterializationManager(
                new SyncMailboxExecutor(),
                Executors.newDirectExecutorService(),
                "testTask",
                (message, exception) -> asyncComplete.completeExceptionally(exception),
                keyedBackend,
                10,
                1,
                "testTask");
    }

    /** Dummy {@link CheckpointStorageAccess}. */
    public static class DummyCheckpointingStorageAccess implements CheckpointStorageAccess {

        DummyCheckpointingStorageAccess() {}

        @Override
        public boolean supportsHighlyAvailableStorage() {
            return false;
        }

        @Override
        public boolean hasDefaultSavepointLocation() {
            return false;
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) {
            throw new UnsupportedOperationException(
                    "Checkpoints are not supported in a single key state backend");
        }

        @Override
        public void initializeBaseLocationsForCheckpoint() {}

        @Override
        public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) {
            throw new UnsupportedOperationException(
                    "Checkpoints are not supported in a single key state backend");
        }

        @Override
        public CheckpointStorageLocation initializeLocationForSavepoint(
                long checkpointId, @Nullable String externalLocationPointer) {
            throw new UnsupportedOperationException(
                    "Checkpoints are not supported in a single key state backend");
        }

        @Override
        public CheckpointStreamFactory resolveCheckpointStorageLocation(
                long checkpointId, CheckpointStorageLocationReference reference) {
            throw new UnsupportedOperationException(
                    "Checkpoints are not supported in a single key state backend");
        }

        @Override
        public CheckpointStateOutputStream createTaskOwnedStateStream() {
            throw new UnsupportedOperationException(
                    "Checkpoints are not supported in a single key state backend");
        }

        @Override
        public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
            throw new UnsupportedOperationException(
                    "Checkpoints are not supported in a single key state backend");
        }
    }
}
