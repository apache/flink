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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultCompletedCheckpointStore} with {@link ZooKeeperStateHandleStore}. */
class ZooKeeperCompletedCheckpointStoreTest {

    @RegisterExtension
    public static AllCallbackWrapper<ZooKeeperExtension> zooKeeperExtensionWrapper =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final ZooKeeperCheckpointStoreUtil zooKeeperCheckpointStoreUtil =
            ZooKeeperCheckpointStoreUtil.INSTANCE;

    @Test
    void testPathConversion() {
        final long checkpointId = 42L;

        final String path = zooKeeperCheckpointStoreUtil.checkpointIDToName(checkpointId);

        assertThat(zooKeeperCheckpointStoreUtil.nameToCheckpointID(path)).isEqualTo(checkpointId);
    }

    @Test
    void testRecoverFailsIfDownloadFails() {
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());
        final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> checkpointsInZk =
                new ArrayList<>();
        try (CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration, NoOpFatalErrorHandler.INSTANCE)) {
            final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper =
                    new ZooKeeperStateHandleStore<CompletedCheckpoint>(
                            curatorFrameworkWrapper.asCuratorFramework(),
                            new TestingRetrievableStateStorageHelper<>()) {
                        @Override
                        public List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>>
                                getAllAndLock() {
                            return checkpointsInZk;
                        }
                    };

            checkpointsInZk.add(
                    createHandle(
                            1,
                            id -> {
                                throw new ExpectedTestException();
                            }));
            assertThatThrownBy(
                            () ->
                                    DefaultCompletedCheckpointStoreUtils
                                            .retrieveCompletedCheckpoints(
                                                    checkpointsInZooKeeper,
                                                    zooKeeperCheckpointStoreUtil))
                    .satisfies(FlinkAssertions.anyCauseMatches(ExpectedTestException.class));
        }
    }

    private Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> createHandle(
            long id, Function<Long, CompletedCheckpoint> checkpointSupplier) {
        return Tuple2.of(
                new CheckpointStateHandle(checkpointSupplier, id),
                zooKeeperCheckpointStoreUtil.checkpointIDToName(id));
    }

    /** Tests that subsumed checkpoints are discarded. */
    @Test
    void testDiscardingSubsumedCheckpoints() throws Exception {
        final SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());

        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
        final CompletedCheckpointStore checkpointStore =
                createZooKeeperCheckpointStore(curatorFrameworkWrapper.asCuratorFramework());

        try {
            final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint1 =
                    CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);

            checkpointStore.addCheckpointAndSubsumeOldestOne(
                    checkpoint1, new CheckpointsCleaner(), () -> {});
            assertThat(checkpointStore.getAllCheckpoints()).containsExactly(checkpoint1);

            final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint2 =
                    CompletedCheckpointStoreTest.createCheckpoint(1, sharedStateRegistry);
            checkpointStore.addCheckpointAndSubsumeOldestOne(
                    checkpoint2, new CheckpointsCleaner(), () -> {});
            final List<CompletedCheckpoint> allCheckpoints = checkpointStore.getAllCheckpoints();
            assertThat(allCheckpoints).containsExactly(checkpoint2);

            // verify that the subsumed checkpoint is discarded
            CompletedCheckpointStoreTest.verifyCheckpointDiscarded(checkpoint1);
        } finally {
            curatorFrameworkWrapper.close();
        }
    }

    /**
     * Tests that checkpoints are discarded when the completed checkpoint store is shut down with a
     * globally terminal state.
     */
    @Test
    void testDiscardingCheckpointsAtShutDown() throws Exception {
        final SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());

        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
        final CompletedCheckpointStore checkpointStore =
                createZooKeeperCheckpointStore(curatorFrameworkWrapper.asCuratorFramework());

        try {
            final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint1 =
                    CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);

            checkpointStore.addCheckpointAndSubsumeOldestOne(
                    checkpoint1, new CheckpointsCleaner(), () -> {});
            assertThat(checkpointStore.getAllCheckpoints()).containsExactly(checkpoint1);

            checkpointStore.shutdown(JobStatus.FINISHED, new CheckpointsCleaner());

            // verify that the checkpoint is discarded
            CompletedCheckpointStoreTest.verifyCheckpointDiscarded(checkpoint1);
        } finally {
            curatorFrameworkWrapper.close();
        }
    }

    @Nonnull
    private CompletedCheckpointStore createZooKeeperCheckpointStore(CuratorFramework client)
            throws Exception {
        final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper =
                ZooKeeperUtils.createZooKeeperStateHandleStore(
                        client, "/checkpoints", new TestingRetrievableStateStorageHelper<>());
        return new DefaultCompletedCheckpointStore<>(
                1,
                checkpointsInZooKeeper,
                zooKeeperCheckpointStoreUtil,
                Collections.emptyList(),
                SharedStateRegistry.DEFAULT_FACTORY.create(
                        Executors.directExecutor(), emptyList(), RestoreMode.DEFAULT),
                Executors.directExecutor());
    }

    private static class CheckpointStateHandle
            implements RetrievableStateHandle<CompletedCheckpoint> {
        private static final long serialVersionUID = 1L;
        private final Function<Long, CompletedCheckpoint> checkpointSupplier;
        private final long id;

        CheckpointStateHandle(Function<Long, CompletedCheckpoint> checkpointSupplier, long id) {
            this.checkpointSupplier = checkpointSupplier;
            this.id = id;
        }

        @Override
        public CompletedCheckpoint retrieveState() {
            return checkpointSupplier.apply(id);
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return 0;
        }
    }

    /**
     * Tests that the checkpoint does not exist in the store when we fail to add it into the store
     * (i.e., there exists an exception thrown by the method).
     */
    @Test
    void testAddCheckpointWithFailedRemove() throws Exception {

        final int numCheckpointsToRetain = 1;
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());

        try (final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration, NoOpFatalErrorHandler.INSTANCE)) {
            final CompletedCheckpointStore store =
                    createZooKeeperCheckpointStore(curatorFrameworkWrapper.asCuratorFramework());

            CountDownLatch discardAttempted = new CountDownLatch(1);
            for (long i = 0; i < numCheckpointsToRetain + 1; ++i) {
                CompletedCheckpoint checkpointToAdd =
                        new CompletedCheckpoint(
                                new JobID(),
                                i,
                                i,
                                i,
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION),
                                new TestCompletedCheckpointStorageLocation(),
                                null);
                // shouldn't fail despite the exception
                store.addCheckpointAndSubsumeOldestOne(
                        checkpointToAdd,
                        new CheckpointsCleaner(),
                        () -> {
                            discardAttempted.countDown();
                            throw new RuntimeException();
                        });
            }
            discardAttempted.await();
        }
    }
}
