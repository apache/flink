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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Tests for {@link DefaultCompletedCheckpointStore} with {@link ZooKeeperStateHandleStore}. */
public class ZooKeeperCompletedCheckpointStoreTest extends TestLogger {

    @ClassRule public static ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

    private static final ZooKeeperCheckpointStoreUtil zooKeeperCheckpointStoreUtil =
            ZooKeeperCheckpointStoreUtil.INSTANCE;

    @Test
    public void testPathConversion() {
        final long checkpointId = 42L;

        final String path = zooKeeperCheckpointStoreUtil.checkpointIDToName(checkpointId);

        assertEquals(checkpointId, zooKeeperCheckpointStoreUtil.nameToCheckpointID(path));
    }

    @Test
    public void testRecoverFailsIfDownloadFails() {
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
        final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> checkpointsInZk =
                new ArrayList<>();
        final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper =
                new ZooKeeperStateHandleStore<CompletedCheckpoint>(
                        ZooKeeperUtils.startCuratorFramework(
                                        configuration, NoOpFatalErrorHandler.INSTANCE)
                                .asCuratorFramework(),
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
        final Exception exception =
                assertThrows(
                        Exception.class,
                        () ->
                                DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(
                                        checkpointsInZooKeeper, zooKeeperCheckpointStoreUtil));
        assertThat(exception, FlinkMatchers.containsCause(ExpectedTestException.class));
    }

    private Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> createHandle(
            long id, Function<Long, CompletedCheckpoint> checkpointSupplier) {
        return Tuple2.of(
                new CheckpointStateHandle(checkpointSupplier, id),
                zooKeeperCheckpointStoreUtil.checkpointIDToName(id));
    }

    /** Tests that subsumed checkpoints are discarded. */
    @Test
    public void testDiscardingSubsumedCheckpoints() throws Exception {
        final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());

        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
        final CompletedCheckpointStore checkpointStore =
                createZooKeeperCheckpointStore(curatorFrameworkWrapper.asCuratorFramework());

        try {
            final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint1 =
                    CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);

            checkpointStore.addCheckpoint(checkpoint1, new CheckpointsCleaner(), () -> {});
            assertThat(checkpointStore.getAllCheckpoints(), Matchers.contains(checkpoint1));

            final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint2 =
                    CompletedCheckpointStoreTest.createCheckpoint(1, sharedStateRegistry);
            checkpointStore.addCheckpoint(checkpoint2, new CheckpointsCleaner(), () -> {});
            final List<CompletedCheckpoint> allCheckpoints = checkpointStore.getAllCheckpoints();
            assertThat(allCheckpoints, Matchers.contains(checkpoint2));
            assertThat(allCheckpoints, Matchers.not(Matchers.contains(checkpoint1)));

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
    public void testDiscardingCheckpointsAtShutDown() throws Exception {
        final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());

        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
        final CompletedCheckpointStore checkpointStore =
                createZooKeeperCheckpointStore(curatorFrameworkWrapper.asCuratorFramework());

        try {
            final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint1 =
                    CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);

            checkpointStore.addCheckpoint(checkpoint1, new CheckpointsCleaner(), () -> {});
            assertThat(checkpointStore.getAllCheckpoints(), Matchers.contains(checkpoint1));

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
    public void testAddCheckpointWithFailedRemove() throws Exception {

        final int numCheckpointsToRetain = 1;
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());

        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);

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
                            new TestCompletedCheckpointStorageLocation());
            // shouldn't fail despite the exception
            store.addCheckpoint(
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
