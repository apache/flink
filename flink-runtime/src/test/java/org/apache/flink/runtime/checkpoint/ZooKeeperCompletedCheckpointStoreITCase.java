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
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.checkpoint.CheckpointRequestDeciderTest.regularCheckpoint;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for basic {@link CompletedCheckpointStore} contract and ZooKeeper state handling. */
class ZooKeeperCompletedCheckpointStoreITCase extends CompletedCheckpointStoreTest {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private CuratorFramework getZooKeeperClient() {
        return zooKeeperExtension.getZooKeeperClient(
                testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
    }

    private static final String CHECKPOINT_PATH = "/checkpoints";

    private static final ZooKeeperCheckpointStoreUtil checkpointStoreUtil =
            ZooKeeperCheckpointStoreUtil.INSTANCE;

    @Override
    protected CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain, Executor executor) throws Exception {
        final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper =
                ZooKeeperUtils.createZooKeeperStateHandleStore(
                        getZooKeeperClient(),
                        CHECKPOINT_PATH,
                        new TestingRetrievableStateStorageHelper<>());
        return new DefaultCompletedCheckpointStore<>(
                maxNumberOfCheckpointsToRetain,
                checkpointsInZooKeeper,
                checkpointStoreUtil,
                DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(
                        checkpointsInZooKeeper, checkpointStoreUtil),
                SharedStateRegistry.DEFAULT_FACTORY.create(
                        Executors.directExecutor(), emptyList(), RestoreMode.DEFAULT),
                executor);
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Tests that older checkpoints are not cleaned up right away when recovering. Only after
     * another checkpoint has been completed the old checkpoints exceeding the number of checkpoints
     * to retain will be removed.
     */
    @Test
    void testRecover() throws Exception {

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore checkpoints = createRecoveredCompletedCheckpointStore(3);

        TestCompletedCheckpoint[] expected =
                new TestCompletedCheckpoint[] {
                    createCheckpoint(0, sharedStateRegistry),
                    createCheckpoint(1, sharedStateRegistry),
                    createCheckpoint(2, sharedStateRegistry)
                };

        // Add multiple checkpoints
        checkpoints.addCheckpointAndSubsumeOldestOne(
                expected[0], new CheckpointsCleaner(), () -> {});
        checkpoints.addCheckpointAndSubsumeOldestOne(
                expected[1], new CheckpointsCleaner(), () -> {});
        checkpoints.addCheckpointAndSubsumeOldestOne(
                expected[2], new CheckpointsCleaner(), () -> {});

        verifyCheckpointRegistered(expected[0].getOperatorStates().values());
        verifyCheckpointRegistered(expected[1].getOperatorStates().values());
        verifyCheckpointRegistered(expected[2].getOperatorStates().values());

        // All three should be in ZK
        assertThat(getZooKeeperClient().getChildren().forPath(CHECKPOINT_PATH)).hasSize(3);
        assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isEqualTo(3);

        // Recover
        sharedStateRegistry.close();
        sharedStateRegistry = new SharedStateRegistryImpl();

        assertThat(getZooKeeperClient().getChildren().forPath(CHECKPOINT_PATH)).hasSize(3);
        assertThat(checkpoints.getNumberOfRetainedCheckpoints()).isEqualTo(3);
        assertThat(checkpoints.getLatestCheckpoint()).isEqualTo(expected[2]);

        List<CompletedCheckpoint> expectedCheckpoints = new ArrayList<>(3);
        expectedCheckpoints.add(expected[1]);
        expectedCheckpoints.add(expected[2]);
        expectedCheckpoints.add(createCheckpoint(3, sharedStateRegistry));

        checkpoints.addCheckpointAndSubsumeOldestOne(
                expectedCheckpoints.get(2), new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> actualCheckpoints = checkpoints.getAllCheckpoints();

        assertThat(actualCheckpoints).isEqualTo(expectedCheckpoints);

        for (CompletedCheckpoint actualCheckpoint : actualCheckpoints) {
            verifyCheckpointRegistered(actualCheckpoint.getOperatorStates().values());
        }
    }

    /** Tests that shutdown discards all checkpoints. */
    @Test
    void testShutdownDiscardsCheckpoints() throws Exception {
        CuratorFramework client = getZooKeeperClient();

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore store = createRecoveredCompletedCheckpointStore(1);
        TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);

        store.addCheckpointAndSubsumeOldestOne(checkpoint, new CheckpointsCleaner(), () -> {});
        assertThat(store.getNumberOfRetainedCheckpoints()).isOne();
        assertThat(
                        client.checkExists()
                                .forPath(
                                        CHECKPOINT_PATH
                                                + checkpointStoreUtil.checkpointIDToName(
                                                        checkpoint.getCheckpointID())))
                .isNotNull();

        store.shutdown(JobStatus.FINISHED, new CheckpointsCleaner());
        assertThat(store.getNumberOfRetainedCheckpoints()).isZero();
        assertThat(
                        client.checkExists()
                                .forPath(
                                        CHECKPOINT_PATH
                                                + checkpointStoreUtil.checkpointIDToName(
                                                        checkpoint.getCheckpointID())))
                .isNull();

        sharedStateRegistry.close();

        assertThat(createRecoveredCompletedCheckpointStore(1).getNumberOfRetainedCheckpoints())
                .isZero();
    }

    /**
     * Tests that suspends keeps all checkpoints (so that they can be recovered later by the
     * ZooKeeper store). Furthermore, suspending a job should release all locks.
     */
    @Test
    void testSuspendKeepsCheckpoints() throws Exception {
        CuratorFramework client = getZooKeeperClient();

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore store = createRecoveredCompletedCheckpointStore(1);
        TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);

        store.addCheckpointAndSubsumeOldestOne(checkpoint, new CheckpointsCleaner(), () -> {});
        assertThat(store.getNumberOfRetainedCheckpoints()).isOne();
        assertThat(
                        client.checkExists()
                                .forPath(
                                        CHECKPOINT_PATH
                                                + checkpointStoreUtil.checkpointIDToName(
                                                        checkpoint.getCheckpointID())))
                .isNotNull();

        store.shutdown(JobStatus.SUSPENDED, new CheckpointsCleaner());

        assertThat(store.getNumberOfRetainedCheckpoints()).isZero();

        final String checkpointPath =
                CHECKPOINT_PATH
                        + checkpointStoreUtil.checkpointIDToName(checkpoint.getCheckpointID());
        final List<String> checkpointPathChildren = client.getChildren().forPath(checkpointPath);
        assertThat(checkpointPathChildren)
                .as("The checkpoint node should not be marked for deletion.")
                .hasSize(1);

        final String locksNodeName = Iterables.getOnlyElement(checkpointPathChildren);
        final String locksNodePath =
                ZooKeeperUtils.generateZookeeperPath(checkpointPath, locksNodeName);

        final Stat locksStat = client.checkExists().forPath(locksNodePath);
        assertThat(locksStat.getNumChildren())
                .as("There shouldn't be any lock node available for the checkpoint")
                .isZero();

        // Recover again
        sharedStateRegistry.close();
        store = createRecoveredCompletedCheckpointStore(1);

        CompletedCheckpoint recovered = store.getLatestCheckpoint();
        assertThat(recovered).isEqualTo(checkpoint);
    }

    /**
     * FLINK-6284.
     *
     * <p>Tests that the latest recovered checkpoint is the one with the highest checkpoint id
     */
    @Test
    void testLatestCheckpointRecovery() throws Exception {
        final int numCheckpoints = 3;
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore checkpointStore =
                createRecoveredCompletedCheckpointStore(numCheckpoints);
        List<CompletedCheckpoint> checkpoints = new ArrayList<>(numCheckpoints);

        checkpoints.add(createCheckpoint(9, sharedStateRegistry));
        checkpoints.add(createCheckpoint(10, sharedStateRegistry));
        checkpoints.add(createCheckpoint(11, sharedStateRegistry));

        for (CompletedCheckpoint checkpoint : checkpoints) {
            checkpointStore.addCheckpointAndSubsumeOldestOne(
                    checkpoint, new CheckpointsCleaner(), () -> {});
        }

        sharedStateRegistry.close();

        final CompletedCheckpoint latestCheckpoint =
                createRecoveredCompletedCheckpointStore(numCheckpoints).getLatestCheckpoint();
        assertThat(latestCheckpoint).isEqualTo(checkpoints.get(checkpoints.size() - 1));
    }

    /**
     * FLINK-6612
     *
     * <p>Checks that a concurrent checkpoint completion won't discard a checkpoint which has been
     * recovered by a different completed checkpoint store.
     */
    @Test
    void testConcurrentCheckpointOperations() throws Exception {
        final int numberOfCheckpoints = 1;
        final long waitingTimeout = 50L;

        final CompletedCheckpointStore zkCheckpointStore1 =
                createRecoveredCompletedCheckpointStore(numberOfCheckpoints);

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        TestCompletedCheckpoint completedCheckpoint = createCheckpoint(1, sharedStateRegistry);

        // complete the first checkpoint
        zkCheckpointStore1.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint, new CheckpointsCleaner(), () -> {});

        // recover the checkpoint by a different checkpoint store
        sharedStateRegistry.close();
        sharedStateRegistry = new SharedStateRegistryImpl();
        final CompletedCheckpointStore zkCheckpointStore2 =
                createRecoveredCompletedCheckpointStore(numberOfCheckpoints);

        CompletedCheckpoint recoveredCheckpoint = zkCheckpointStore2.getLatestCheckpoint();
        assertThat(recoveredCheckpoint).isInstanceOf(TestCompletedCheckpoint.class);
        TestCompletedCheckpoint recoveredTestCheckpoint =
                (TestCompletedCheckpoint) recoveredCheckpoint;

        // Check that the recovered checkpoint is not yet discarded
        assertThat(recoveredTestCheckpoint.isDiscarded()).isFalse();

        // complete another checkpoint --> this should remove the first checkpoint from the store
        // because the number of retained checkpoints == 1
        TestCompletedCheckpoint completedCheckpoint2 = createCheckpoint(2, sharedStateRegistry);
        zkCheckpointStore1.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint2, new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> allCheckpoints = zkCheckpointStore1.getAllCheckpoints();

        // check that we have removed the first checkpoint from zkCompletedStore1
        assertThat(allCheckpoints).isEqualTo(Collections.singletonList(completedCheckpoint2));

        // lets wait a little bit to see that no discard operation will be executed
        assertThat(recoveredTestCheckpoint.awaitDiscard(waitingTimeout))
                .as("The checkpoint should not have been discarded.")
                .isFalse();

        // check that we have not discarded the first completed checkpoint
        assertThat(recoveredTestCheckpoint.isDiscarded()).isFalse();

        TestCompletedCheckpoint completedCheckpoint3 = createCheckpoint(3, sharedStateRegistry);

        // this should release the last lock on completedCheckpoint and thus discard it
        zkCheckpointStore2.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint3, new CheckpointsCleaner(), () -> {});

        // the checkpoint should be discarded eventually because there is no lock on it anymore
        recoveredTestCheckpoint.awaitDiscard();
    }

    /**
     * FLINK-17073 tests that there is no request triggered when there are too many checkpoints
     * waiting to clean and that it resumes when the number of waiting checkpoints as gone below the
     * threshold.
     */
    @Test
    void testChekpointingPausesAndResumeWhenTooManyCheckpoints() throws Exception {
        ManualClock clock = new ManualClock();
        clock.advanceTime(1, TimeUnit.DAYS);
        int maxCleaningCheckpoints = 1;
        CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
        CheckpointRequestDecider checkpointRequestDecider =
                new CheckpointRequestDecider(
                        maxCleaningCheckpoints,
                        (currentTimeMillis, tillNextMillis) -> {},
                        clock,
                        1,
                        new AtomicInteger(0)::get,
                        checkpointsCleaner::getNumberOfCheckpointsToClean);

        final int maxCheckpointsToRetain = 1;
        ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
        CompletedCheckpointStore checkpointStore =
                createRecoveredCompletedCheckpointStore(maxCheckpointsToRetain, executor);

        int nbCheckpointsToInject = 3;
        for (int i = 1; i <= nbCheckpointsToInject; i++) {
            // add checkpoints to clean, the ManuallyTriggeredScheduledExecutor.execute() just
            // queues the runnables but does not execute them.
            TestCompletedCheckpoint completedCheckpoint =
                    new TestCompletedCheckpoint(
                            new JobID(),
                            i,
                            i,
                            Collections.emptyMap(),
                            CheckpointProperties.forCheckpoint(
                                    CheckpointRetentionPolicy.RETAIN_ON_FAILURE));
            checkpointStore.addCheckpointAndSubsumeOldestOne(
                    completedCheckpoint, checkpointsCleaner, () -> {});
        }

        int nbCheckpointsSubmittedForCleaning = nbCheckpointsToInject - maxCheckpointsToRetain;
        // wait for cleaning request submission by checkpointsStore
        CommonTestUtils.waitUntilCondition(
                () ->
                        checkpointsCleaner.getNumberOfCheckpointsToClean()
                                == nbCheckpointsSubmittedForCleaning);
        assertThat(checkpointsCleaner.getNumberOfCheckpointsToClean())
                .isEqualTo(nbCheckpointsSubmittedForCleaning);
        // checkpointing is on hold because checkpointsCleaner.getNumberOfCheckpointsToClean() >
        // maxCleaningCheckpoints
        assertThat(checkpointRequestDecider.chooseRequestToExecute(regularCheckpoint(), false, 0))
                .isNotPresent();

        // make the executor execute checkpoint requests.
        executor.triggerAll();
        // wait for a checkpoint to be cleaned
        CommonTestUtils.waitUntilCondition(
                () ->
                        checkpointsCleaner.getNumberOfCheckpointsToClean()
                                < nbCheckpointsSubmittedForCleaning);
        // some checkpoints were cleaned
        assertThat(checkpointsCleaner.getNumberOfCheckpointsToClean())
                .isLessThan(nbCheckpointsSubmittedForCleaning);
        // checkpointing is resumed because checkpointsCleaner.getNumberOfCheckpointsToClean() <=
        // maxCleaningCheckpoints
        assertThat(checkpointRequestDecider.chooseRequestToExecute(regularCheckpoint(), false, 0))
                .isPresent();
        checkpointStore.shutdown(JobStatus.FINISHED, checkpointsCleaner);
    }
}
