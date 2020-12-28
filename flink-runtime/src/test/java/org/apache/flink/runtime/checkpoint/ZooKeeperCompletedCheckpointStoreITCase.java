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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.clock.ManualClock;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.checkpoint.CheckpointRequestDeciderTest.regularCheckpoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for basic {@link CompletedCheckpointStore} contract and ZooKeeper state handling. */
public class ZooKeeperCompletedCheckpointStoreITCase extends CompletedCheckpointStoreTest {

    private static final ZooKeeperTestEnvironment ZOOKEEPER = new ZooKeeperTestEnvironment(1);

    private static final String CHECKPOINT_PATH = "/checkpoints";

    private static final ZooKeeperCheckpointStoreUtil checkpointStoreUtil =
            ZooKeeperCheckpointStoreUtil.INSTANCE;

    @AfterClass
    public static void tearDown() throws Exception {
        if (ZOOKEEPER != null) {
            ZOOKEEPER.shutdown();
        }
    }

    @Before
    public void cleanUp() throws Exception {
        ZOOKEEPER.deleteAll();
    }

    @Override
    protected CompletedCheckpointStore createCompletedCheckpoints(
            int maxNumberOfCheckpointsToRetain, Executor executor) throws Exception {
        final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper =
                ZooKeeperUtils.createZooKeeperStateHandleStore(
                        ZOOKEEPER.getClient(),
                        CHECKPOINT_PATH,
                        new TestingRetrievableStateStorageHelper<>());

        return new DefaultCompletedCheckpointStore<>(
                maxNumberOfCheckpointsToRetain,
                checkpointsInZooKeeper,
                checkpointStoreUtil,
                executor);
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Tests that older checkpoints are not cleaned up right away when recovering. Only after
     * another checkpoint has been completed the old checkpoints exceeding the number of checkpoints
     * to retain will be removed.
     */
    @Test
    public void testRecover() throws Exception {

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        CompletedCheckpointStore checkpoints = createCompletedCheckpoints(3);

        TestCompletedCheckpoint[] expected =
                new TestCompletedCheckpoint[] {
                    createCheckpoint(0, sharedStateRegistry),
                    createCheckpoint(1, sharedStateRegistry),
                    createCheckpoint(2, sharedStateRegistry)
                };

        // Add multiple checkpoints
        checkpoints.addCheckpoint(expected[0], new CheckpointsCleaner(), () -> {});
        checkpoints.addCheckpoint(expected[1], new CheckpointsCleaner(), () -> {});
        checkpoints.addCheckpoint(expected[2], new CheckpointsCleaner(), () -> {});

        verifyCheckpointRegistered(expected[0].getOperatorStates().values(), sharedStateRegistry);
        verifyCheckpointRegistered(expected[1].getOperatorStates().values(), sharedStateRegistry);
        verifyCheckpointRegistered(expected[2].getOperatorStates().values(), sharedStateRegistry);

        // All three should be in ZK
        assertEquals(3, ZOOKEEPER.getClient().getChildren().forPath(CHECKPOINT_PATH).size());
        assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());

        // Recover
        sharedStateRegistry.close();
        sharedStateRegistry = new SharedStateRegistry();
        checkpoints.recover();

        assertEquals(3, ZOOKEEPER.getClient().getChildren().forPath(CHECKPOINT_PATH).size());
        assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());
        assertEquals(expected[2], checkpoints.getLatestCheckpoint(false));

        List<CompletedCheckpoint> expectedCheckpoints = new ArrayList<>(3);
        expectedCheckpoints.add(expected[1]);
        expectedCheckpoints.add(expected[2]);
        expectedCheckpoints.add(createCheckpoint(3, sharedStateRegistry));

        checkpoints.addCheckpoint(expectedCheckpoints.get(2), new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> actualCheckpoints = checkpoints.getAllCheckpoints();

        assertEquals(expectedCheckpoints, actualCheckpoints);

        for (CompletedCheckpoint actualCheckpoint : actualCheckpoints) {
            verifyCheckpointRegistered(
                    actualCheckpoint.getOperatorStates().values(), sharedStateRegistry);
        }
    }

    /** Tests that shutdown discards all checkpoints. */
    @Test
    public void testShutdownDiscardsCheckpoints() throws Exception {
        CuratorFramework client = ZOOKEEPER.getClient();

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        CompletedCheckpointStore store = createCompletedCheckpoints(1);
        TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);

        store.addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {});
        assertEquals(1, store.getNumberOfRetainedCheckpoints());
        assertNotNull(
                client.checkExists()
                        .forPath(
                                CHECKPOINT_PATH
                                        + checkpointStoreUtil.checkpointIDToName(
                                                checkpoint.getCheckpointID())));

        store.shutdown(JobStatus.FINISHED, new CheckpointsCleaner(), () -> {});
        assertEquals(0, store.getNumberOfRetainedCheckpoints());
        assertNull(
                client.checkExists()
                        .forPath(
                                CHECKPOINT_PATH
                                        + checkpointStoreUtil.checkpointIDToName(
                                                checkpoint.getCheckpointID())));

        sharedStateRegistry.close();
        store.recover();

        assertEquals(0, store.getNumberOfRetainedCheckpoints());
    }

    /**
     * Tests that suspends keeps all checkpoints (so that they can be recovered later by the
     * ZooKeeper store). Furthermore, suspending a job should release all locks.
     */
    @Test
    public void testSuspendKeepsCheckpoints() throws Exception {
        CuratorFramework client = ZOOKEEPER.getClient();

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        CompletedCheckpointStore store = createCompletedCheckpoints(1);
        TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);

        store.addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {});
        assertEquals(1, store.getNumberOfRetainedCheckpoints());
        assertNotNull(
                client.checkExists()
                        .forPath(
                                CHECKPOINT_PATH
                                        + checkpointStoreUtil.checkpointIDToName(
                                                checkpoint.getCheckpointID())));

        store.shutdown(JobStatus.SUSPENDED, new CheckpointsCleaner(), () -> {});

        assertEquals(0, store.getNumberOfRetainedCheckpoints());

        final String checkpointPath =
                CHECKPOINT_PATH
                        + checkpointStoreUtil.checkpointIDToName(checkpoint.getCheckpointID());
        Stat stat = client.checkExists().forPath(checkpointPath);

        assertNotNull("The checkpoint node should exist.", stat);
        assertEquals("The checkpoint node should not be locked.", 0, stat.getNumChildren());

        // Recover again
        sharedStateRegistry.close();
        store.recover();

        CompletedCheckpoint recovered = store.getLatestCheckpoint(false);
        assertEquals(checkpoint, recovered);
    }

    /**
     * FLINK-6284.
     *
     * <p>Tests that the latest recovered checkpoint is the one with the highest checkpoint id
     */
    @Test
    public void testLatestCheckpointRecovery() throws Exception {
        final int numCheckpoints = 3;
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        CompletedCheckpointStore checkpointStore = createCompletedCheckpoints(numCheckpoints);
        List<CompletedCheckpoint> checkpoints = new ArrayList<>(numCheckpoints);

        checkpoints.add(createCheckpoint(9, sharedStateRegistry));
        checkpoints.add(createCheckpoint(10, sharedStateRegistry));
        checkpoints.add(createCheckpoint(11, sharedStateRegistry));

        for (CompletedCheckpoint checkpoint : checkpoints) {
            checkpointStore.addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {});
        }

        sharedStateRegistry.close();
        checkpointStore.recover();

        CompletedCheckpoint latestCheckpoint = checkpointStore.getLatestCheckpoint(false);

        assertEquals(checkpoints.get(checkpoints.size() - 1), latestCheckpoint);
    }

    /**
     * FLINK-6612
     *
     * <p>Checks that a concurrent checkpoint completion won't discard a checkpoint which has been
     * recovered by a different completed checkpoint store.
     */
    @Test
    public void testConcurrentCheckpointOperations() throws Exception {
        final int numberOfCheckpoints = 1;
        final long waitingTimeout = 50L;

        CompletedCheckpointStore zkCheckpointStore1 =
                createCompletedCheckpoints(numberOfCheckpoints);
        CompletedCheckpointStore zkCheckpointStore2 =
                createCompletedCheckpoints(numberOfCheckpoints);

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

        TestCompletedCheckpoint completedCheckpoint = createCheckpoint(1, sharedStateRegistry);

        // complete the first checkpoint
        zkCheckpointStore1.addCheckpoint(completedCheckpoint, new CheckpointsCleaner(), () -> {});

        // recover the checkpoint by a different checkpoint store
        sharedStateRegistry.close();
        sharedStateRegistry = new SharedStateRegistry();
        zkCheckpointStore2.recover();

        CompletedCheckpoint recoveredCheckpoint = zkCheckpointStore2.getLatestCheckpoint(false);
        assertTrue(recoveredCheckpoint instanceof TestCompletedCheckpoint);
        TestCompletedCheckpoint recoveredTestCheckpoint =
                (TestCompletedCheckpoint) recoveredCheckpoint;

        // Check that the recovered checkpoint is not yet discarded
        assertFalse(recoveredTestCheckpoint.isDiscarded());

        // complete another checkpoint --> this should remove the first checkpoint from the store
        // because the number of retained checkpoints == 1
        TestCompletedCheckpoint completedCheckpoint2 = createCheckpoint(2, sharedStateRegistry);
        zkCheckpointStore1.addCheckpoint(completedCheckpoint2, new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> allCheckpoints = zkCheckpointStore1.getAllCheckpoints();

        // check that we have removed the first checkpoint from zkCompletedStore1
        assertEquals(Collections.singletonList(completedCheckpoint2), allCheckpoints);

        // lets wait a little bit to see that no discard operation will be executed
        assertFalse(
                "The checkpoint should not have been discarded.",
                recoveredTestCheckpoint.awaitDiscard(waitingTimeout));

        // check that we have not discarded the first completed checkpoint
        assertFalse(recoveredTestCheckpoint.isDiscarded());

        TestCompletedCheckpoint completedCheckpoint3 = createCheckpoint(3, sharedStateRegistry);

        // this should release the last lock on completedCheckpoint and thus discard it
        zkCheckpointStore2.addCheckpoint(completedCheckpoint3, new CheckpointsCleaner(), () -> {});

        // the checkpoint should be discarded eventually because there is no lock on it anymore
        recoveredTestCheckpoint.awaitDiscard();
    }

    /**
     * FLINK-17073 tests that there is no request triggered when there are too many checkpoints
     * waiting to clean and that it resumes when the number of waiting checkpoints as gone below the
     * threshold.
     */
    @Test
    public void testChekpointingPausesAndResumeWhenTooManyCheckpoints() throws Exception {
        ManualClock clock = new ManualClock();
        clock.advanceTime(1, TimeUnit.DAYS);
        int maxCleaningCheckpoints = 1;
        CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
        CheckpointRequestDecider checkpointRequestDecider =
                new CheckpointRequestDecider(
                        maxCleaningCheckpoints,
                        unused -> {},
                        clock,
                        1,
                        new AtomicInteger(0)::get,
                        checkpointsCleaner::getNumberOfCheckpointsToClean);

        final int maxCheckpointsToRetain = 1;
        ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
        CompletedCheckpointStore checkpointStore =
                createCompletedCheckpoints(maxCheckpointsToRetain, executor);

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
            checkpointStore.addCheckpoint(completedCheckpoint, checkpointsCleaner, () -> {});
        }

        int nbCheckpointsSubmittedForCleaning = nbCheckpointsToInject - maxCheckpointsToRetain;
        // wait for cleaning request submission by checkpointsStore
        CommonTestUtils.waitUntilCondition(
                () ->
                        checkpointsCleaner.getNumberOfCheckpointsToClean()
                                == nbCheckpointsSubmittedForCleaning,
                Deadline.fromNow(Duration.ofSeconds(3)));
        assertEquals(
                nbCheckpointsSubmittedForCleaning,
                checkpointsCleaner.getNumberOfCheckpointsToClean());
        // checkpointing is on hold because checkpointsCleaner.getNumberOfCheckpointsToClean() >
        // maxCleaningCheckpoints
        assertFalse(
                checkpointRequestDecider
                        .chooseRequestToExecute(regularCheckpoint(), false, 0)
                        .isPresent());

        // make the executor execute checkpoint requests.
        executor.triggerAll();
        // wait for a checkpoint to be cleaned
        CommonTestUtils.waitUntilCondition(
                () ->
                        checkpointsCleaner.getNumberOfCheckpointsToClean()
                                < nbCheckpointsSubmittedForCleaning,
                Deadline.fromNow(Duration.ofSeconds(3)));
        // some checkpoints were cleaned
        assertTrue(
                checkpointsCleaner.getNumberOfCheckpointsToClean()
                        < nbCheckpointsSubmittedForCleaning);
        // checkpointing is resumed because checkpointsCleaner.getNumberOfCheckpointsToClean() <=
        // maxCleaningCheckpoints
        assertTrue(
                checkpointRequestDecider
                        .chooseRequestToExecute(regularCheckpoint(), false, 0)
                        .isPresent());
        checkpointStore.shutdown(JobStatus.FINISHED, checkpointsCleaner, () -> {});
    }

    static class HeapRetrievableStateHandle<T extends Serializable>
            implements RetrievableStateHandle<T> {

        private static final long serialVersionUID = -268548467968932L;

        private static AtomicInteger nextKey = new AtomicInteger(0);

        private static HashMap<Integer, Object> stateMap = new HashMap<>();

        private final int key;

        public HeapRetrievableStateHandle(T state) {
            key = nextKey.getAndIncrement();
            stateMap.put(key, state);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T retrieveState() {
            return (T) stateMap.get(key);
        }

        @Override
        public void discardState() throws Exception {
            stateMap.remove(key);
        }

        @Override
        public long getStateSize() {
            return 0;
        }
    }
}
