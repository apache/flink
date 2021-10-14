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
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for basic {@link CompletedCheckpointStore} contract. */
public class StandaloneCompletedCheckpointStoreTest extends CompletedCheckpointStoreTest {

    @Override
    protected CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain, Executor executor) throws Exception {
        return new StandaloneCompletedCheckpointStore(maxNumberOfCheckpointsToRetain);
    }

    /** Tests that shutdown discards all checkpoints. */
    @Test
    public void testShutdownDiscardsCheckpoints() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        CompletedCheckpointStore store = createRecoveredCompletedCheckpointStore(1);
        TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);
        Collection<OperatorState> operatorStates = checkpoint.getOperatorStates().values();

        store.addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {});
        assertEquals(1, store.getNumberOfRetainedCheckpoints());
        verifyCheckpointRegistered(operatorStates, sharedStateRegistry);

        store.shutdown(JobStatus.FINISHED, new CheckpointsCleaner());
        assertEquals(0, store.getNumberOfRetainedCheckpoints());
        assertTrue(checkpoint.isDiscarded());
        verifyCheckpointDiscarded(operatorStates);
    }

    /**
     * Tests that suspends discards all checkpoints (as they cannot be recovered later in standalone
     * recovery mode).
     */
    @Test
    public void testSuspendDiscardsCheckpoints() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        CompletedCheckpointStore store = createRecoveredCompletedCheckpointStore(1);
        TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);
        Collection<OperatorState> taskStates = checkpoint.getOperatorStates().values();

        store.addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {});
        assertEquals(1, store.getNumberOfRetainedCheckpoints());
        verifyCheckpointRegistered(taskStates, sharedStateRegistry);

        store.shutdown(JobStatus.SUSPENDED, new CheckpointsCleaner());
        assertEquals(0, store.getNumberOfRetainedCheckpoints());
        assertTrue(checkpoint.isDiscarded());
        verifyCheckpointDiscarded(taskStates);
    }

    /**
     * Tests that the checkpoint does not exist in the store when we fail to add it into the store
     * (i.e., there exists an exception thrown by the method).
     */
    @Test
    public void testAddCheckpointWithFailedRemove() throws Exception {

        final int numCheckpointsToRetain = 1;
        CompletedCheckpointStore store =
                createRecoveredCompletedCheckpointStore(
                        numCheckpointsToRetain, Executors.directExecutor());

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
                            new TestCompletedCheckpointStorageLocation()) {
                        @Override
                        public boolean discardOnSubsume() {
                            discardAttempted.countDown();
                            throw new RuntimeException();
                        }
                    };
            // should fail despite the exception
            store.addCheckpoint(checkpointToAdd, new CheckpointsCleaner(), () -> {});
        }
        discardAttempted.await();
    }
}
