/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.EmbeddedCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Tests for the {@link SchedulerUtils} utilities. */
public class SchedulerUtilsTest extends TestLogger {

    @Test
    public void testSettingMaxNumberOfCheckpointsToRetain() throws Exception {

        final int maxNumberOfCheckpointsToRetain = 10;
        final Configuration jobManagerConfig = new Configuration();
        jobManagerConfig.setInteger(
                CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, maxNumberOfCheckpointsToRetain);

        final CompletedCheckpointStore completedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStore(
                        jobManagerConfig,
                        new StandaloneCheckpointRecoveryFactory(),
                        Executors.directExecutor(),
                        log,
                        new JobID());

        assertEquals(
                maxNumberOfCheckpointsToRetain,
                completedCheckpointStore.getMaxNumberOfRetainedCheckpoints());
    }

    /**
     * Check that a {@link SharedStateRegistryFactory} used by {@link SchedulerUtils} registers
     * shared checkpoint state on restore.
     */
    @Test
    public void testSharedStateRegistration() throws Exception {
        UUID backendId = UUID.randomUUID();
        StateHandleID key = new StateHandleID("k0");
        StreamStateHandle handle = new ByteStreamStateHandle("h0", new byte[] {1, 2, 3});
        CheckpointRecoveryFactory recoveryFactory =
                buildRecoveryFactory(
                        buildCheckpoint(buildIncrementalHandle(key, handle, backendId)));

        CompletedCheckpointStore checkpointStore =
                SchedulerUtils.createCompletedCheckpointStore(
                        new Configuration(),
                        recoveryFactory,
                        Executors.directExecutor(),
                        log,
                        new JobID());

        SharedStateRegistry sharedStateRegistry = checkpointStore.getSharedStateRegistry();

        IncrementalRemoteKeyedStateHandle newHandle =
                buildIncrementalHandle(key, new PlaceholderStreamStateHandle(1L), backendId);
        newHandle.registerSharedStates(sharedStateRegistry, 1L);

        assertSame(handle, newHandle.getSharedState().get(key));
    }

    private CheckpointRecoveryFactory buildRecoveryFactory(CompletedCheckpoint checkpoint) {
        return new CheckpointRecoveryFactory() {
            @Override
            public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
                    JobID jobId,
                    int maxNumberOfCheckpointsToRetain,
                    SharedStateRegistryFactory sharedStateRegistryFactory,
                    Executor ioExecutor) {
                List<CompletedCheckpoint> checkpoints = singletonList(checkpoint);
                return new EmbeddedCompletedCheckpointStore(
                        maxNumberOfCheckpointsToRetain,
                        checkpoints,
                        sharedStateRegistryFactory.create(ioExecutor, checkpoints));
            }

            @Override
            public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) {
                return new StandaloneCheckpointIDCounter();
            }
        };
    }

    private CompletedCheckpoint buildCheckpoint(KeyedStateHandle incremental) {
        OperatorID operatorID = new OperatorID();
        OperatorState operatorState = new OperatorState(operatorID, 1, 1);
        operatorState.putState(
                0, OperatorSubtaskState.builder().setManagedKeyedState(incremental).build());
        return new CompletedCheckpoint(
                new JobID(),
                1,
                1,
                1,
                singletonMap(operatorID, operatorState),
                emptyList(),
                CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION),
                new TestCompletedCheckpointStorageLocation(),
                null);
    }

    private IncrementalRemoteKeyedStateHandle buildIncrementalHandle(
            StateHandleID key, StreamStateHandle shared, UUID backendIdentifier) {
        StreamStateHandle meta = new ByteStreamStateHandle("meta", new byte[] {1, 2, 3});
        Map<StateHandleID, StreamStateHandle> sharedStateMap = new HashMap<>();
        sharedStateMap.put(key, shared);
        return new IncrementalRemoteKeyedStateHandle(
                backendIdentifier,
                KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                1,
                sharedStateMap,
                emptyMap(),
                meta);
    }
}
