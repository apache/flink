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
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

import static java.util.Collections.emptyList;
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
                        new JobID(),
                        RestoreMode.CLAIM);

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
        String localPath = "k0";
        StreamStateHandle handle = new ByteStreamStateHandle("h0", new byte[] {1, 2, 3});
        CheckpointRecoveryFactory recoveryFactory =
                buildRecoveryFactory(
                        buildCheckpoint(buildIncrementalHandle(localPath, handle, backendId)));

        CompletedCheckpointStore checkpointStore =
                SchedulerUtils.createCompletedCheckpointStore(
                        new Configuration(),
                        recoveryFactory,
                        Executors.directExecutor(),
                        log,
                        new JobID(),
                        RestoreMode.CLAIM);

        SharedStateRegistry sharedStateRegistry = checkpointStore.getSharedStateRegistry();

        IncrementalRemoteKeyedStateHandle newHandle =
                buildIncrementalHandle(
                        localPath,
                        new PlaceholderStreamStateHandle(
                                handle.getStreamStateHandleID(), handle.getStateSize()),
                        backendId);
        newHandle.registerSharedStates(sharedStateRegistry, 1L);

        assertSame(
                handle,
                newHandle.getSharedState().stream()
                        .filter(e -> e.getLocalPath().equals(localPath))
                        .findFirst()
                        .get()
                        .getHandle());
    }

    private CheckpointRecoveryFactory buildRecoveryFactory(CompletedCheckpoint checkpoint) {
        return new CheckpointRecoveryFactory() {
            @Override
            public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
                    JobID jobId,
                    int maxNumberOfCheckpointsToRetain,
                    SharedStateRegistryFactory sharedStateRegistryFactory,
                    Executor ioExecutor,
                    RestoreMode restoreMode) {
                List<CompletedCheckpoint> checkpoints = singletonList(checkpoint);
                return new EmbeddedCompletedCheckpointStore(
                        maxNumberOfCheckpointsToRetain,
                        checkpoints,
                        sharedStateRegistryFactory.create(
                                ioExecutor, checkpoints, RestoreMode.DEFAULT));
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
            String localPath, StreamStateHandle shared, UUID backendIdentifier) {
        StreamStateHandle meta = new ByteStreamStateHandle("meta", new byte[] {1, 2, 3});
        List<HandleAndLocalPath> sharedState = new ArrayList<>(1);
        sharedState.add(HandleAndLocalPath.of(shared, localPath));
        return new IncrementalRemoteKeyedStateHandle(
                backendIdentifier,
                KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                1,
                sharedState,
                emptyList(),
                meta);
    }
}
