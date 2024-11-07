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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphCheckpointPlanCalculatorContext;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.StringSerializer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the user-defined hooks that the checkpoint coordinator can call. */
class CheckpointCoordinatorMasterHooksTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    // ------------------------------------------------------------------------
    //  hook registration
    // ------------------------------------------------------------------------

    /** This method tests that hooks with the same identifier are not registered multiple times. */
    @Test
    void testDeduplicateOnRegister() throws Exception {
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());
        final CheckpointCoordinator cc = instantiateCheckpointCoordinator(graph);

        MasterTriggerRestoreHook<?> hook1 = mock(MasterTriggerRestoreHook.class);
        when(hook1.getIdentifier()).thenReturn("test id");

        MasterTriggerRestoreHook<?> hook2 = mock(MasterTriggerRestoreHook.class);
        when(hook2.getIdentifier()).thenReturn("test id");

        MasterTriggerRestoreHook<?> hook3 = mock(MasterTriggerRestoreHook.class);
        when(hook3.getIdentifier()).thenReturn("anotherId");

        assertThat(cc.addMasterHook(hook1)).isTrue();
        assertThat(cc.addMasterHook(hook2)).isFalse();
        assertThat(cc.addMasterHook(hook3)).isTrue();
    }

    /** Test that validates correct exceptions when supplying hooks with invalid IDs. */
    @Test
    void testNullOrInvalidId() throws Exception {
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());
        final CheckpointCoordinator cc = instantiateCheckpointCoordinator(graph);

        try {
            cc.addMasterHook(null);
            fail("expected an exception");
        } catch (NullPointerException ignored) {
        }

        try {
            cc.addMasterHook(mock(MasterTriggerRestoreHook.class));
            fail("expected an exception");
        } catch (IllegalArgumentException ignored) {
        }

        try {
            MasterTriggerRestoreHook<?> hook = mock(MasterTriggerRestoreHook.class);
            when(hook.getIdentifier()).thenReturn("        ");

            cc.addMasterHook(hook);
            fail("expected an exception");
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    void testHookReset() throws Exception {
        final String id1 = "id1";
        final String id2 = "id2";

        final MasterTriggerRestoreHook<String> hook1 = mockGeneric(MasterTriggerRestoreHook.class);
        when(hook1.getIdentifier()).thenReturn(id1);
        final MasterTriggerRestoreHook<String> hook2 = mockGeneric(MasterTriggerRestoreHook.class);
        when(hook2.getIdentifier()).thenReturn(id2);

        // create the checkpoint coordinator
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());
        CheckpointCoordinator cc = instantiateCheckpointCoordinator(graph);

        cc.addMasterHook(hook1);
        cc.addMasterHook(hook2);

        // initialize the hooks
        cc.restoreLatestCheckpointedStateToAll(Collections.emptySet(), false);
        verify(hook1, times(1)).reset();
        verify(hook2, times(1)).reset();

        // shutdown
        cc.shutdown();
        verify(hook1, times(1)).close();
        verify(hook2, times(1)).close();
    }

    // ------------------------------------------------------------------------
    //  trigger / restore behavior
    // ------------------------------------------------------------------------

    @Test
    void testHooksAreCalledOnTrigger() throws Exception {
        final String id1 = "id1";
        final String id2 = "id2";

        final String state1 = "the-test-string-state";
        final byte[] state1serialized = new StringSerializer().serialize(state1);

        final long state2 = 987654321L;
        final byte[] state2serialized = new LongSerializer().serialize(state2);

        final MasterTriggerRestoreHook<String> statefulHook1 =
                mockGeneric(MasterTriggerRestoreHook.class);
        when(statefulHook1.getIdentifier()).thenReturn(id1);
        when(statefulHook1.createCheckpointDataSerializer()).thenReturn(new StringSerializer());
        when(statefulHook1.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
                .thenReturn(CompletableFuture.completedFuture(state1));

        final MasterTriggerRestoreHook<Long> statefulHook2 =
                mockGeneric(MasterTriggerRestoreHook.class);
        when(statefulHook2.getIdentifier()).thenReturn(id2);
        when(statefulHook2.createCheckpointDataSerializer()).thenReturn(new LongSerializer());
        when(statefulHook2.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
                .thenReturn(CompletableFuture.completedFuture(state2));

        final MasterTriggerRestoreHook<Void> statelessHook =
                mockGeneric(MasterTriggerRestoreHook.class);
        when(statelessHook.getIdentifier()).thenReturn("some-id");

        // create the checkpoint coordinator
        JobVertexID jobVertexId = new JobVertexID();
        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexId)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final CheckpointCoordinator cc =
                instantiateCheckpointCoordinator(graph, manuallyTriggeredScheduledExecutor);

        cc.addMasterHook(statefulHook1);
        cc.addMasterHook(statelessHook);
        cc.addMasterHook(statefulHook2);

        // trigger a checkpoint
        final CompletableFuture<CompletedCheckpoint> checkpointFuture = cc.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointFuture).isNotCompletedExceptionally();
        assertThat(cc.getNumberOfPendingCheckpoints()).isOne();

        verify(statefulHook1, times(1))
                .triggerCheckpoint(anyLong(), anyLong(), any(Executor.class));
        verify(statefulHook2, times(1))
                .triggerCheckpoint(anyLong(), anyLong(), any(Executor.class));
        verify(statelessHook, times(1))
                .triggerCheckpoint(anyLong(), anyLong(), any(Executor.class));

        ExecutionAttemptID attemptID =
                graph.getJobVertex(jobVertexId)
                        .getTaskVertices()[0]
                        .getCurrentExecutionAttempt()
                        .getAttemptId();

        final long checkpointId =
                cc.getPendingCheckpoints().values().iterator().next().getCheckpointID();
        cc.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID, checkpointId),
                "Unknown location");
        assertThat(cc.getNumberOfPendingCheckpoints()).isZero();

        assertThat(cc.getNumberOfRetainedSuccessfulCheckpoints()).isOne();
        final CompletedCheckpoint chk = cc.getCheckpointStore().getLatestCheckpoint();

        final Collection<MasterState> masterStates = chk.getMasterHookStates();
        assertThat(masterStates.size()).isEqualTo(2);

        for (MasterState ms : masterStates) {
            if (ms.name().equals(id1)) {
                assertThat(ms.bytes()).isEqualTo(state1serialized);
                assertThat(ms.version()).isEqualTo(StringSerializer.VERSION);
            } else if (ms.name().equals(id2)) {
                assertThat(ms.bytes()).isEqualTo(state2serialized);
                assertThat(ms.version()).isEqualTo(LongSerializer.VERSION);
            } else {
                fail("unrecognized state name: " + ms.name());
            }
        }
    }

    @Test
    void testHooksAreCalledOnRestore() throws Exception {
        final String id1 = "id1";
        final String id2 = "id2";

        final String state1 = "the-test-string-state";
        final byte[] state1serialized = new StringSerializer().serialize(state1);

        final long state2 = 987654321L;
        final byte[] state2serialized = new LongSerializer().serialize(state2);

        final List<MasterState> masterHookStates =
                Arrays.asList(
                        new MasterState(id1, state1serialized, StringSerializer.VERSION),
                        new MasterState(id2, state2serialized, LongSerializer.VERSION));

        final MasterTriggerRestoreHook<String> statefulHook1 =
                mockGeneric(MasterTriggerRestoreHook.class);
        when(statefulHook1.getIdentifier()).thenReturn(id1);
        when(statefulHook1.createCheckpointDataSerializer()).thenReturn(new StringSerializer());
        when(statefulHook1.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
                .thenThrow(new Exception("not expected"));

        final MasterTriggerRestoreHook<Long> statefulHook2 =
                mockGeneric(MasterTriggerRestoreHook.class);
        when(statefulHook2.getIdentifier()).thenReturn(id2);
        when(statefulHook2.createCheckpointDataSerializer()).thenReturn(new LongSerializer());
        when(statefulHook2.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
                .thenThrow(new Exception("not expected"));

        final MasterTriggerRestoreHook<Void> statelessHook =
                mockGeneric(MasterTriggerRestoreHook.class);
        when(statelessHook.getIdentifier()).thenReturn("some-id");

        final JobID jid = new JobID();
        final long checkpointId = 13L;

        final CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        jid,
                        checkpointId,
                        123L,
                        125L,
                        Collections.<OperatorID, OperatorState>emptyMap(),
                        masterHookStates,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null);
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());
        CheckpointCoordinator cc = instantiateCheckpointCoordinator(graph);

        cc.addMasterHook(statefulHook1);
        cc.addMasterHook(statelessHook);
        cc.addMasterHook(statefulHook2);

        cc.getCheckpointStore()
                .addCheckpointAndSubsumeOldestOne(checkpoint, new CheckpointsCleaner(), () -> {});
        cc.restoreLatestCheckpointedStateToAll(Collections.emptySet(), false);

        verify(statefulHook1, times(1)).restoreCheckpoint(eq(checkpointId), eq(state1));
        verify(statefulHook2, times(1)).restoreCheckpoint(eq(checkpointId), eq(state2));
        verify(statelessHook, times(1)).restoreCheckpoint(eq(checkpointId), isNull(Void.class));
    }

    @Test
    void checkUnMatchedStateOnRestore() throws Exception {
        final String id1 = "id1";
        final String id2 = "id2";

        final String state1 = "the-test-string-state";
        final byte[] state1serialized = new StringSerializer().serialize(state1);

        final long state2 = 987654321L;
        final byte[] state2serialized = new LongSerializer().serialize(state2);

        final List<MasterState> masterHookStates =
                Arrays.asList(
                        new MasterState(id1, state1serialized, StringSerializer.VERSION),
                        new MasterState(id2, state2serialized, LongSerializer.VERSION));

        final MasterTriggerRestoreHook<String> statefulHook =
                mockGeneric(MasterTriggerRestoreHook.class);
        when(statefulHook.getIdentifier()).thenReturn(id1);
        when(statefulHook.createCheckpointDataSerializer()).thenReturn(new StringSerializer());
        when(statefulHook.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
                .thenThrow(new Exception("not expected"));

        final MasterTriggerRestoreHook<Void> statelessHook =
                mockGeneric(MasterTriggerRestoreHook.class);
        when(statelessHook.getIdentifier()).thenReturn("some-id");

        final JobID jid = new JobID();
        final long checkpointId = 44L;

        final CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        jid,
                        checkpointId,
                        123L,
                        125L,
                        Collections.<OperatorID, OperatorState>emptyMap(),
                        masterHookStates,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());
        CheckpointCoordinator cc = instantiateCheckpointCoordinator(graph);

        cc.addMasterHook(statefulHook);
        cc.addMasterHook(statelessHook);

        cc.getCheckpointStore()
                .addCheckpointAndSubsumeOldestOne(checkpoint, new CheckpointsCleaner(), () -> {});

        // since we have unmatched state, this should fail
        try {
            cc.restoreLatestCheckpointedStateToAll(Collections.emptySet(), false);
            fail("exception expected");
        } catch (IllegalStateException ignored) {
        }

        // permitting unmatched state should succeed
        cc.restoreLatestCheckpointedStateToAll(Collections.emptySet(), true);

        verify(statefulHook, times(1)).restoreCheckpoint(eq(checkpointId), eq(state1));
        verify(statelessHook, times(1)).restoreCheckpoint(eq(checkpointId), isNull(Void.class));
    }

    // ------------------------------------------------------------------------
    //  failure scenarios
    // ------------------------------------------------------------------------

    /**
     * This test makes sure that the checkpoint is already registered by the time. that the hooks
     * are called
     */
    @Test
    void ensureRegisteredAtHookTime() throws Exception {
        final String id = "id";

        // create the checkpoint coordinator
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());
        final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        CheckpointCoordinator cc =
                instantiateCheckpointCoordinator(graph, manuallyTriggeredScheduledExecutor);

        final MasterTriggerRestoreHook<Void> hook = mockGeneric(MasterTriggerRestoreHook.class);
        when(hook.getIdentifier()).thenReturn(id);
        when(hook.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
                .thenAnswer(
                        new Answer<CompletableFuture<Void>>() {

                            @Override
                            public CompletableFuture<Void> answer(InvocationOnMock invocation)
                                    throws Throwable {
                                assertThat(cc.getNumberOfPendingCheckpoints()).isOne();

                                long checkpointId = (Long) invocation.getArguments()[0];
                                assertThat(cc.getPendingCheckpoints()).containsKey(checkpointId);
                                return null;
                            }
                        });

        cc.addMasterHook(hook);

        // trigger a checkpoint
        final CompletableFuture<CompletedCheckpoint> checkpointFuture = cc.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointFuture).isNotCompletedExceptionally();
    }

    // ------------------------------------------------------------------------
    //  failure scenarios
    // ------------------------------------------------------------------------

    @Test
    void testSerializationFailsOnTrigger() {}

    @Test
    void testHookCallFailsOnTrigger() {}

    @Test
    void testDeserializationFailsOnRestore() {}

    @Test
    void testHookCallFailsOnRestore() {}

    @Test
    void testTypeIncompatibleWithSerializerOnStore() {}

    @Test
    void testTypeIncompatibleWithHookOnRestore() {}

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private CheckpointCoordinator instantiateCheckpointCoordinator(ExecutionGraph executionGraph) {

        return instantiateCheckpointCoordinator(
                executionGraph, new ManuallyTriggeredScheduledExecutor());
    }

    private CheckpointCoordinator instantiateCheckpointCoordinator(
            ExecutionGraph graph, ScheduledExecutor testingScheduledExecutor) {

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration(
                        10000000L,
                        600000L,
                        0L,
                        1,
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                        true,
                        false,
                        0,
                        0);
        Executor executor = Executors.directExecutor();
        return new CheckpointCoordinator(
                graph.getJobID(),
                chkConfig,
                Collections.emptyList(),
                new StandaloneCheckpointIDCounter(),
                new StandaloneCompletedCheckpointStore(10),
                new JobManagerCheckpointStorage(),
                executor,
                new CheckpointsCleaner(),
                testingScheduledExecutor,
                new CheckpointFailureManager(0, NoOpFailJobCall.INSTANCE),
                new DefaultCheckpointPlanCalculator(
                        graph.getJobID(),
                        new ExecutionGraphCheckpointPlanCalculatorContext(graph),
                        graph.getVerticesTopologically(),
                        false),
                new DefaultCheckpointStatsTracker(
                        1, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup()));
    }

    private static <T> T mockGeneric(Class<?> clazz) {
        @SuppressWarnings("unchecked")
        Class<T> typedClass = (Class<T>) clazz;
        return mock(typedClass);
    }

    // ------------------------------------------------------------------------

    private static final class LongSerializer implements SimpleVersionedSerializer<Long> {

        static final int VERSION = 5;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(Long checkpointData) throws IOException {
            final byte[] bytes = new byte[8];
            ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(0, checkpointData);
            return bytes;
        }

        @Override
        public Long deserialize(int version, byte[] serialized) throws IOException {
            assertThat(version).isEqualTo(VERSION);
            assertThat(serialized.length).isEqualTo(8);

            return ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN).getLong(0);
        }
    }
}
