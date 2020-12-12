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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.Test;
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

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.StringSerializer;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionVertex;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the user-defined hooks that the checkpoint coordinator can call.
 */
public class CheckpointCoordinatorMasterHooksTest {

	// ------------------------------------------------------------------------
	//  hook registration
	// ------------------------------------------------------------------------

	/**
	 * This method tests that hooks with the same identifier are not registered
	 * multiple times.
	 */
	@Test
	public void testDeduplicateOnRegister() {
		final CheckpointCoordinator cc = instantiateCheckpointCoordinator(new JobID());

		MasterTriggerRestoreHook<?> hook1 = mock(MasterTriggerRestoreHook.class);
		when(hook1.getIdentifier()).thenReturn("test id");

		MasterTriggerRestoreHook<?> hook2 = mock(MasterTriggerRestoreHook.class);
		when(hook2.getIdentifier()).thenReturn("test id");

		MasterTriggerRestoreHook<?> hook3 = mock(MasterTriggerRestoreHook.class);
		when(hook3.getIdentifier()).thenReturn("anotherId");

		assertTrue(cc.addMasterHook(hook1));
		assertFalse(cc.addMasterHook(hook2));
		assertTrue(cc.addMasterHook(hook3));
	}

	/**
	 * Test that validates correct exceptions when supplying hooks with invalid IDs.
	 */
	@Test
	public void testNullOrInvalidId() {
		final CheckpointCoordinator cc = instantiateCheckpointCoordinator(new JobID());

		try {
			cc.addMasterHook(null);
			fail("expected an exception");
		} catch (NullPointerException ignored) {}

		try {
			cc.addMasterHook(mock(MasterTriggerRestoreHook.class));
			fail("expected an exception");
		} catch (IllegalArgumentException ignored) {}

		try {
			MasterTriggerRestoreHook<?> hook = mock(MasterTriggerRestoreHook.class);
			when(hook.getIdentifier()).thenReturn("        ");

			cc.addMasterHook(hook);
			fail("expected an exception");
		} catch (IllegalArgumentException ignored) {}
	}

	@Test
	public void testHookReset() throws Exception {
		final String id1 = "id1";
		final String id2 = "id2";

		final MasterTriggerRestoreHook<String> hook1 = mockGeneric(MasterTriggerRestoreHook.class);
		when(hook1.getIdentifier()).thenReturn(id1);
		final MasterTriggerRestoreHook<String> hook2 = mockGeneric(MasterTriggerRestoreHook.class);
		when(hook2.getIdentifier()).thenReturn(id2);

		// create the checkpoint coordinator
		final JobID jid = new JobID();
		final ExecutionAttemptID execId = new ExecutionAttemptID();
		final ExecutionVertex ackVertex = mockExecutionVertex(execId);
		final CheckpointCoordinator cc = instantiateCheckpointCoordinator(jid, ackVertex);

		cc.addMasterHook(hook1);
		cc.addMasterHook(hook2);

		// initialize the hooks
		cc.restoreLatestCheckpointedStateToAll(
			Collections.emptySet(),
			false);
		verify(hook1, times(1)).reset();
		verify(hook2, times(1)).reset();

		// shutdown
		cc.shutdown(JobStatus.CANCELED);
		verify(hook1, times(1)).close();
		verify(hook2, times(1)).close();
	}

	// ------------------------------------------------------------------------
	//  trigger / restore behavior
	// ------------------------------------------------------------------------

	@Test
	public void testHooksAreCalledOnTrigger() throws Exception {
		final String id1 = "id1";
		final String id2 = "id2";

		final String state1 = "the-test-string-state";
		final byte[] state1serialized = new StringSerializer().serialize(state1);

		final long state2 = 987654321L;
		final byte[] state2serialized = new LongSerializer().serialize(state2);

		final MasterTriggerRestoreHook<String> statefulHook1 = mockGeneric(MasterTriggerRestoreHook.class);
		when(statefulHook1.getIdentifier()).thenReturn(id1);
		when(statefulHook1.createCheckpointDataSerializer()).thenReturn(new StringSerializer());
		when(statefulHook1.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
				.thenReturn(CompletableFuture.completedFuture(state1));

		final MasterTriggerRestoreHook<Long> statefulHook2 = mockGeneric(MasterTriggerRestoreHook.class);
		when(statefulHook2.getIdentifier()).thenReturn(id2);
		when(statefulHook2.createCheckpointDataSerializer()).thenReturn(new LongSerializer());
		when(statefulHook2.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
				.thenReturn(CompletableFuture.completedFuture(state2));

		final MasterTriggerRestoreHook<Void> statelessHook = mockGeneric(MasterTriggerRestoreHook.class);
		when(statelessHook.getIdentifier()).thenReturn("some-id");

		// create the checkpoint coordinator
		final JobID jid = new JobID();
		final ExecutionAttemptID execId = new ExecutionAttemptID();
		final ExecutionVertex ackVertex = mockExecutionVertex(execId);
		final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
			new ManuallyTriggeredScheduledExecutor();
		final CheckpointCoordinator cc = instantiateCheckpointCoordinator(
			jid, manuallyTriggeredScheduledExecutor, ackVertex);

		cc.addMasterHook(statefulHook1);
		cc.addMasterHook(statelessHook);
		cc.addMasterHook(statefulHook2);

		// trigger a checkpoint
		final CompletableFuture<CompletedCheckpoint> checkpointFuture = cc.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(checkpointFuture.isCompletedExceptionally());
		assertEquals(1, cc.getNumberOfPendingCheckpoints());

		verify(statefulHook1, times(1)).triggerCheckpoint(anyLong(), anyLong(), any(Executor.class));
		verify(statefulHook2, times(1)).triggerCheckpoint(anyLong(), anyLong(), any(Executor.class));
		verify(statelessHook, times(1)).triggerCheckpoint(anyLong(), anyLong(), any(Executor.class));

		final long checkpointId = cc.getPendingCheckpoints().values().iterator().next().getCheckpointId();
		cc.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, execId, checkpointId), "Unknown location");
		assertEquals(0, cc.getNumberOfPendingCheckpoints());

		assertEquals(1, cc.getNumberOfRetainedSuccessfulCheckpoints());
		final CompletedCheckpoint chk = cc.getCheckpointStore().getLatestCheckpoint(false);

		final Collection<MasterState> masterStates = chk.getMasterHookStates();
		assertEquals(2, masterStates.size());

		for (MasterState ms : masterStates) {
			if (ms.name().equals(id1)) {
				assertArrayEquals(state1serialized, ms.bytes());
				assertEquals(StringSerializer.VERSION, ms.version());
			}
			else if (ms.name().equals(id2)) {
				assertArrayEquals(state2serialized, ms.bytes());
				assertEquals(LongSerializer.VERSION, ms.version());
			}
			else {
				fail("unrecognized state name: " + ms.name());
			}
		}
	}

	@Test
	public void testHooksAreCalledOnRestore() throws Exception {
		final String id1 = "id1";
		final String id2 = "id2";

		final String state1 = "the-test-string-state";
		final byte[] state1serialized = new StringSerializer().serialize(state1);

		final long state2 = 987654321L;
		final byte[] state2serialized = new LongSerializer().serialize(state2);

		final List<MasterState> masterHookStates = Arrays.asList(
				new MasterState(id1, state1serialized, StringSerializer.VERSION),
				new MasterState(id2, state2serialized, LongSerializer.VERSION));

		final MasterTriggerRestoreHook<String> statefulHook1 = mockGeneric(MasterTriggerRestoreHook.class);
		when(statefulHook1.getIdentifier()).thenReturn(id1);
		when(statefulHook1.createCheckpointDataSerializer()).thenReturn(new StringSerializer());
		when(statefulHook1.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
				.thenThrow(new Exception("not expected"));

		final MasterTriggerRestoreHook<Long> statefulHook2 = mockGeneric(MasterTriggerRestoreHook.class);
		when(statefulHook2.getIdentifier()).thenReturn(id2);
		when(statefulHook2.createCheckpointDataSerializer()).thenReturn(new LongSerializer());
		when(statefulHook2.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
				.thenThrow(new Exception("not expected"));

		final MasterTriggerRestoreHook<Void> statelessHook = mockGeneric(MasterTriggerRestoreHook.class);
		when(statelessHook.getIdentifier()).thenReturn("some-id");

		final JobID jid = new JobID();
		final long checkpointId = 13L;

		final CompletedCheckpoint checkpoint = new CompletedCheckpoint(
				jid, checkpointId, 123L, 125L,
				Collections.<OperatorID, OperatorState>emptyMap(),
				masterHookStates,
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
				new TestCompletedCheckpointStorageLocation()
		);
		final ExecutionAttemptID execId = new ExecutionAttemptID();
		final ExecutionVertex ackVertex = mockExecutionVertex(execId);
		final CheckpointCoordinator cc = instantiateCheckpointCoordinator(jid, ackVertex);

		cc.addMasterHook(statefulHook1);
		cc.addMasterHook(statelessHook);
		cc.addMasterHook(statefulHook2);

		cc.getCheckpointStore().addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {
		});
		cc.restoreLatestCheckpointedStateToAll(
				Collections.emptySet(),
				false);

		verify(statefulHook1, times(1)).restoreCheckpoint(eq(checkpointId), eq(state1));
		verify(statefulHook2, times(1)).restoreCheckpoint(eq(checkpointId), eq(state2));
		verify(statelessHook, times(1)).restoreCheckpoint(eq(checkpointId), isNull(Void.class));
	}

	@Test
	public void checkUnMatchedStateOnRestore() throws Exception {
		final String id1 = "id1";
		final String id2 = "id2";

		final String state1 = "the-test-string-state";
		final byte[] state1serialized = new StringSerializer().serialize(state1);

		final long state2 = 987654321L;
		final byte[] state2serialized = new LongSerializer().serialize(state2);

		final List<MasterState> masterHookStates = Arrays.asList(
				new MasterState(id1, state1serialized, StringSerializer.VERSION),
				new MasterState(id2, state2serialized, LongSerializer.VERSION));

		final MasterTriggerRestoreHook<String> statefulHook = mockGeneric(MasterTriggerRestoreHook.class);
		when(statefulHook.getIdentifier()).thenReturn(id1);
		when(statefulHook.createCheckpointDataSerializer()).thenReturn(new StringSerializer());
		when(statefulHook.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class)))
				.thenThrow(new Exception("not expected"));

		final MasterTriggerRestoreHook<Void> statelessHook = mockGeneric(MasterTriggerRestoreHook.class);
		when(statelessHook.getIdentifier()).thenReturn("some-id");

		final JobID jid = new JobID();
		final long checkpointId = 44L;

		final CompletedCheckpoint checkpoint = new CompletedCheckpoint(
				jid, checkpointId, 123L, 125L,
				Collections.<OperatorID, OperatorState>emptyMap(),
				masterHookStates,
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
				new TestCompletedCheckpointStorageLocation()
		);

		final ExecutionAttemptID execId = new ExecutionAttemptID();
		final ExecutionVertex ackVertex = mockExecutionVertex(execId);
		final CheckpointCoordinator cc = instantiateCheckpointCoordinator(jid, ackVertex);

		cc.addMasterHook(statefulHook);
		cc.addMasterHook(statelessHook);

		cc.getCheckpointStore().addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {
		});

		// since we have unmatched state, this should fail
		try {
			cc.restoreLatestCheckpointedStateToAll(
					Collections.emptySet(),
					false);
			fail("exception expected");
		}
		catch (IllegalStateException ignored) {}

		// permitting unmatched state should succeed
		cc.restoreLatestCheckpointedStateToAll(
				Collections.emptySet(),
				true);

		verify(statefulHook, times(1)).restoreCheckpoint(eq(checkpointId), eq(state1));
		verify(statelessHook, times(1)).restoreCheckpoint(eq(checkpointId), isNull(Void.class));
	}

	// ------------------------------------------------------------------------
	//  failure scenarios
	// ------------------------------------------------------------------------

	/**
	 * This test makes sure that the checkpoint is already registered by the time.
	 * that the hooks are called
	 */
	@Test
	public void ensureRegisteredAtHookTime() throws Exception {
		final String id = "id";

		// create the checkpoint coordinator
		final JobID jid = new JobID();
		final ExecutionAttemptID execId = new ExecutionAttemptID();
		final ExecutionVertex ackVertex = mockExecutionVertex(execId);
		final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
			new ManuallyTriggeredScheduledExecutor();
		final CheckpointCoordinator cc = instantiateCheckpointCoordinator(
			jid, manuallyTriggeredScheduledExecutor, ackVertex);

		final MasterTriggerRestoreHook<Void> hook = mockGeneric(MasterTriggerRestoreHook.class);
		when(hook.getIdentifier()).thenReturn(id);
		when(hook.triggerCheckpoint(anyLong(), anyLong(), any(Executor.class))).thenAnswer(
				new Answer<CompletableFuture<Void>>() {

					@Override
					public CompletableFuture<Void> answer(InvocationOnMock invocation) throws Throwable {
						assertEquals(1, cc.getNumberOfPendingCheckpoints());

						long checkpointId = (Long) invocation.getArguments()[0];
						assertNotNull(cc.getPendingCheckpoints().get(checkpointId));
						return null;
					}
				}
		);

		cc.addMasterHook(hook);

		// trigger a checkpoint
		final CompletableFuture<CompletedCheckpoint> checkpointFuture = cc.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(checkpointFuture.isCompletedExceptionally());
	}


	// ------------------------------------------------------------------------
	//  failure scenarios
	// ------------------------------------------------------------------------

	@Test
	public void testSerializationFailsOnTrigger() {
	}

	@Test
	public void testHookCallFailsOnTrigger() {
	}

	@Test
	public void testDeserializationFailsOnRestore() {
	}

	@Test
	public void testHookCallFailsOnRestore() {
	}

	@Test
	public void testTypeIncompatibleWithSerializerOnStore() {
	}

	@Test
	public void testTypeIncompatibleWithHookOnRestore() {
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private CheckpointCoordinator instantiateCheckpointCoordinator(
		JobID jid,
		ExecutionVertex... ackVertices) {

		return instantiateCheckpointCoordinator(jid, new ManuallyTriggeredScheduledExecutor(), ackVertices);
	}

	private CheckpointCoordinator instantiateCheckpointCoordinator(
		JobID jid,
		ScheduledExecutor testingScheduledExecutor,
		ExecutionVertex... ackVertices) {

		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			10000000L,
			600000L,
			0L,
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			false,
			0);
		Executor executor = Executors.directExecutor();
		return new CheckpointCoordinator(
				jid,
				chkConfig,
				new ExecutionVertex[0],
				ackVertices,
				new ExecutionVertex[0],
				Collections.emptyList(),
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(10),
				new MemoryStateBackend(),
				executor,
				new CheckpointsCleaner(),
				testingScheduledExecutor,
				SharedStateRegistry.DEFAULT_FACTORY,
				new CheckpointFailureManager(
					0,
					NoOpFailJobCall.INSTANCE));
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
			assertEquals(VERSION, version);
			assertEquals(8, serialized.length);

			return ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN).getLong(0);
		}
	}
}
