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
package org.apache.flink.streaming.api.operators;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.RunnableFuture;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for the facilities provided by {@link AbstractStreamOperator}. This mostly
 * tests timers and state and whether they are correctly checkpointed/restored
 * with key-group reshuffling.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AbstractStreamOperator.class)
public class AbstractStreamOperatorTest {

	@Test
	public void testStateDoesNotInterfere() throws Exception {
		TestOperator testOperator = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(testOperator, new TestKeySelector(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);
		testHarness.processElement(new Tuple2<>(1, "SET_STATE:CIAO"), 0);

		testHarness.processElement(new Tuple2<>(1, "EMIT_STATE"), 0);
		testHarness.processElement(new Tuple2<>(0, "EMIT_STATE"), 0);

		assertThat(
				extractResult(testHarness),
				contains("ON_ELEMENT:1:CIAO", "ON_ELEMENT:0:HELLO"));
	}

	/**
	 * Verify that firing event-time timers see the state of the key that was active
	 * when the timer was set.
	 */
	@Test
	public void testEventTimeTimersDontInterfere() throws Exception {
		TestOperator testOperator = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(testOperator, new TestKeySelector(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		testHarness.processWatermark(0L);

		testHarness.processElement(new Tuple2<>(1, "SET_EVENT_TIME_TIMER:20"), 0);

		testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);
		testHarness.processElement(new Tuple2<>(1, "SET_STATE:CIAO"), 0);

		testHarness.processElement(new Tuple2<>(0, "SET_EVENT_TIME_TIMER:10"), 0);

		testHarness.processWatermark(10L);

		assertThat(
				extractResult(testHarness),
				contains("ON_EVENT_TIME:HELLO"));

		testHarness.processWatermark(20L);

		assertThat(
				extractResult(testHarness),
				contains("ON_EVENT_TIME:CIAO"));
	}

	/**
	 * Verify that firing processing-time timers see the state of the key that was active
	 * when the timer was set.
	 */
	@Test
	public void testProcessingTimeTimersDontInterfere() throws Exception {
		TestOperator testOperator = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(testOperator, new TestKeySelector(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		testHarness.setProcessingTime(0L);

		testHarness.processElement(new Tuple2<>(1, "SET_PROC_TIME_TIMER:20"), 0);

		testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);
		testHarness.processElement(new Tuple2<>(1, "SET_STATE:CIAO"), 0);

		testHarness.processElement(new Tuple2<>(0, "SET_PROC_TIME_TIMER:10"), 0);

		testHarness.setProcessingTime(10L);

		assertThat(
				extractResult(testHarness),
				contains("ON_PROC_TIME:HELLO"));

		testHarness.setProcessingTime(20L);

		assertThat(
				extractResult(testHarness),
				contains("ON_PROC_TIME:CIAO"));
	}

	/**
	 * Verify that a low-level timer is set for processing-time timers in case of restore.
	 */
	@Test
	public void testEnsureProcessingTimeTimerRegisteredOnRestore() throws Exception {
		TestOperator testOperator = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(testOperator, new TestKeySelector(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		testHarness.setProcessingTime(0L);

		testHarness.processElement(new Tuple2<>(1, "SET_PROC_TIME_TIMER:20"), 0);

		testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);
		testHarness.processElement(new Tuple2<>(1, "SET_STATE:CIAO"), 0);

		testHarness.processElement(new Tuple2<>(0, "SET_PROC_TIME_TIMER:10"), 0);

		OperatorStateHandles snapshot = testHarness.snapshot(0, 0);

		TestOperator testOperator1 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness1 =
				new KeyedOneInputStreamOperatorTestHarness<>(
						testOperator1,
						new TestKeySelector(),
						BasicTypeInfo.INT_TYPE_INFO);

		testHarness1.setProcessingTime(0L);

		testHarness1.setup();
		testHarness1.initializeState(snapshot);
		testHarness1.open();

		testHarness1.setProcessingTime(10L);

		assertThat(
				extractResult(testHarness1),
				contains("ON_PROC_TIME:HELLO"));

		testHarness1.setProcessingTime(20L);

		assertThat(
				extractResult(testHarness1),
				contains("ON_PROC_TIME:CIAO"));
	}


	/**
	 * Verify that timers for the different time domains don't clash.
	 */
	@Test
	public void testProcessingTimeAndEventTimeDontInterfere() throws Exception {
		TestOperator testOperator = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(testOperator, new TestKeySelector(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		testHarness.setProcessingTime(0L);
		testHarness.processWatermark(0L);

		testHarness.processElement(new Tuple2<>(0, "SET_PROC_TIME_TIMER:10"), 0);
		testHarness.processElement(new Tuple2<>(0, "SET_EVENT_TIME_TIMER:20"), 0);

		testHarness.processElement(new Tuple2<>(0, "SET_STATE:HELLO"), 0);

		testHarness.processWatermark(20L);

		assertThat(
				extractResult(testHarness),
				contains("ON_EVENT_TIME:HELLO"));

		testHarness.setProcessingTime(10L);

		assertThat(
				extractResult(testHarness),
				contains("ON_PROC_TIME:HELLO"));
	}

	/**
	 * Verify that state and timers are checkpointed per key group and that they are correctly
	 * assigned to operator subtasks when restoring.
	 */
	@Test
	public void testStateAndTimerStateShufflingScalingUp() throws Exception {
		final int MAX_PARALLELISM = 10;

		// first get two keys that will fall into different key-group ranges that go
		// to different operator subtasks when we restore

		// get two sub key-ranges so that we can restore two ranges separately
		KeyGroupRange subKeyGroupRange1 = new KeyGroupRange(0, (MAX_PARALLELISM / 2) - 1);
		KeyGroupRange subKeyGroupRange2 = new KeyGroupRange(subKeyGroupRange1.getEndKeyGroup() + 1, MAX_PARALLELISM - 1);

		// get two different keys, one per sub range
		int key1 = getKeyInKeyGroupRange(subKeyGroupRange1, MAX_PARALLELISM);
		int key2 = getKeyInKeyGroupRange(subKeyGroupRange2, MAX_PARALLELISM);

		TestOperator testOperator = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						testOperator,
						new TestKeySelector(),
						BasicTypeInfo.INT_TYPE_INFO,
						MAX_PARALLELISM,
						1, /* num subtasks */
						0 /* subtask index */);

		testHarness.open();

		testHarness.processWatermark(0L);
		testHarness.setProcessingTime(0L);

		testHarness.processElement(new Tuple2<>(key1, "SET_EVENT_TIME_TIMER:10"), 0);
		testHarness.processElement(new Tuple2<>(key2, "SET_EVENT_TIME_TIMER:20"), 0);

		testHarness.processElement(new Tuple2<>(key1, "SET_PROC_TIME_TIMER:10"), 0);
		testHarness.processElement(new Tuple2<>(key2, "SET_PROC_TIME_TIMER:20"), 0);

		testHarness.processElement(new Tuple2<>(key1, "SET_STATE:HELLO"), 0);
		testHarness.processElement(new Tuple2<>(key2, "SET_STATE:CIAO"), 0);

		assertTrue(extractResult(testHarness).isEmpty());

		OperatorStateHandles snapshot = testHarness.snapshot(0, 0);

		// now, restore in two operators, first operator 1

		TestOperator testOperator1 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness1 =
				new KeyedOneInputStreamOperatorTestHarness<>(
						testOperator1,
						new TestKeySelector(),
						BasicTypeInfo.INT_TYPE_INFO,
						MAX_PARALLELISM,
						2, /* num subtasks */
						0 /* subtask index */);

		testHarness1.setup();
		testHarness1.initializeState(snapshot);
		testHarness1.open();

		testHarness1.processWatermark(10L);

		assertThat(extractResult(testHarness1), contains("ON_EVENT_TIME:HELLO"));

		assertTrue(extractResult(testHarness1).isEmpty());

		// this should not trigger anything, the trigger for WM=20 should sit in the
		// other operator subtask
		testHarness1.processWatermark(20L);

		assertTrue(extractResult(testHarness1).isEmpty());

		testHarness1.setProcessingTime(10L);

		assertThat(extractResult(testHarness1), contains("ON_PROC_TIME:HELLO"));

		assertTrue(extractResult(testHarness1).isEmpty());

		// this should not trigger anything, the trigger for TIME=20 should sit in the
		// other operator subtask
		testHarness1.setProcessingTime(20L);

		assertTrue(extractResult(testHarness1).isEmpty());

		// now, for the second operator
		TestOperator testOperator2 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness2 =
				new KeyedOneInputStreamOperatorTestHarness<>(
						testOperator2,
						new TestKeySelector(),
						BasicTypeInfo.INT_TYPE_INFO,
						MAX_PARALLELISM,
						2, /* num subtasks */
						1 /* subtask index */);

		testHarness2.setup();
		testHarness2.initializeState(snapshot);
		testHarness2.open();

		testHarness2.processWatermark(10L);

		// nothing should happen because this timer is in the other subtask
		assertTrue(extractResult(testHarness2).isEmpty());

		testHarness2.processWatermark(20L);

		assertThat(extractResult(testHarness2), contains("ON_EVENT_TIME:CIAO"));

		testHarness2.setProcessingTime(10L);

		// nothing should happen because this timer is in the other subtask
		assertTrue(extractResult(testHarness2).isEmpty());

		testHarness2.setProcessingTime(20L);

		assertThat(extractResult(testHarness2), contains("ON_PROC_TIME:CIAO"));

		assertTrue(extractResult(testHarness2).isEmpty());
	}

	@Test
	public void testStateAndTimerStateShufflingScalingDown() throws Exception {
		final int MAX_PARALLELISM = 10;

		// first get two keys that will fall into different key-group ranges that go
		// to different operator subtasks when we restore

		// get two sub key-ranges so that we can restore two ranges separately
		KeyGroupRange subKeyGroupRange1 = new KeyGroupRange(0, (MAX_PARALLELISM / 2) - 1);
		KeyGroupRange subKeyGroupRange2 = new KeyGroupRange(subKeyGroupRange1.getEndKeyGroup() + 1, MAX_PARALLELISM - 1);

		// get two different keys, one per sub range
		int key1 = getKeyInKeyGroupRange(subKeyGroupRange1, MAX_PARALLELISM);
		int key2 = getKeyInKeyGroupRange(subKeyGroupRange2, MAX_PARALLELISM);

		TestOperator testOperator1 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness1 =
			new KeyedOneInputStreamOperatorTestHarness<>(
				testOperator1,
				new TestKeySelector(),
				BasicTypeInfo.INT_TYPE_INFO,
				MAX_PARALLELISM,
				2, /* num subtasks */
				0 /* subtask index */);

		testHarness1.setup();
		testHarness1.open();

		testHarness1.processWatermark(0L);
		testHarness1.setProcessingTime(0L);

		TestOperator testOperator2 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness2 =
			new KeyedOneInputStreamOperatorTestHarness<>(
				testOperator2,
				new TestKeySelector(),
				BasicTypeInfo.INT_TYPE_INFO,
				MAX_PARALLELISM,
				2, /* num subtasks */
				1 /* subtask index */);


		testHarness2.setup();
		testHarness2.open();

		testHarness2.processWatermark(0L);
		testHarness2.setProcessingTime(0L);

		// register some state with both instances and scale down to parallelism 1

		testHarness1.processElement(new Tuple2<>(key1, "SET_EVENT_TIME_TIMER:30"), 0);
		testHarness1.processElement(new Tuple2<>(key1, "SET_PROC_TIME_TIMER:30"), 0);
		testHarness1.processElement(new Tuple2<>(key1, "SET_STATE:HELLO"), 0);

		testHarness2.processElement(new Tuple2<>(key2, "SET_EVENT_TIME_TIMER:40"), 0);
		testHarness2.processElement(new Tuple2<>(key2, "SET_PROC_TIME_TIMER:40"), 0);
		testHarness2.processElement(new Tuple2<>(key2, "SET_STATE:CIAO"), 0);

		// take a snapshot from each one of the "parallel" instances of the operator
		// and combine them into one so that we can scale down

		OperatorStateHandles repackagedState =
			AbstractStreamOperatorTestHarness.repackageState(
				testHarness1.snapshot(0, 0),
				testHarness2.snapshot(0, 0)
			);

		// now, for the third operator that scales down from parallelism of 2 to 1
		TestOperator testOperator3 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness3 =
			new KeyedOneInputStreamOperatorTestHarness<>(
				testOperator3,
				new TestKeySelector(),
				BasicTypeInfo.INT_TYPE_INFO,
				MAX_PARALLELISM,
				1, /* num subtasks */
				0 /* subtask index */);

		testHarness3.setup();
		testHarness3.initializeState(repackagedState);
		testHarness3.open();

		testHarness3.processWatermark(30L);
		assertThat(extractResult(testHarness3), contains("ON_EVENT_TIME:HELLO"));
		assertTrue(extractResult(testHarness3).isEmpty());

		testHarness3.processWatermark(40L);
		assertThat(extractResult(testHarness3), contains("ON_EVENT_TIME:CIAO"));
		assertTrue(extractResult(testHarness3).isEmpty());

		testHarness3.setProcessingTime(30L);
		assertThat(extractResult(testHarness3), contains("ON_PROC_TIME:HELLO"));
		assertTrue(extractResult(testHarness3).isEmpty());

		testHarness3.setProcessingTime(40L);
		assertThat(extractResult(testHarness3), contains("ON_PROC_TIME:CIAO"));
		assertTrue(extractResult(testHarness3).isEmpty());
	}

	/**
	 * Checks that the state snapshot context is closed after a successful snapshot operation.
	 */
	@Test
	public void testSnapshotMethod() throws Exception {
		final long checkpointId = 42L;
		final long timestamp = 1L;

		final CloseableRegistry closeableRegistry = new CloseableRegistry();

		StateSnapshotContextSynchronousImpl context = mock(StateSnapshotContextSynchronousImpl.class);

		whenNew(StateSnapshotContextSynchronousImpl.class).withAnyArguments().thenReturn(context);

		StreamTask<Void, AbstractStreamOperator<Void>> containingTask = mock(StreamTask.class);
		when(containingTask.getCancelables()).thenReturn(closeableRegistry);

		AbstractStreamOperator<Void> operator = mock(AbstractStreamOperator.class);
		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenCallRealMethod();
		doReturn(containingTask).when(operator).getContainingTask();

		operator.snapshotState(checkpointId, timestamp, CheckpointOptions.forFullCheckpoint());

		verify(context).close();
	}

	/**
	 * Tests that the created StateSnapshotContextSynchronousImpl is closed in case of a failing
	 * Operator#snapshotState(StaetSnapshotContextSynchronousImpl) call.
	 */
	@Test
	public void testFailingSnapshotMethod() throws Exception {
		final long checkpointId = 42L;
		final long timestamp = 1L;

		final Exception failingException = new Exception("Test exception");

		final CloseableRegistry closeableRegistry = new CloseableRegistry();

		StateSnapshotContextSynchronousImpl context = mock(StateSnapshotContextSynchronousImpl.class);

		whenNew(StateSnapshotContextSynchronousImpl.class).withAnyArguments().thenReturn(context);

		StreamTask<Void, AbstractStreamOperator<Void>> containingTask = mock(StreamTask.class);
		when(containingTask.getCancelables()).thenReturn(closeableRegistry);

		AbstractStreamOperator<Void> operator = mock(AbstractStreamOperator.class);
		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenCallRealMethod();
		doReturn(containingTask).when(operator).getContainingTask();

		// lets fail when calling the actual snapshotState method
		doThrow(failingException).when(operator).snapshotState(eq(context));

		try {
			operator.snapshotState(checkpointId, timestamp, CheckpointOptions.forFullCheckpoint());
			fail("Exception expected.");
		} catch (Exception e) {
			assertEquals(failingException, e.getCause());
		}

		verify(context).close();
	}

	/**
	 * Tests that a failing snapshot method call to the keyed state backend will trigger the closing
	 * of the StateSnapshotContextSynchronousImpl and the cancellation of the
	 * OperatorSnapshotResult. The latter is supposed to also cancel all assigned futures.
	 */
	@Test
	public void testFailingBackendSnapshotMethod() throws Exception {
		final long checkpointId = 42L;
		final long timestamp = 1L;

		final Exception failingException = new Exception("Test exception");

		final CloseableRegistry closeableRegistry = new CloseableRegistry();

		RunnableFuture<KeyedStateHandle> futureKeyedStateHandle = mock(RunnableFuture.class);
		RunnableFuture<OperatorStateHandle> futureOperatorStateHandle = mock(RunnableFuture.class);

		StateSnapshotContextSynchronousImpl context = mock(StateSnapshotContextSynchronousImpl.class);
		when(context.getKeyedStateStreamFuture()).thenReturn(futureKeyedStateHandle);
		when(context.getOperatorStateStreamFuture()).thenReturn(futureOperatorStateHandle);

		OperatorSnapshotResult operatorSnapshotResult = spy(new OperatorSnapshotResult());

		whenNew(StateSnapshotContextSynchronousImpl.class).withAnyArguments().thenReturn(context);
		whenNew(OperatorSnapshotResult.class).withAnyArguments().thenReturn(operatorSnapshotResult);

		CheckpointStreamFactory streamFactory = mock(CheckpointStreamFactory.class);
		StreamTask<Void, AbstractStreamOperator<Void>> containingTask = mock(StreamTask.class);
		when(containingTask.getCancelables()).thenReturn(closeableRegistry);

		AbstractStreamOperator<Void> operator = mock(AbstractStreamOperator.class);
		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenCallRealMethod();

		// The amount of mocking in this test makes it necessary to make the
		// getCheckpointStreamFactory method visible for the test and to
		// overwrite its behaviour.
		when(operator.getCheckpointStreamFactory(any(CheckpointOptions.class))).thenReturn(streamFactory);

		doReturn(containingTask).when(operator).getContainingTask();

		RunnableFuture<OperatorStateHandle> futureManagedOperatorStateHandle = mock(RunnableFuture.class);

		OperatorStateBackend operatorStateBackend = mock(OperatorStateBackend.class);
		when(operatorStateBackend.snapshot(eq(checkpointId), eq(timestamp), eq(streamFactory), any(CheckpointOptions.class))).thenReturn(futureManagedOperatorStateHandle);

		AbstractKeyedStateBackend<?> keyedStateBackend = mock(AbstractKeyedStateBackend.class);
		when(keyedStateBackend.snapshot(eq(checkpointId), eq(timestamp), eq(streamFactory), eq(CheckpointOptions.forFullCheckpoint()))).thenThrow(failingException);

		Whitebox.setInternalState(operator, "operatorStateBackend", operatorStateBackend);
		Whitebox.setInternalState(operator, "keyedStateBackend", keyedStateBackend);
		Whitebox.setInternalState(operator, "checkpointStreamFactory", streamFactory);

		try {
			operator.snapshotState(checkpointId, timestamp, CheckpointOptions.forFullCheckpoint());
			fail("Exception expected.");
		} catch (Exception e) {
			assertEquals(failingException, e.getCause());
		}

		// verify that the context has been closed, the operator snapshot result has been cancelled
		// and that all futures have been cancelled.
		verify(context).close();
		verify(operatorSnapshotResult).cancel();

		verify(futureKeyedStateHandle).cancel(anyBoolean());
		verify(futureOperatorStateHandle).cancel(anyBoolean());
		verify(futureKeyedStateHandle).cancel(anyBoolean());
	}

	@Test
	public void testWatermarkCallbackServiceScalingUp() throws Exception {
		final int MAX_PARALLELISM = 10;

		KeySelector<Tuple2<Integer, String>, Integer> keySelector = new TestKeySelector();

		Tuple2<Integer, String> element1 = new Tuple2<>(7, "first");
		Tuple2<Integer, String> element2 = new Tuple2<>(10, "start");

		int keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(element1), MAX_PARALLELISM);
		assertEquals(1, keygroup);
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, keygroup));

		keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(element2), MAX_PARALLELISM);
		assertEquals(9, keygroup);
		assertEquals(1, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, keygroup));

		// now we start the test, we go from parallelism 1 to 2.

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness1 =
			getTestHarness(MAX_PARALLELISM, 1, 0);
		testHarness1.open();

		testHarness1.processElement(new StreamRecord<>(element1));
		testHarness1.processElement(new StreamRecord<>(element2));

		assertEquals(0, testHarness1.getOutput().size());

		// take a snapshot with some elements in internal sorting queue
		OperatorStateHandles snapshot = testHarness1.snapshot(0, 0);
		testHarness1.close();

		// initialize two sub-tasks with the previously snapshotted state to simulate scaling up

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness2 =
			getTestHarness(MAX_PARALLELISM, 2, 0);

		testHarness2.setup();
		testHarness2.initializeState(snapshot);
		testHarness2.open();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness3 =
			getTestHarness(MAX_PARALLELISM, 2, 1);

		testHarness3.setup();
		testHarness3.initializeState(snapshot);
		testHarness3.open();

		testHarness2.processWatermark(new Watermark(10));
		testHarness3.processWatermark(new Watermark(10));

		assertEquals(2, testHarness2.getOutput().size());
		verifyElement(testHarness2.getOutput().poll(), 7);
		verifyWatermark(testHarness2.getOutput().poll(), 10);

		assertEquals(2, testHarness3.getOutput().size());
		verifyElement(testHarness3.getOutput().poll(), 10);
		verifyWatermark(testHarness3.getOutput().poll(), 10);

		testHarness1.close();
		testHarness2.close();
		testHarness3.close();
	}

	@Test
	public void testWatermarkCallbackServiceScalingDown() throws Exception {
		final int MAX_PARALLELISM = 10;

		KeySelector<Tuple2<Integer, String>, Integer> keySelector = new TestKeySelector();

		Tuple2<Integer, String> element1 = new Tuple2<>(7, "first");
		Tuple2<Integer, String> element2 = new Tuple2<>(45, "start");
		Tuple2<Integer, String> element3 = new Tuple2<>(90, "start");
		Tuple2<Integer, String> element4 = new Tuple2<>(10, "start");

		int keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(element1), MAX_PARALLELISM);
		assertEquals(1, keygroup);
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 3, keygroup));
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, keygroup));

		keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(element2), MAX_PARALLELISM);
		assertEquals(6, keygroup);
		assertEquals(1, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 3, keygroup));
		assertEquals(1, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, keygroup));

		keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(element3), MAX_PARALLELISM);
		assertEquals(2, keygroup);
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 3, keygroup));
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, keygroup));

		keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(element4), MAX_PARALLELISM);
		assertEquals(9, keygroup);
		assertEquals(2, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 3, keygroup));
		assertEquals(1, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, keygroup));

		// starting the test, we will go from parallelism of 3 to parallelism of 2

		// first operator
		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness1 =
			getTestHarness(MAX_PARALLELISM, 3, 0);
		testHarness1.open();

		// second operator
		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness2 =
			getTestHarness(MAX_PARALLELISM, 3, 1);
		testHarness2.open();

		// third operator
		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness3 =
			getTestHarness(MAX_PARALLELISM, 3, 2);
		testHarness3.open();

		testHarness1.processWatermark(Long.MIN_VALUE);
		testHarness2.processWatermark(Long.MIN_VALUE);
		testHarness3.processWatermark(Long.MIN_VALUE);

		testHarness1.processElement(new StreamRecord<>(element1));
		testHarness1.processElement(new StreamRecord<>(element3));

		testHarness2.processElement(new StreamRecord<>(element2));
		testHarness3.processElement(new StreamRecord<>(element4));

		// so far we only have the initial watermark
		assertEquals(1, testHarness1.getOutput().size());
		verifyWatermark(testHarness1.getOutput().poll(), Long.MIN_VALUE);

		assertEquals(1, testHarness2.getOutput().size());
		verifyWatermark(testHarness2.getOutput().poll(), Long.MIN_VALUE);

		assertEquals(1, testHarness3.getOutput().size());
		verifyWatermark(testHarness3.getOutput().poll(), Long.MIN_VALUE);

		// we take a snapshot and make it look as a single operator
		// this will be the initial state of all downstream tasks.
		OperatorStateHandles snapshot = AbstractStreamOperatorTestHarness.repackageState(
			testHarness2.snapshot(0, 0),
			testHarness1.snapshot(0, 0),
			testHarness3.snapshot(0, 0)
		);

		// first new operator
		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness4 =
			getTestHarness(MAX_PARALLELISM, 2, 0);
		testHarness4.setup();
		testHarness4.initializeState(snapshot);
		testHarness4.open();

		// second new operator
		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness5 =
			getTestHarness(MAX_PARALLELISM, 2, 1);
		testHarness5.setup();
		testHarness5.initializeState(snapshot);
		testHarness5.open();

		testHarness4.processWatermark(10);
		testHarness5.processWatermark(10);

		assertEquals(3, testHarness4.getOutput().size());
		verifyElement(testHarness4.getOutput().poll(), 7);
		verifyElement(testHarness4.getOutput().poll(), 90);
		verifyWatermark(testHarness4.getOutput().poll(), 10);

		assertEquals(3, testHarness5.getOutput().size());
		verifyElement(testHarness5.getOutput().poll(), 45);
		verifyElement(testHarness5.getOutput().poll(), 10);
		verifyWatermark(testHarness5.getOutput().poll(), 10);

		testHarness1.close();
		testHarness2.close();
		testHarness3.close();
		testHarness4.close();
		testHarness5.close();
	}

	@Test
	public void testWatermarkCallbackServiceKeyDeletion() throws Exception {
		final int MAX_PARALLELISM = 10;

		Tuple2<Integer, String> element1 = new Tuple2<>(7, "start");
		Tuple2<Integer, String> element2 = new Tuple2<>(45, "start");
		Tuple2<Integer, String> element3 = new Tuple2<>(90, "start");

		TestOperatorWithDeletingKeyCallback op = new TestOperatorWithDeletingKeyCallback(45);

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> testHarness1 =
			new KeyedOneInputStreamOperatorTestHarness<>(
				op,
				new TestKeySelector(),
				BasicTypeInfo.INT_TYPE_INFO,
				MAX_PARALLELISM,
				1,
				0);
		testHarness1.open();

		testHarness1.processElement(new StreamRecord<>(element1));
		testHarness1.processElement(new StreamRecord<>(element2));

		testHarness1.processWatermark(10L);

		assertEquals(3L, testHarness1.getOutput().size());
		verifyElement(testHarness1.getOutput().poll(), 7);
		verifyElement(testHarness1.getOutput().poll(), 45);
		verifyWatermark(testHarness1.getOutput().poll(), 10);

		testHarness1.processElement(new StreamRecord<>(element3));
		testHarness1.processWatermark(20L);

		// because at the first watermark the operator removed key 45
		assertEquals(3L, testHarness1.getOutput().size());
		verifyElement(testHarness1.getOutput().poll(), 7);
		verifyElement(testHarness1.getOutput().poll(), 90);
		verifyWatermark(testHarness1.getOutput().poll(), 20);

		testHarness1.processWatermark(25L);

		verifyElement(testHarness1.getOutput().poll(), 7);
		verifyElement(testHarness1.getOutput().poll(), 90);
		verifyWatermark(testHarness1.getOutput().poll(), 25);

		// unregister key and then fail
		op.unregisterKey(90);

		// take a snapshot with some elements in internal sorting queue
		OperatorStateHandles snapshot = testHarness1.snapshot(0, 0);
		testHarness1.close();

		testHarness1 = new KeyedOneInputStreamOperatorTestHarness<>(
				new TestOperatorWithDeletingKeyCallback(45),
				new TestKeySelector(),
				BasicTypeInfo.INT_TYPE_INFO,
				MAX_PARALLELISM,
				1,
				0);
		testHarness1.setup();
		testHarness1.initializeState(snapshot);
		testHarness1.open();

		testHarness1.processWatermark(30L);

		assertEquals(2L, testHarness1.getOutput().size());
		verifyElement(testHarness1.getOutput().poll(), 7);
		verifyWatermark(testHarness1.getOutput().poll(), 30);

		testHarness1.close();
	}

	private KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, Integer> getTestHarness(
			int maxParallelism, int noOfTasks, int taskIdx) throws Exception {

		return new KeyedOneInputStreamOperatorTestHarness<>(
			new TestOperatorWithCallback(),
			new TestKeySelector(),
			BasicTypeInfo.INT_TYPE_INFO,
			maxParallelism,
			noOfTasks, /* num subtasks */
			taskIdx /* subtask index */);
	}

	private void verifyWatermark(Object outputObject, long timestamp) {
		Assert.assertTrue(outputObject instanceof Watermark);
		assertEquals(timestamp, ((Watermark) outputObject).getTimestamp());
	}

	private void verifyElement(Object outputObject, int expected) {
		Assert.assertTrue(outputObject instanceof StreamRecord);

		StreamRecord<?> resultRecord = (StreamRecord<?>) outputObject;
		Assert.assertTrue(resultRecord.getValue() instanceof Integer);

		@SuppressWarnings("unchecked")
		int actual = (Integer) resultRecord.getValue();
		assertEquals(expected, actual);
	}

	/**
	 * Extracts the result values form the test harness and clear the output queue.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private <T> List<T> extractResult(OneInputStreamOperatorTestHarness<?, T> testHarness) {
		List<StreamRecord<? extends T>> streamRecords = testHarness.extractOutputStreamRecords();
		List<T> result = new ArrayList<>();
		for (Object in : streamRecords) {
			if (in instanceof StreamRecord) {
				result.add((T) ((StreamRecord) in).getValue());
			}
		}
		testHarness.getOutput().clear();
		return result;
	}

	private static class TestKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer getKey(Tuple2<Integer, String> value) throws Exception {
			return value.f0;
		}
	}

	private static class TestOperatorWithCallback
			extends AbstractStreamOperator<Integer>
			implements OneInputStreamOperator<Tuple2<Integer, String>, Integer> {

		private static final long serialVersionUID = 9215057823264582305L;

		@Override
		public void open() throws Exception {
			super.open();

			InternalWatermarkCallbackService<Integer> callbackService = getInternalWatermarkCallbackService();

			callbackService.setWatermarkCallback(new OnWatermarkCallback<Integer>() {

				@Override
				public void onWatermark(Integer integer, Watermark watermark) throws IOException {
					output.collect(new StreamRecord<>(integer));
				}
			}, IntSerializer.INSTANCE);
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
			getInternalWatermarkCallbackService().registerKeyForWatermarkCallback(element.getValue().f0);
		}
	}

	private static class TestOperatorWithDeletingKeyCallback
			extends AbstractStreamOperator<Integer>
			implements OneInputStreamOperator<Tuple2<Integer, String>, Integer> {

		private static final long serialVersionUID = 9215057823264582305L;

		private final int keyToDelete;

		public TestOperatorWithDeletingKeyCallback(int keyToDelete) {
			this.keyToDelete = keyToDelete;
		}

		@Override
		public void open() throws Exception {
			super.open();

			InternalWatermarkCallbackService<Integer> callbackService = getInternalWatermarkCallbackService();

			callbackService.setWatermarkCallback(new OnWatermarkCallback<Integer>() {

				@Override
				public void onWatermark(Integer integer, Watermark watermark) throws IOException {

					// this is to simulate the case where we may have a concurrent modification
					// exception as we iterate over the list of registered keys and we concurrently
					// delete the key.

					if (integer.equals(keyToDelete)) {
						getInternalWatermarkCallbackService().unregisterKeyFromWatermarkCallback(integer);
					}
					output.collect(new StreamRecord<>(integer));
				}
			}, IntSerializer.INSTANCE);
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
			getInternalWatermarkCallbackService().registerKeyForWatermarkCallback(element.getValue().f0);
		}

		public void unregisterKey(int key) {
			getInternalWatermarkCallbackService().unregisterKeyFromWatermarkCallback(key);
		}
	}

	/**
	 * Testing operator that can respond to commands by either setting/deleting state, emitting
	 * state or setting timers.
	 */
	private static class TestOperator
			extends AbstractStreamOperator<String>
			implements OneInputStreamOperator<Tuple2<Integer, String>, String>, Triggerable<Integer, VoidNamespace> {

		private static final long serialVersionUID = 1L;

		private transient InternalTimerService<VoidNamespace> timerService;

		private final ValueStateDescriptor<String> stateDescriptor =
				new ValueStateDescriptor<>("state", StringSerializer.INSTANCE);

		@Override
		public void open() throws Exception {
			super.open();

			this.timerService = getInternalTimerService(
					"test-timers",
					VoidNamespaceSerializer.INSTANCE,
					this);
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
			String[] command = element.getValue().f1.split(":");
			switch (command[0]) {
				case "SET_STATE":
					getPartitionedState(stateDescriptor).update(command[1]);
					break;
				case "DELETE_STATE":
					getPartitionedState(stateDescriptor).clear();
					break;
				case "SET_EVENT_TIME_TIMER":
					timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, Long.parseLong(command[1]));
					break;
				case "SET_PROC_TIME_TIMER":
					timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, Long.parseLong(command[1]));
					break;
				case "EMIT_STATE":
					String stateValue = getPartitionedState(stateDescriptor).value();
					output.collect(new StreamRecord<>("ON_ELEMENT:" + element.getValue().f0 + ":" + stateValue));
					break;
				default:
					throw new IllegalArgumentException();
			}
		}

		@Override
		public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
			String stateValue = getPartitionedState(stateDescriptor).value();
			output.collect(new StreamRecord<>("ON_EVENT_TIME:" + stateValue));
		}

		@Override
		public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
			String stateValue = getPartitionedState(stateDescriptor).value();
			output.collect(new StreamRecord<>("ON_PROC_TIME:" + stateValue));
		}
	}

	private static int getKeyInKeyGroupRange(KeyGroupRange range, int maxParallelism) {
		Random rand = new Random(System.currentTimeMillis());
		int result = rand.nextInt();
		while (!range.contains(KeyGroupRangeAssignment.assignToKeyGroup(result, maxParallelism))) {
			result = rand.nextInt();
		}
		return result;
	}
}
