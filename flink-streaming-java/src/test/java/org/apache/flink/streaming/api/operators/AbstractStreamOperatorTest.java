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

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.RunnableFuture;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Tests for the facilities provided by {@link AbstractStreamOperator}. This mostly
 * tests timers and state and whether they are correctly checkpointed/restored
 * with key-group reshuffling.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AbstractStreamOperator.class)
@PowerMockIgnore({"java.*", "javax.*", "org.slf4j.*", "org.apache.log4j.*"})
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

		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

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
		final int maxParallelism = 10;

		// first get two keys that will fall into different key-group ranges that go
		// to different operator subtasks when we restore

		// get two sub key-ranges so that we can restore two ranges separately
		KeyGroupRange subKeyGroupRange1 = new KeyGroupRange(0, (maxParallelism / 2) - 1);
		KeyGroupRange subKeyGroupRange2 = new KeyGroupRange(subKeyGroupRange1.getEndKeyGroup() + 1, maxParallelism - 1);

		// get two different keys, one per sub range
		int key1 = getKeyInKeyGroupRange(subKeyGroupRange1, maxParallelism);
		int key2 = getKeyInKeyGroupRange(subKeyGroupRange2, maxParallelism);

		TestOperator testOperator = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						testOperator,
						new TestKeySelector(),
						BasicTypeInfo.INT_TYPE_INFO,
						maxParallelism,
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

		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

		// now, restore in two operators, first operator 1
		OperatorSubtaskState initState1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot, maxParallelism, 1, 2, 0);

		TestOperator testOperator1 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness1 =
				new KeyedOneInputStreamOperatorTestHarness<>(
						testOperator1,
						new TestKeySelector(),
						BasicTypeInfo.INT_TYPE_INFO,
						maxParallelism,
						2, /* num subtasks */
						0 /* subtask index */);

		testHarness1.setup();
		testHarness1.initializeState(initState1);
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
		OperatorSubtaskState initState2 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot, maxParallelism, 1, 2, 1);

		TestOperator testOperator2 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness2 =
				new KeyedOneInputStreamOperatorTestHarness<>(
						testOperator2,
						new TestKeySelector(),
						BasicTypeInfo.INT_TYPE_INFO,
						maxParallelism,
						2, /* num subtasks */
						1 /* subtask index */);

		testHarness2.setup();
		testHarness2.initializeState(initState2);
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
		final int maxParallelism = 10;

		// first get two keys that will fall into different key-group ranges that go
		// to different operator subtasks when we restore

		// get two sub key-ranges so that we can restore two ranges separately
		KeyGroupRange subKeyGroupRange1 = new KeyGroupRange(0, (maxParallelism / 2) - 1);
		KeyGroupRange subKeyGroupRange2 = new KeyGroupRange(subKeyGroupRange1.getEndKeyGroup() + 1, maxParallelism - 1);

		// get two different keys, one per sub range
		int key1 = getKeyInKeyGroupRange(subKeyGroupRange1, maxParallelism);
		int key2 = getKeyInKeyGroupRange(subKeyGroupRange2, maxParallelism);

		TestOperator testOperator1 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness1 =
			new KeyedOneInputStreamOperatorTestHarness<>(
				testOperator1,
				new TestKeySelector(),
				BasicTypeInfo.INT_TYPE_INFO,
				maxParallelism,
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
				maxParallelism,
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

		OperatorSubtaskState repackagedState =
			AbstractStreamOperatorTestHarness.repackageState(
				testHarness1.snapshot(0, 0),
				testHarness2.snapshot(0, 0)
			);

		OperatorSubtaskState initSubTaskState =
			AbstractStreamOperatorTestHarness.repartitionOperatorState(repackagedState, maxParallelism, 2, 1, 0);

		// now, for the third operator that scales down from parallelism of 2 to 1
		TestOperator testOperator3 = new TestOperator();

		KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String> testHarness3 =
			new KeyedOneInputStreamOperatorTestHarness<>(
				testOperator3,
				new TestKeySelector(),
				BasicTypeInfo.INT_TYPE_INFO,
				maxParallelism,
				1, /* num subtasks */
				0 /* subtask index */);

		testHarness3.setup();
		testHarness3.initializeState(initSubTaskState);
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

		StateSnapshotContextSynchronousImpl context = spy(new StateSnapshotContextSynchronousImpl(0L, 0L));

		whenNew(StateSnapshotContextSynchronousImpl.class).withAnyArguments().thenReturn(context);

		StreamTask<Void, AbstractStreamOperator<Void>> containingTask = mock(StreamTask.class);
		when(containingTask.getCancelables()).thenReturn(closeableRegistry);

		AbstractStreamOperator<Void> operator = mock(AbstractStreamOperator.class);
		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class), any(CheckpointStreamFactory.class))).thenCallRealMethod();
		doReturn(containingTask).when(operator).getContainingTask();

		operator.snapshotState(
				checkpointId,
				timestamp,
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				new MemCheckpointStreamFactory(Integer.MAX_VALUE));

	}

	/**
	 * Tests that the created StateSnapshotContextSynchronousImpl is closed in case of a failing
	 * Operator#snapshotState(StateSnapshotContextSynchronousImpl) call.
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
		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class), any(CheckpointStreamFactory.class))).thenCallRealMethod();
		doReturn(containingTask).when(operator).getContainingTask();

		// lets fail when calling the actual snapshotState method
		doThrow(failingException).when(operator).snapshotState(eq(context));

		try {
			operator.snapshotState(
					checkpointId,
					timestamp,
					CheckpointOptions.forCheckpointWithDefaultLocation(),
					new MemCheckpointStreamFactory(Integer.MAX_VALUE));
			fail("Exception expected.");
		} catch (Exception e) {
			assertEquals(failingException, e.getCause());
		}
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

		RunnableFuture<SnapshotResult<KeyedStateHandle>> futureKeyedStateHandle = mock(RunnableFuture.class);
		RunnableFuture<SnapshotResult<OperatorStateHandle>> futureOperatorStateHandle = mock(RunnableFuture.class);

		StateSnapshotContextSynchronousImpl context = spy(new StateSnapshotContextSynchronousImpl(checkpointId, timestamp));
		when(context.getKeyedStateStreamFuture()).thenReturn(futureKeyedStateHandle);
		when(context.getOperatorStateStreamFuture()).thenReturn(futureOperatorStateHandle);

		OperatorSnapshotFutures operatorSnapshotResult = spy(new OperatorSnapshotFutures());

		whenNew(StateSnapshotContextSynchronousImpl.class)
			.withArguments(
				anyLong(),
				anyLong(),
				any(CheckpointStreamFactory.class),
				nullable(KeyGroupRange.class),
				any(CloseableRegistry.class))
			.thenReturn(context);
		whenNew(OperatorSnapshotFutures.class).withAnyArguments().thenReturn(operatorSnapshotResult);

		StreamTask<Void, AbstractStreamOperator<Void>> containingTask = mock(StreamTask.class);
		when(containingTask.getCancelables()).thenReturn(closeableRegistry);

		AbstractStreamOperator<Void> operator = mock(AbstractStreamOperator.class);
		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class), any(CheckpointStreamFactory.class))).thenCallRealMethod();

		doCallRealMethod().when(operator).close();
		doCallRealMethod().when(operator).dispose();

		doReturn(containingTask).when(operator).getContainingTask();

		RunnableFuture<SnapshotResult<OperatorStateHandle>> futureManagedOperatorStateHandle = mock(RunnableFuture.class);

		OperatorStateBackend operatorStateBackend = mock(OperatorStateBackend.class);
		when(operatorStateBackend.snapshot(
			eq(checkpointId),
			eq(timestamp),
			any(CheckpointStreamFactory.class),
			any(CheckpointOptions.class))).thenReturn(futureManagedOperatorStateHandle);

		AbstractKeyedStateBackend<?> keyedStateBackend = mock(AbstractKeyedStateBackend.class);
		when(keyedStateBackend.snapshot(
			eq(checkpointId),
			eq(timestamp),
			any(CheckpointStreamFactory.class),
			eq(CheckpointOptions.forCheckpointWithDefaultLocation()))).thenThrow(failingException);

		closeableRegistry.registerCloseable(operatorStateBackend);
		closeableRegistry.registerCloseable(keyedStateBackend);

		Whitebox.setInternalState(operator, "operatorStateBackend", operatorStateBackend);
		Whitebox.setInternalState(operator, "keyedStateBackend", keyedStateBackend);

		try {
			operator.snapshotState(
					checkpointId,
					timestamp,
					CheckpointOptions.forCheckpointWithDefaultLocation(),
					new MemCheckpointStreamFactory(Integer.MAX_VALUE));
			fail("Exception expected.");
		} catch (Exception e) {
			assertEquals(failingException, e.getCause());
		}

		// verify that the context has been closed, the operator snapshot result has been cancelled
		// and that all futures have been cancelled.
		verify(operatorSnapshotResult).cancel();

		verify(futureKeyedStateHandle).cancel(anyBoolean());
		verify(futureOperatorStateHandle).cancel(anyBoolean());
		verify(futureKeyedStateHandle).cancel(anyBoolean());

		operator.close();

		operator.dispose();

		verify(operatorStateBackend).close();
		verify(keyedStateBackend).close();
		verify(operatorStateBackend).dispose();
		verify(keyedStateBackend).dispose();
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
