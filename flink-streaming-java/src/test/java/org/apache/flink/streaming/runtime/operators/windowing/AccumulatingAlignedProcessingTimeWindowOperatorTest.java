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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"serial"})
public class AccumulatingAlignedProcessingTimeWindowOperatorTest {

	@SuppressWarnings("unchecked")
	private final WindowFunction<String, String, String, TimeWindow> mockFunction = mock(WindowFunction.class);

	@SuppressWarnings("unchecked")
	private final KeySelector<String, String> mockKeySelector = mock(KeySelector.class);
	
	private final KeySelector<Integer, Integer> identitySelector = new KeySelector<Integer, Integer>() {
		@Override
		public Integer getKey(Integer value) {
			return value;
		}
	};
	
	private final WindowFunction<Integer, Integer, Integer, TimeWindow> validatingIdentityFunction =
			new WindowFunction<Integer, Integer, Integer, TimeWindow>()
	{
		@Override
		public void apply(Integer key,
				TimeWindow window,
				Iterable<Integer> values,
				Collector<Integer> out) {
			for (Integer val : values) {
				assertEquals(key, val);
				out.collect(val);
			}
		}
	};

	// ------------------------------------------------------------------------

	public AccumulatingAlignedProcessingTimeWindowOperatorTest() {
		ClosureCleaner.clean(identitySelector, false);
		ClosureCleaner.clean(validatingIdentityFunction, false);
	}
	
	// ------------------------------------------------------------------------

	@After
	public void checkNoTriggerThreadsRunning() {
		// make sure that all the threads we trigger are shut down
		long deadline = System.currentTimeMillis() + 5000;
		while (StreamTask.TRIGGER_THREAD_GROUP.activeCount() > 0 && System.currentTimeMillis() < deadline) {
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException ignored) {}
		}

		assertTrue("Not all trigger threads where properly shut down",
				StreamTask.TRIGGER_THREAD_GROUP.activeCount() == 0);
	}
	
	// ------------------------------------------------------------------------
	
	@Test
	public void testInvalidParameters() {
		try {
			assertInvalidParameter(-1L, -1L);
			assertInvalidParameter(10000L, -1L);
			assertInvalidParameter(-1L, 1000L);
			assertInvalidParameter(1000L, 2000L);
			
			// actual internal slide is too low here:
			assertInvalidParameter(1000L, 999L);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testWindowSizeAndSlide() {
		try {
			AccumulatingProcessingTimeWindowOperator<String, String, String> op;
			
			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 5000, 1000);
			assertEquals(5000, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(1000, op.getPaneSize());
			assertEquals(5, op.getNumPanesPerWindow());

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1000, 1000);
			assertEquals(1000, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(1000, op.getPaneSize());
			assertEquals(1, op.getNumPanesPerWindow());

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1500, 1000);
			assertEquals(1500, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(500, op.getPaneSize());
			assertEquals(3, op.getNumPanesPerWindow());

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1200, 1100);
			assertEquals(1200, op.getWindowSize());
			assertEquals(1100, op.getWindowSlide());
			assertEquals(100, op.getPaneSize());
			assertEquals(12, op.getNumPanesPerWindow());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testWindowTriggerTimeAlignment() throws Exception {

		try {
			AccumulatingProcessingTimeWindowOperator<String, String, String> op =
					new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
							StringSerializer.INSTANCE, StringSerializer.INSTANCE, 5000, 1000);

			KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, mockKeySelector, BasicTypeInfo.STRING_TYPE_INFO);

			testHarness.open();

			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			testHarness.close();

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1000, 1000);

			testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, mockKeySelector, BasicTypeInfo.STRING_TYPE_INFO);

			testHarness.open();

			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			testHarness.close();


			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1500, 1000);

			testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, mockKeySelector, BasicTypeInfo.STRING_TYPE_INFO);

			testHarness.open();

			assertTrue(op.getNextSlideTime() % 500 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			testHarness.close();

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1200, 1100);

			testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, mockKeySelector, BasicTypeInfo.STRING_TYPE_INFO);

			testHarness.open();

			assertEquals(0, op.getNextSlideTime() % 100);
			assertEquals(0, op.getNextEvaluationTime() % 1100);
			testHarness.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTumblingWindow() throws Exception {
		try {
			final int windowSize = 50;

			// tumbling window that triggers every 20 milliseconds
			AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(
							validatingIdentityFunction, identitySelector,
							IntSerializer.INSTANCE, IntSerializer.INSTANCE,
							windowSize, windowSize);

			KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, identitySelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.open();

			final int numElements = 1000;

			long currentTime = 0;

			for (int i = 0; i < numElements; i++) {
				testHarness.processElement(new StreamRecord<>(i));
				currentTime = currentTime + 10;
				testHarness.setProcessingTime(currentTime);
			}


			List<Integer> result = extractFromStreamRecords(testHarness.extractOutputStreamRecords());
			assertEquals(numElements, result.size());

			Collections.sort(result);
			for (int i = 0; i < numElements; i++) {
				assertEquals(i, result.get(i).intValue());
			}

			testHarness.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingWindow() throws Exception {

		// tumbling window that triggers every 20 milliseconds
		AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> op =
				new AccumulatingProcessingTimeWindowOperator<>(
						validatingIdentityFunction, identitySelector,
						IntSerializer.INSTANCE, IntSerializer.INSTANCE, 150, 50);

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(op, identitySelector, BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		final int numElements = 1000;

		long currentTime = 0;

		for (int i = 0; i < numElements; i++) {
			testHarness.processElement(new StreamRecord<>(i));
			currentTime = currentTime + 10;
			testHarness.setProcessingTime(currentTime);
		}

		// get and verify the result
		List<Integer> result = extractFromStreamRecords(testHarness.extractOutputStreamRecords());

		// if we kept this running, each element would be in the result three times (for each slide).
		// we are closing the window before the final panes are through three times, so we may have less
		// elements.
		if (result.size() < numElements || result.size() > 3 * numElements) {
			fail("Wrong number of results: " + result.size());
		}

		Collections.sort(result);
		int lastNum = -1;
		int lastCount = -1;

		for (int num : result) {
			if (num == lastNum) {
				lastCount++;
				assertTrue(lastCount <= 3);
			}
			else {
				lastNum = num;
				lastCount = 1;
			}
		}

		testHarness.close();
	}

	@Test
	public void testTumblingWindowSingleElements() throws Exception {

		try {

			// tumbling window that triggers every 20 milliseconds
			AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(
							validatingIdentityFunction, identitySelector,
							IntSerializer.INSTANCE, IntSerializer.INSTANCE, 50, 50);

			KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, identitySelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.open();

			testHarness.setProcessingTime(0);

			testHarness.processElement(new StreamRecord<>(1));
			testHarness.processElement(new StreamRecord<>(2));

			testHarness.setProcessingTime(50);

			testHarness.processElement(new StreamRecord<>(3));
			testHarness.processElement(new StreamRecord<>(4));
			testHarness.processElement(new StreamRecord<>(5));

			testHarness.setProcessingTime(100);

			testHarness.processElement(new StreamRecord<>(6));

			testHarness.setProcessingTime(200);


			List<Integer> result = extractFromStreamRecords(testHarness.extractOutputStreamRecords());
			assertEquals(6, result.size());

			Collections.sort(result);
			assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), result);

			testHarness.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSlidingWindowSingleElements() throws Exception {
		try {

			// tumbling window that triggers every 20 milliseconds
			AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(
							validatingIdentityFunction, identitySelector,
							IntSerializer.INSTANCE, IntSerializer.INSTANCE, 150, 50);

			KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, identitySelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.setProcessingTime(0);

			testHarness.open();

			testHarness.processElement(new StreamRecord<>(1));
			testHarness.processElement(new StreamRecord<>(2));

			testHarness.setProcessingTime(50);
			testHarness.setProcessingTime(100);
			testHarness.setProcessingTime(150);

			List<Integer> result = extractFromStreamRecords(testHarness.extractOutputStreamRecords());

			assertEquals(6, result.size());
			
			Collections.sort(result);
			assertEquals(Arrays.asList(1, 1, 1, 2, 2, 2), result);

			testHarness.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void checkpointRestoreWithPendingWindowTumbling() {
		try {
			final int windowSize = 200;

			// tumbling window that triggers every 200 milliseconds
			AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(
							validatingIdentityFunction, identitySelector,
							IntSerializer.INSTANCE, IntSerializer.INSTANCE,
							windowSize, windowSize);

			OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
					new OneInputStreamOperatorTestHarness<>(op);

			testHarness.setup();
			testHarness.open();

			testHarness.setProcessingTime(0);

			// inject some elements
			final int numElementsFirst = 700;
			final int numElements = 1000;
			for (int i = 0; i < numElementsFirst; i++) {
				testHarness.processElement(new StreamRecord<>(i));
			}

			// draw a snapshot and dispose the window
			int beforeSnapShot = testHarness.getOutput().size();
			StreamStateHandle state = testHarness.snapshotLegacy(1L, System.currentTimeMillis());
			List<Integer> resultAtSnapshot = extractFromStreamRecords(testHarness.getOutput());
			int afterSnapShot = testHarness.getOutput().size();
			assertEquals("operator performed computation during snapshot", beforeSnapShot, afterSnapShot);
			assertTrue(afterSnapShot <= numElementsFirst);

			// inject some random elements, which should not show up in the state
			for (int i = 0; i < 300; i++) {
				testHarness.processElement(new StreamRecord<>(i + numElementsFirst));
			}

			testHarness.close();
			op.dispose();

			// re-create the operator and restore the state
			op = new AccumulatingProcessingTimeWindowOperator<>(
							validatingIdentityFunction, identitySelector,
							IntSerializer.INSTANCE, IntSerializer.INSTANCE,
							windowSize, windowSize);

			testHarness = new OneInputStreamOperatorTestHarness<>(op);

			testHarness.setup();
			testHarness.restore(state);
			testHarness.open();

			// inject some more elements
			for (int i = numElementsFirst; i < numElements; i++) {
				testHarness.processElement(new StreamRecord<>(i));
			}

			testHarness.setProcessingTime(400);

			// get and verify the result
			List<Integer> finalResult = new ArrayList<>();
			finalResult.addAll(resultAtSnapshot);
			List<Integer> finalPartialResult = extractFromStreamRecords(testHarness.getOutput());
			finalResult.addAll(finalPartialResult);
			assertEquals(numElements, finalResult.size());

			Collections.sort(finalResult);
			for (int i = 0; i < numElements; i++) {
				assertEquals(i, finalResult.get(i).intValue());
			}
			testHarness.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void checkpointRestoreWithPendingWindowSliding() {
		try {
			final int factor = 4;
			final int windowSlide = 50;
			final int windowSize = factor * windowSlide;

			// sliding window (200 msecs) every 50 msecs
			AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(
							validatingIdentityFunction, identitySelector,
							IntSerializer.INSTANCE, IntSerializer.INSTANCE,
							windowSize, windowSlide);

			OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
					new OneInputStreamOperatorTestHarness<>(op);

			testHarness.setProcessingTime(0);

			testHarness.setup();
			testHarness.open();

			// inject some elements
			final int numElements = 1000;
			final int numElementsFirst = 700;
			
			for (int i = 0; i < numElementsFirst; i++) {
				testHarness.processElement(new StreamRecord<>(i));
			}

			// draw a snapshot
			List<Integer> resultAtSnapshot = extractFromStreamRecords(testHarness.getOutput());
			int beforeSnapShot = testHarness.getOutput().size();
			StreamStateHandle state = testHarness.snapshotLegacy(1L, System.currentTimeMillis());
			int afterSnapShot = testHarness.getOutput().size();
			assertEquals("operator performed computation during snapshot", beforeSnapShot, afterSnapShot);

			assertTrue(resultAtSnapshot.size() <= factor * numElementsFirst);

			// inject the remaining elements - these should not influence the snapshot
			for (int i = numElementsFirst; i < numElements; i++) {
				testHarness.processElement(new StreamRecord<>(i));
			}

			testHarness.close();

			// re-create the operator and restore the state
			op = new AccumulatingProcessingTimeWindowOperator<>(
					validatingIdentityFunction, identitySelector,
					IntSerializer.INSTANCE, IntSerializer.INSTANCE,
					windowSize, windowSlide);

			testHarness = new OneInputStreamOperatorTestHarness<>(op);

			testHarness.setup();
			testHarness.restore(state);
			testHarness.open();


			// inject again the remaining elements
			for (int i = numElementsFirst; i < numElements; i++) {
				testHarness.processElement(new StreamRecord<>(i));
			}

			testHarness.setProcessingTime(50);
			testHarness.setProcessingTime(100);
			testHarness.setProcessingTime(150);
			testHarness.setProcessingTime(200);
			testHarness.setProcessingTime(250);
			testHarness.setProcessingTime(300);
			testHarness.setProcessingTime(350);

			// get and verify the result
			List<Integer> finalResult = new ArrayList<>(resultAtSnapshot);
			List<Integer> finalPartialResult = extractFromStreamRecords(testHarness.getOutput());
			finalResult.addAll(finalPartialResult);
			assertEquals(factor * numElements, finalResult.size());

			Collections.sort(finalResult);
			for (int i = 0; i < factor * numElements; i++) {
				assertEquals(i / factor, finalResult.get(i).intValue());
			}

			testHarness.close();
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testKeyValueStateInWindowFunction() {
		try {

			StatefulFunction.globalCounts.clear();
			
			// tumbling window that triggers every 20 milliseconds
			AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(
							new StatefulFunction(), identitySelector,
							IntSerializer.INSTANCE, IntSerializer.INSTANCE, 50, 50);

			OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, identitySelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.open();

			testHarness.setProcessingTime(0);

			testHarness.processElement(new StreamRecord<>(1));
			testHarness.processElement(new StreamRecord<>(2));

			op.processElement(new StreamRecord<>(1));
			op.processElement(new StreamRecord<>(2));
			op.processElement(new StreamRecord<>(1));
			op.processElement(new StreamRecord<>(1));
			op.processElement(new StreamRecord<>(2));
			op.processElement(new StreamRecord<>(2));

			testHarness.setProcessingTime(1000);

			List<Integer> result = extractFromStreamRecords(testHarness.getOutput());
			assertEquals(8, result.size());

			Collections.sort(result);
			assertEquals(Arrays.asList(1, 1, 1, 1, 2, 2, 2, 2), result);

			assertEquals(4, StatefulFunction.globalCounts.get(1).intValue());
			assertEquals(4, StatefulFunction.globalCounts.get(2).intValue());
			
			testHarness.close();
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// ------------------------------------------------------------------------
	
	private void assertInvalidParameter(long windowSize, long windowSlide) {
		try {
			new AccumulatingProcessingTimeWindowOperator<String, String, String>(
					mockFunction, mockKeySelector, 
					StringSerializer.INSTANCE, StringSerializer.INSTANCE,
					windowSize, windowSlide);
			fail("This should fail with an IllegalArgumentException");
		}
		catch (IllegalArgumentException e) {
			// expected
		}
		catch (Exception e) {
			fail("Wrong exception. Expected IllegalArgumentException but found " + e.getClass().getSimpleName());
		}
	}

	// ------------------------------------------------------------------------

	private static class StatefulFunction extends RichWindowFunction<Integer, Integer, Integer, TimeWindow> {

		// we use a concurrent map here even though there is no concurrency, to
		// get "volatile" style access to entries
		static final Map<Integer, Integer> globalCounts = new ConcurrentHashMap<>();

		private ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			assertNotNull(getRuntimeContext());
			state = getRuntimeContext().getState(
					new ValueStateDescriptor<>("totalCount", Integer.class, 0));
		}

		@Override
		public void apply(Integer key,
						  TimeWindow window,
						  Iterable<Integer> values,
						  Collector<Integer> out) throws Exception {
			for (Integer i : values) {
				// we need to update this state before emitting elements. Else, the test's main
				// thread will have received all output elements before the state is updated and
				// the checks may fail
				state.update(state.value() + 1);
				globalCounts.put(key, state.value());
				
				out.collect(i);
			}
		}
	}

	// ------------------------------------------------------------------------

	private static StreamTask<?, ?> createMockTask() {
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.STATE_BACKEND, "jobmanager");

		StreamTask<?, ?> task = mock(StreamTask.class);
		when(task.getAccumulatorMap()).thenReturn(new HashMap<String, Accumulator<?, ?>>());
		when(task.getName()).thenReturn("Test task name");
		when(task.getExecutionConfig()).thenReturn(new ExecutionConfig());

		final TaskManagerRuntimeInfo mockTaskManagerRuntimeInfo = mock(TaskManagerRuntimeInfo.class);
		when(mockTaskManagerRuntimeInfo.getConfiguration()).thenReturn(configuration);

		final Environment env = mock(Environment.class);
		when(env.getTaskInfo()).thenReturn(new TaskInfo("Test task name", 1, 0, 1, 0));
		when(env.getUserClassLoader()).thenReturn(AggregatingAlignedProcessingTimeWindowOperatorTest.class.getClassLoader());
		when(env.getMetricGroup()).thenReturn(new UnregisteredTaskMetricsGroup());
		when(env.getTaskManagerInfo()).thenReturn(new TestingTaskManagerRuntimeInfo());

		when(task.getEnvironment()).thenReturn(env);
		return task;
	}

	private static StreamTask<?, ?> createMockTaskWithTimer(
		final ProcessingTimeService timerService)
	{
		StreamTask<?, ?> mockTask = createMockTask();
		when(mockTask.getProcessingTimeService()).thenReturn(timerService);
		return mockTask;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private <T> List<T> extractFromStreamRecords(Iterable<?> input) {
		List<T> result = new ArrayList<>();
		for (Object in : input) {
			if (in instanceof StreamRecord) {
				result.add((T) ((StreamRecord) in).getValue());
			}
		}
		return result;
	}

	private static void shutdownTimerServiceAndWait(ProcessingTimeService timers) throws Exception {
		timers.shutdownService();

		while (!timers.isTerminated()) {
			Thread.sleep(2);
		}
	}
}
