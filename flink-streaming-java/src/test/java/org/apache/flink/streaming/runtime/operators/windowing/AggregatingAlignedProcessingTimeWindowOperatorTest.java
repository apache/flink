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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import org.junit.After;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"serial", "SynchronizationOnLocalVariableOrMethodParameter"})
public class AggregatingAlignedProcessingTimeWindowOperatorTest {

	@SuppressWarnings("unchecked")
	private final ReduceFunction<String> mockFunction = mock(ReduceFunction.class);

	@SuppressWarnings("unchecked")
	private final KeySelector<String, String> mockKeySelector = mock(KeySelector.class);
	
	private final KeySelector<Tuple2<Integer, Integer>, Integer> fieldOneSelector = 
			new KeySelector<Tuple2<Integer,Integer>, Integer>() {
				@Override
				public Integer getKey(Tuple2<Integer,Integer> value) {
					return value.f0;
				}
	};
	
	private final ReduceFunction<Tuple2<Integer, Integer>> sumFunction = new ReduceFunction<Tuple2<Integer, Integer>>() {
		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) {
			return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
		}
	};
	
	private final TypeSerializer<Tuple2<Integer, Integer>> tupleSerializer = 
			new TupleTypeInfo<Tuple2<Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
					.createSerializer(new ExecutionConfig());

	private final Comparator<Tuple2<Integer, Integer>> tupleComparator = new Comparator<Tuple2<Integer, Integer>>() {
		@Override
		public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
			int diff0 = o1.f0 - o2.f0;
			int diff1 = o1.f1 - o2.f1;
			return diff0 != 0 ? diff0 : diff1;
		}
	};
	
	// ------------------------------------------------------------------------

	public AggregatingAlignedProcessingTimeWindowOperatorTest() {
		ClosureCleaner.clean(fieldOneSelector, false);
		ClosureCleaner.clean(sumFunction, false);
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
			AggregatingProcessingTimeWindowOperator<String, String> op;
			
			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 5000, 1000);
			assertEquals(5000, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(1000, op.getPaneSize());
			assertEquals(5, op.getNumPanesPerWindow());

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1000, 1000);
			assertEquals(1000, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(1000, op.getPaneSize());
			assertEquals(1, op.getNumPanesPerWindow());

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1500, 1000);
			assertEquals(1500, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(500, op.getPaneSize());
			assertEquals(3, op.getNumPanesPerWindow());

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
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
	public void testWindowTriggerTimeAlignment() {
		try {
			@SuppressWarnings("unchecked")
			final Output<StreamRecord<String>> mockOut = mock(Output.class);
			final StreamTask<?, ?> mockTask = createMockTask();
			
			AggregatingProcessingTimeWindowOperator<String, String> op;

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 5000, 1000);
			op.setup(mockTask, new StreamConfig(new Configuration()), mockOut);
			op.open();
			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1000, 1000);
			op.setup(mockTask, new StreamConfig(new Configuration()), mockOut);
			op.open();
			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1500, 1000);
			op.setup(mockTask, new StreamConfig(new Configuration()), mockOut);
			op.open();
			assertTrue(op.getNextSlideTime() % 500 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1200, 1100);
			op.setup(mockTask, new StreamConfig(new Configuration()), mockOut);
			op.open();
			assertTrue(op.getNextSlideTime() % 100 == 0);
			assertTrue(op.getNextEvaluationTime() % 1100 == 0);
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTumblingWindowUniqueElements() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
		try {
			final int windowSize = 50;
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>(windowSize);
			
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							windowSize, windowSize);
			
			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);
			
			op.setup(mockTask, new StreamConfig(new Configuration()), out);
			op.open();

			final int numElements = 1000;

			for (int i = 0; i < numElements; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
				Thread.sleep(1);
			}

			synchronized (lock) {
				op.close();
			}
			op.dispose();

			// get and verify the result
			List<Tuple2<Integer, Integer>> result = out.getElements();
			assertEquals(numElements, result.size());

			Collections.sort(result, tupleComparator);
			for (int i = 0; i < numElements; i++) {
				assertEquals(i, result.get(i).f0.intValue());
				assertEquals(i, result.get(i).f1.intValue());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdownNow();
		}
	}

	@Test
	public void  testTumblingWindowDuplicateElements() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();

		try {
			final int windowSize = 50;
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>(windowSize);

			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);

			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							windowSize, windowSize);
			
			op.setup(mockTask, new StreamConfig(new Configuration()), out);
			op.open();

			final int numWindows = 10;

			long previousNextTime = 0;
			int window = 1;
			
			while (window <= numWindows) {
				synchronized (lock) {
					long nextTime = op.getNextEvaluationTime();
					int val = ((int) nextTime) ^ ((int) (nextTime >>> 32));

					StreamRecord<Tuple2<Integer, Integer>> next =  new StreamRecord<>(new Tuple2<>(val, val));
					op.setKeyContextElement(next);
					op.processElement(next);

					if (nextTime != previousNextTime) {
						window++;
						previousNextTime = nextTime;
					}
				}
				Thread.sleep(1);
			}

			synchronized (lock) {
				op.close();
			}
			op.dispose();
			
			List<Tuple2<Integer, Integer>> result = out.getElements();
			
			// we have ideally one element per window. we may have more, when we emitted a value into the
			// successive window (corner case), so we can have twice the number of elements, in the worst case.
			assertTrue(result.size() >= numWindows && result.size() <= 2 * numWindows);

			// deduplicate for more accurate checks
			HashSet<Tuple2<Integer, Integer>> set = new HashSet<>(result);
			assertTrue(set.size() == 10);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdown();
		}
	}

	@Test
	public void testSlidingWindow() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
		try {
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>(50);

			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);

			// tumbling window that triggers every 20 milliseconds
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							150, 50);

			op.setup(mockTask, new StreamConfig(new Configuration()), out);
			op.open();

			final int numElements = 1000;

			for (int i = 0; i < numElements; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
				Thread.sleep(1);
			}

			synchronized (lock) {
				op.close();
			}
			op.dispose();

			// get and verify the result
			List<Tuple2<Integer, Integer>> result = out.getElements();
			
			// every element can occur between one and three times
			if (result.size() < numElements || result.size() > 3 * numElements) {
				System.out.println(result);
				fail("Wrong number of results: " + result.size());
			}

			Collections.sort(result, tupleComparator);
			int lastNum = -1;
			int lastCount = -1;
			
			for (Tuple2<Integer, Integer> val : result) {
				assertEquals(val.f0, val.f1);
				
				if (val.f0 == lastNum) {
					lastCount++;
					assertTrue(lastCount <= 3);
				}
				else {
					lastNum = val.f0;
					lastCount = 1;
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdownNow();
		}
	}

	@Test
	public void testSlidingWindowSingleElements() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();

		try {
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>(50);
			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);

			// tumbling window that triggers every 20 milliseconds
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer, 150, 50);

			op.setup(mockTask, new StreamConfig(new Configuration()), out);
			op.open();

			synchronized (lock) {
				StreamRecord<Tuple2<Integer, Integer>> next1 = new StreamRecord<>(new Tuple2<>(1, 1));
				op.setKeyContextElement(next1);
				op.processElement(next1);
				
				StreamRecord<Tuple2<Integer, Integer>> next2 = new StreamRecord<>(new Tuple2<>(2, 2));
				op.setKeyContextElement(next2);
				op.processElement(next2);
			}

			// each element should end up in the output three times
			// wait until the elements have arrived 6 times in the output
			out.waitForNElements(6, 120000);
			
			List<Tuple2<Integer, Integer>> result = out.getElements();
			assertEquals(6, result.size());
			
			Collections.sort(result, tupleComparator);
			assertEquals(Arrays.asList(
					new Tuple2<>(1, 1),
					new Tuple2<>(1, 1),
					new Tuple2<>(1, 1),
					new Tuple2<>(2, 2),
					new Tuple2<>(2, 2),
					new Tuple2<>(2, 2)
			), result);

			synchronized (lock) {
				op.close();
			}
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdown();
		}
	}
	
	@Test
	public void testEmitTrailingDataOnClose() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
		try {
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>();
			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);
			
			// the operator has a window time that is so long that it will not fire in this test
			final long oneYear = 365L * 24 * 60 * 60 * 1000;
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer, oneYear, oneYear);

			op.setup(mockTask, new StreamConfig(new Configuration()), out);
			op.open();
			
			List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
			for (Integer i : data) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
			}

			synchronized (lock) {
				op.close();
			}
			op.dispose();
			
			// get and verify the result
			List<Tuple2<Integer, Integer>> result = out.getElements();
			assertEquals(data.size(), result.size());
			
			Collections.sort(result, tupleComparator);
			for (int i = 0; i < data.size(); i++) {
				assertEquals(data.get(i), result.get(i).f0);
				assertEquals(data.get(i), result.get(i).f1);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdown();
		}
	}

	@Test
	public void testPropagateExceptionsFromProcessElement() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
		try {
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>();
			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);

			ReduceFunction<Tuple2<Integer, Integer>> failingFunction = new FailingFunction(100);

			// the operator has a window time that is so long that it will not fire in this test
			final long hundredYears = 100L * 365 * 24 * 60 * 60 * 1000;
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							failingFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							hundredYears, hundredYears);

			op.setup(mockTask, new StreamConfig(new Configuration()), out);
			op.open();

			for (int i = 0; i < 100; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(1, 1)); 
					op.setKeyContextElement(next);
					op.processElement(next);
				}
			}
			
			try {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(1, 1));
				op.setKeyContextElement(next);
				op.processElement(next);
				fail("This fail with an exception");
			}
			catch (Exception e) {
				assertTrue(e.getMessage().contains("Artificial Test Exception"));
			}

			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdown();
		}
	}

	@Test
	public void checkpointRestoreWithPendingWindowTumbling() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
		try {
			final int windowSize = 200;
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>(windowSize);
			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);

			// tumbling window that triggers every 50 milliseconds
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							windowSize, windowSize);

			op.setup(mockTask, new StreamConfig(new Configuration()), out);
			op.open();

			// inject some elements
			final int numElementsFirst = 700;
			final int numElements = 1000;
			
			for (int i = 0; i < numElementsFirst; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
				Thread.sleep(1);
			}

			// draw a snapshot and dispose the window
			StreamTaskState state;
			List<Tuple2<Integer, Integer>> resultAtSnapshot;
			synchronized (lock) {
				int beforeSnapShot = out.getElements().size();
				state = op.snapshotOperatorState(1L, System.currentTimeMillis());
				resultAtSnapshot = new ArrayList<>(out.getElements());
				int afterSnapShot = out.getElements().size();
				assertEquals("operator performed computation during snapshot", beforeSnapShot, afterSnapShot);
			}
			
			assertTrue(resultAtSnapshot.size() <= numElementsFirst);

			// inject some random elements, which should not show up in the state
			for (int i = numElementsFirst; i < numElements; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
				Thread.sleep(1);
			}

			op.dispose();

			// re-create the operator and restore the state
			final CollectingOutput<Tuple2<Integer, Integer>> out2 = new CollectingOutput<>(windowSize);
			op = new AggregatingProcessingTimeWindowOperator<>(
					sumFunction, fieldOneSelector,
					IntSerializer.INSTANCE, tupleSerializer,
					windowSize, windowSize);

			op.setup(mockTask, new StreamConfig(new Configuration()), out2);
			op.restoreState(state, 1);
			op.open();

			// inject the remaining elements
			for (int i = numElementsFirst; i < numElements; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
				Thread.sleep(1);
			}

			synchronized (lock) {
				op.close();
			}
			op.dispose();

			// get and verify the result
			List<Tuple2<Integer, Integer>> finalResult = new ArrayList<>(resultAtSnapshot);
			finalResult.addAll(out2.getElements());
			assertEquals(numElements, finalResult.size());

			Collections.sort(finalResult, tupleComparator);
			for (int i = 0; i < numElements; i++) {
				assertEquals(i, finalResult.get(i).f0.intValue());
				assertEquals(i, finalResult.get(i).f1.intValue());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdown();
		}
	}

	@Test
	public void checkpointRestoreWithPendingWindowSliding() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
		try {
			final int factor = 4;
			final int windowSlide = 50;
			final int windowSize = factor * windowSlide;

			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>(windowSlide);
			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);

			// sliding window (200 msecs) every 50 msecs
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							windowSize, windowSlide);

			op.setup(mockTask, new StreamConfig(new Configuration()), out);
			op.open();

			// inject some elements
			final int numElements = 1000;
			final int numElementsFirst = 700;

			for (int i = 0; i < numElementsFirst; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
				Thread.sleep(1);
			}

			// draw a snapshot
			StreamTaskState state;
			List<Tuple2<Integer, Integer>> resultAtSnapshot;
			synchronized (lock) {
				int beforeSnapShot = out.getElements().size();
				state = op.snapshotOperatorState(1L, System.currentTimeMillis());
				resultAtSnapshot = new ArrayList<>(out.getElements());
				int afterSnapShot = out.getElements().size();
				assertEquals("operator performed computation during snapshot", beforeSnapShot, afterSnapShot);
			}

			assertTrue(resultAtSnapshot.size() <= factor * numElementsFirst);

			// inject the remaining elements - these should not influence the snapshot
			for (int i = numElementsFirst; i < numElements; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
				Thread.sleep(1);
			}

			op.dispose();

			// re-create the operator and restore the state
			final CollectingOutput<Tuple2<Integer, Integer>> out2 = new CollectingOutput<>(windowSlide);
			op = new AggregatingProcessingTimeWindowOperator<>(
					sumFunction, fieldOneSelector,
					IntSerializer.INSTANCE, tupleSerializer,
					windowSize, windowSlide);

			op.setup(mockTask, new StreamConfig(new Configuration()), out2);
			op.restoreState(state, 1);
			op.open();


			// inject again the remaining elements
			for (int i = numElementsFirst; i < numElements; i++) {
				synchronized (lock) {
					StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
					op.setKeyContextElement(next);
					op.processElement(next);
				}
				Thread.sleep(1);
			}

			// for a deterministic result, we need to wait until all pending triggers
			// have fired and emitted their results
			long deadline = System.currentTimeMillis() + 120000;
			do {
				Thread.sleep(20);
			}
			while (resultAtSnapshot.size() + out2.getElements().size() < factor * numElements
					&& System.currentTimeMillis() < deadline);

			synchronized (lock) {
				op.close();
			}
			op.dispose();

			// get and verify the result
			List<Tuple2<Integer, Integer>> finalResult = new ArrayList<>(resultAtSnapshot);
			finalResult.addAll(out2.getElements());
			assertEquals(factor * numElements, finalResult.size());

			Collections.sort(finalResult, tupleComparator);
			for (int i = 0; i < factor * numElements; i++) {
				assertEquals(i / factor, finalResult.get(i).f0.intValue());
				assertEquals(i / factor, finalResult.get(i).f1.intValue());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdown();
		}
	}

	@Test
	public void testKeyValueStateInWindowFunctionTumbling() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
		try {
			final long hundredYears = 100L * 365 * 24 * 60 * 60 * 1000;
			
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>();
			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);

			StatefulFunction.globalCounts.clear();
			
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							new StatefulFunction(), fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer, hundredYears, hundredYears);

			op.setup(mockTask, createTaskConfig(fieldOneSelector, IntSerializer.INSTANCE), out);
			op.open();

			// because the window interval is so large, everything should be in one window
			// and aggregate into one value per key
			
			synchronized (lock) {
				for (int i = 0; i < 10; i++) {
					StreamRecord<Tuple2<Integer, Integer>> next1 = new StreamRecord<>(new Tuple2<>(1, i));
					op.setKeyContextElement(next1);
					op.processElement(next1);
	
					StreamRecord<Tuple2<Integer, Integer>> next2 = new StreamRecord<>(new Tuple2<>(2, i));
					op.setKeyContextElement(next2);
					op.processElement(next2);
				}

				op.close();
			}

			List<Tuple2<Integer, Integer>> result = out.getElements();
			assertEquals(2, result.size());

			Collections.sort(result, tupleComparator);
			assertEquals(45, result.get(0).f1.intValue());
			assertEquals(45, result.get(1).f1.intValue());

			assertEquals(10, StatefulFunction.globalCounts.get(1).intValue());
			assertEquals(10, StatefulFunction.globalCounts.get(2).intValue());
		
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdown();
		}
	}

	@Test
	public void testKeyValueStateInWindowFunctionSliding() {
		final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
		try {
			final int factor = 2;
			final int windowSlide = 50;
			final int windowSize = factor * windowSlide;
			
			final CollectingOutput<Tuple2<Integer, Integer>> out = new CollectingOutput<>();
			final Object lock = new Object();
			final StreamTask<?, ?> mockTask = createMockTaskWithTimer(timerService, lock);

			StatefulFunction.globalCounts.clear();
			
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							new StatefulFunction(), fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer, windowSize, windowSlide);

			op.setup(mockTask, createTaskConfig(fieldOneSelector, IntSerializer.INSTANCE), out);
			op.open();

			// because the window interval is so large, everything should be in one window
			// and aggregate into one value per key
			final int numElements = 100;
			
			// because we do not release the lock here, these elements
			for (int i = 0; i < numElements; i++) {
				
				StreamRecord<Tuple2<Integer, Integer>> next1 = new StreamRecord<>(new Tuple2<>(1, i));
				StreamRecord<Tuple2<Integer, Integer>> next2 = new StreamRecord<>(new Tuple2<>(2, i));
				StreamRecord<Tuple2<Integer, Integer>> next3 = new StreamRecord<>(new Tuple2<>(1, i));
				StreamRecord<Tuple2<Integer, Integer>> next4 = new StreamRecord<>(new Tuple2<>(2, i));

				// because we do not release the lock between elements, they end up in the same windows
				synchronized (lock) {
					op.setKeyContextElement(next1);
					op.processElement(next1);
					op.setKeyContextElement(next2);
					op.processElement(next2);
					op.setKeyContextElement(next3);
					op.processElement(next3);
					op.setKeyContextElement(next4);
					op.processElement(next4);
				}

				Thread.sleep(1);
			}
			
			synchronized (lock) {
				op.close();
			}

			int count1 = StatefulFunction.globalCounts.get(1);
			int count2 = StatefulFunction.globalCounts.get(2);
			
			assertTrue(count1 >= 2 && count1 <= 2 * numElements);
			assertEquals(count1, count2);
			
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			timerService.shutdown();
		}
	}
	
	// ------------------------------------------------------------------------
	
	private void assertInvalidParameter(long windowSize, long windowSlide) {
		try {
			new AggregatingProcessingTimeWindowOperator<String, String>(
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
	
	private static class FailingFunction implements ReduceFunction<Tuple2<Integer, Integer>> {

		private final int failAfterElements;
		
		private int numElements;

		FailingFunction(int failAfterElements) {
			this.failAfterElements = failAfterElements;
		}

		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
			numElements++;

			if (numElements >= failAfterElements) {
				throw new Exception("Artificial Test Exception");
			}
			
			return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
		}
	}

	// ------------------------------------------------------------------------

	private static class StatefulFunction extends RichReduceFunction<Tuple2<Integer, Integer>> {

		static final Map<Integer, Integer> globalCounts = new ConcurrentHashMap<>();

		private OperatorState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			assertNotNull(getRuntimeContext());
			
			// start with one, so the final count is correct and we test that we do not
			// initialize with 0 always by default
			state = getRuntimeContext().getKeyValueState("totalCount", Integer.class, 1);
		}

		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
			state.update(state.value() + 1);
			globalCounts.put(value1.f0, state.value());
			
			return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
		}
	}

	// ------------------------------------------------------------------------
	
	private static StreamTask<?, ?> createMockTask() {
		StreamTask<?, ?> task = mock(StreamTask.class);
		when(task.getAccumulatorMap()).thenReturn(new HashMap<String, Accumulator<?, ?>>());
		when(task.getName()).thenReturn("Test task name");
		when(task.getExecutionConfig()).thenReturn(new ExecutionConfig());

		Environment env = mock(Environment.class);
		when(env.getTaskInfo()).thenReturn(new TaskInfo("Test task name", 0, 1, 0));
		when(env.getUserClassLoader()).thenReturn(AggregatingAlignedProcessingTimeWindowOperatorTest.class.getClassLoader());
		
		when(task.getEnvironment()).thenReturn(env);

		// ugly java generic hacks to get the state backend into the mock
		@SuppressWarnings("unchecked")
		OngoingStubbing<StateBackend<?>> stubbing =
				(OngoingStubbing<StateBackend<?>>) (OngoingStubbing<?>) when(task.getStateBackend());
		stubbing.thenReturn(MemoryStateBackend.defaultInstance());
		
		return task;
	}

	private static StreamTask<?, ?> createMockTaskWithTimer(
			final ScheduledExecutorService timerService, final Object lock)
	{
		StreamTask<?, ?> mockTask = createMockTask();

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				final Long timestamp = (Long) invocationOnMock.getArguments()[0];
				final Triggerable target = (Triggerable) invocationOnMock.getArguments()[1];
				timerService.schedule(
						new Callable<Object>() {
							@Override
							public Object call() throws Exception {
								synchronized (lock) {
									target.trigger(timestamp);
								}
								return null;
							}
						},
						timestamp - System.currentTimeMillis(),
						TimeUnit.MILLISECONDS);
				return null;
			}
		}).when(mockTask).registerTimer(anyLong(), any(Triggerable.class));
		
		return mockTask;
	}

	private static StreamConfig createTaskConfig(KeySelector<?, ?> partitioner, TypeSerializer<?> keySerializer) {
		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStatePartitioner(partitioner);
		cfg.setStateKeySerializer(keySerializer);
		return cfg;
	}
}
