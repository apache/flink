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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@SuppressWarnings("serial")
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
	public void testWindowTriggerTimeAlignment() throws Exception {
		try {

			AggregatingProcessingTimeWindowOperator<String, String> op =
					new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
						StringSerializer.INSTANCE, StringSerializer.INSTANCE, 5000, 1000);

			KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, mockKeySelector, BasicTypeInfo.STRING_TYPE_INFO);
			testHarness.open();

			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			testHarness.close();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1000, 1000);

			testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, mockKeySelector, BasicTypeInfo.STRING_TYPE_INFO);
			testHarness.open();

			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			testHarness.close();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1500, 1000);

			testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, mockKeySelector, BasicTypeInfo.STRING_TYPE_INFO);
			testHarness.open();

			assertTrue(op.getNextSlideTime() % 500 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			testHarness.close();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector,
					StringSerializer.INSTANCE, StringSerializer.INSTANCE, 1200, 1100);

			testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, mockKeySelector, BasicTypeInfo.STRING_TYPE_INFO);
			testHarness.open();

			assertTrue(op.getNextSlideTime() % 100 == 0);
			assertTrue(op.getNextEvaluationTime() % 1100 == 0);
			testHarness.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTumblingWindowUniqueElements() throws Exception {

		try {
			final int windowSize = 50;

			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							windowSize, windowSize);

			KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, fieldOneSelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.open();

			final int numElements = 1000;

			long currentTime = 0;

			for (int i = 0; i < numElements; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
				testHarness.processElement(next);
				currentTime = currentTime + 10;
				testHarness.setProcessingTime(currentTime);
			}

			// get and verify the result
			List<Tuple2<Integer, Integer>> result = extractFromStreamRecords(testHarness.extractOutputStreamRecords());
			assertEquals(numElements, result.size());

			testHarness.close();

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
	}

	@Test
	public void testTumblingWindowDuplicateElements() throws Exception {
		try {
			final int windowSize = 50;

			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							windowSize, windowSize);

			KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, fieldOneSelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.setProcessingTime(0);
			testHarness.open();

			final int numWindows = 10;

			long previousNextTime = 0;
			int window = 1;

			long currentTime = 0;

			while (window <= numWindows) {
				long nextTime = op.getNextEvaluationTime();
				int val = ((int) nextTime) ^ ((int) (nextTime >>> 32));

				StreamRecord<Tuple2<Integer, Integer>> next =  new StreamRecord<>(new Tuple2<>(val, val));
				testHarness.processElement(next);

				if (nextTime != previousNextTime) {
					window++;
					previousNextTime = nextTime;
				}
				currentTime = currentTime + 1;
				testHarness.setProcessingTime(currentTime);
			}

			testHarness.setProcessingTime(currentTime + 100);

			List<Tuple2<Integer, Integer>> result = extractFromStreamRecords(testHarness.extractOutputStreamRecords());

			testHarness.close();

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
	}

	@Test
	public void testSlidingWindow() throws Exception {
		try {
			// tumbling window that triggers every 20 milliseconds
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							150, 50);

			KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, fieldOneSelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.open();

			final int numElements = 1000;

			long currentTime = 0;

			for (int i = 0; i < numElements; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
				testHarness.processElement(next);
				currentTime = currentTime + 1;
				testHarness.setProcessingTime(currentTime);
			}

			// get and verify the result
			List<Tuple2<Integer, Integer>> result = extractFromStreamRecords(testHarness.extractOutputStreamRecords());

			testHarness.close();

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
	}

	@Test
	public void testSlidingWindowSingleElements() throws Exception {
		try {
			// tumbling window that triggers every 20 milliseconds
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer, 150, 50);

			KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, fieldOneSelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.open();

			testHarness.setProcessingTime(0);

			StreamRecord<Tuple2<Integer, Integer>> next1 = new StreamRecord<>(new Tuple2<>(1, 1));
			testHarness.processElement(next1);

			StreamRecord<Tuple2<Integer, Integer>> next2 = new StreamRecord<>(new Tuple2<>(2, 2));
			testHarness.processElement(next2);

			testHarness.setProcessingTime(50);
			testHarness.setProcessingTime(100);
			testHarness.setProcessingTime(150);

			List<Tuple2<Integer, Integer>> result = extractFromStreamRecords(testHarness.extractOutputStreamRecords());
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

			testHarness.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPropagateExceptionsFromProcessElement() throws Exception {

		try {
			ReduceFunction<Tuple2<Integer, Integer>> failingFunction = new FailingFunction(100);

			// the operator has a window time that is so long that it will not fire in this test
			final long hundredYears = 100L * 365 * 24 * 60 * 60 * 1000;
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							failingFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							hundredYears, hundredYears);

			KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness =
					new KeyedOneInputStreamOperatorTestHarness<>(op, fieldOneSelector, BasicTypeInfo.INT_TYPE_INFO);

			testHarness.open();

			for (int i = 0; i < 100; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(1, 1));
				testHarness.processElement(next);
			}

			try {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(1, 1));
				testHarness.processElement(next);
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
	}

	@Test
	public void checkpointRestoreWithPendingWindowTumbling() {
		try {
			final int windowSize = 200;

			// tumbling window that triggers every 50 milliseconds
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							windowSize, windowSize);

			OneInputStreamOperatorTestHarness<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness =
					new OneInputStreamOperatorTestHarness<>(op);

			testHarness.setProcessingTime(0);

			testHarness.setup();
			testHarness.open();

			// inject some elements
			final int numElementsFirst = 700;
			final int numElements = 1000;

			for (int i = 0; i < numElementsFirst; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
				testHarness.processElement(next);
			}

			// draw a snapshot
			List<Tuple2<Integer, Integer>> resultAtSnapshot = extractFromStreamRecords(testHarness.getOutput());
			int beforeSnapShot = resultAtSnapshot.size();
			StreamStateHandle state = testHarness.snapshotLegacy(1L, System.currentTimeMillis());
			int afterSnapShot = testHarness.getOutput().size();
			assertEquals("operator performed computation during snapshot", beforeSnapShot, afterSnapShot);

			assertTrue(resultAtSnapshot.size() <= numElementsFirst);

			// inject some random elements, which should not show up in the state
			for (int i = numElementsFirst; i < numElements; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
				testHarness.processElement(next);
			}

			testHarness.close();
			op.dispose();

			// re-create the operator and restore the state
			op = new AggregatingProcessingTimeWindowOperator<>(
					sumFunction, fieldOneSelector,
					IntSerializer.INSTANCE, tupleSerializer,
					windowSize, windowSize);

			testHarness = new OneInputStreamOperatorTestHarness<>(op);

			testHarness.setup();
			testHarness.restore(state);
			testHarness.open();

			// inject the remaining elements
			for (int i = numElementsFirst; i < numElements; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
				testHarness.processElement(next);
			}

			testHarness.setProcessingTime(200);

			// get and verify the result
			List<Tuple2<Integer, Integer>> finalResult = new ArrayList<>(resultAtSnapshot);
			List<Tuple2<Integer, Integer>> partialFinalResult = extractFromStreamRecords(testHarness.getOutput());
			finalResult.addAll(partialFinalResult);
			assertEquals(numElements, finalResult.size());

			Collections.sort(finalResult, tupleComparator);
			for (int i = 0; i < numElements; i++) {
				assertEquals(i, finalResult.get(i).f0.intValue());
				assertEquals(i, finalResult.get(i).f1.intValue());
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
	public void checkpointRestoreWithPendingWindowSliding() {
		try {
			final int factor = 4;
			final int windowSlide = 50;
			final int windowSize = factor * windowSlide;

			// sliding window (200 msecs) every 50 msecs
			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer,
							windowSize, windowSlide);


			OneInputStreamOperatorTestHarness<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness =
					new OneInputStreamOperatorTestHarness<>(op);

			testHarness.setProcessingTime(0);

			testHarness.setup();
			testHarness.open();

			// inject some elements
			final int numElements = 1000;
			final int numElementsFirst = 700;

			for (int i = 0; i < numElementsFirst; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
				testHarness.processElement(next);
			}

			// draw a snapshot
			List<Tuple2<Integer, Integer>> resultAtSnapshot = extractFromStreamRecords(testHarness.getOutput());
			int beforeSnapShot = resultAtSnapshot.size();
			StreamStateHandle state = testHarness.snapshotLegacy(1L, System.currentTimeMillis());
			int afterSnapShot = testHarness.getOutput().size();
			assertEquals("operator performed computation during snapshot", beforeSnapShot, afterSnapShot);

			assertTrue(resultAtSnapshot.size() <= factor * numElementsFirst);

			// inject the remaining elements - these should not influence the snapshot
			for (int i = numElementsFirst; i < numElements; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
				testHarness.processElement(next);
			}

			testHarness.close();
			op.dispose();

			// re-create the operator and restore the state
			op = new AggregatingProcessingTimeWindowOperator<>(
					sumFunction, fieldOneSelector,
					IntSerializer.INSTANCE, tupleSerializer,
					windowSize, windowSlide);

			testHarness = new OneInputStreamOperatorTestHarness<>(op);

			testHarness.setup();
			testHarness.restore(state);
			testHarness.open();

			// inject again the remaining elements
			for (int i = numElementsFirst; i < numElements; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next = new StreamRecord<>(new Tuple2<>(i, i));
				testHarness.processElement(next);
			}

			testHarness.setProcessingTime(50);
			testHarness.setProcessingTime(100);
			testHarness.setProcessingTime(150);
			testHarness.setProcessingTime(200);
			testHarness.setProcessingTime(250);
			testHarness.setProcessingTime(300);
			testHarness.setProcessingTime(350);
			testHarness.setProcessingTime(400);

			// get and verify the result
			List<Tuple2<Integer, Integer>> finalResult = new ArrayList<>(resultAtSnapshot);
			List<Tuple2<Integer, Integer>> partialFinalResult = extractFromStreamRecords(testHarness.getOutput());
			finalResult.addAll(partialFinalResult);
			assertEquals(numElements * factor, finalResult.size());

			Collections.sort(finalResult, tupleComparator);
			for (int i = 0; i < factor * numElements; i++) {
				assertEquals(i / factor, finalResult.get(i).f0.intValue());
				assertEquals(i / factor, finalResult.get(i).f1.intValue());
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
	public void testKeyValueStateInWindowFunctionTumbling() {
		try {
			final long twoSeconds = 2000;

			StatefulFunction.globalCounts.clear();

			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							new StatefulFunction(), fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer, twoSeconds, twoSeconds);

			KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
					op,
					fieldOneSelector,
					BasicTypeInfo.INT_TYPE_INFO);

			testHarness.setProcessingTime(0);
			testHarness.open();

			// because the window interval is so large, everything should be in one window
			// and aggregate into one value per key

			for (int i = 0; i < 10; i++) {
				StreamRecord<Tuple2<Integer, Integer>> next1 = new StreamRecord<>(new Tuple2<>(1, i));
				testHarness.processElement(next1);

				StreamRecord<Tuple2<Integer, Integer>> next2 = new StreamRecord<>(new Tuple2<>(2, i));
				testHarness.processElement(next2);
			}

			testHarness.setProcessingTime(1000);

			int count1 = StatefulFunction.globalCounts.get(1);
			int count2 = StatefulFunction.globalCounts.get(2);

			assertTrue(count1 >= 2 && count1 <= 2 * 10);
			assertEquals(count1, count2);

			testHarness.close();
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testKeyValueStateInWindowFunctionSliding() {
		try {
			final int factor = 2;
			final int windowSlide = 50;
			final int windowSize = factor * windowSlide;

			StatefulFunction.globalCounts.clear();

			AggregatingProcessingTimeWindowOperator<Integer, Tuple2<Integer, Integer>> op =
					new AggregatingProcessingTimeWindowOperator<>(
							new StatefulFunction(), fieldOneSelector,
							IntSerializer.INSTANCE, tupleSerializer, windowSize, windowSlide);

			KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
					op,
					fieldOneSelector,
					BasicTypeInfo.INT_TYPE_INFO);

			testHarness.setProcessingTime(0);

			testHarness.open();

			// because the window interval is so large, everything should be in one window
			// and aggregate into one value per key
			final int numElements = 100;

			// because we do not release the lock here, these elements
			for (int i = 0; i < numElements; i++) {

				StreamRecord<Tuple2<Integer, Integer>> next1 = new StreamRecord<>(new Tuple2<>(1, i));
				StreamRecord<Tuple2<Integer, Integer>> next2 = new StreamRecord<>(new Tuple2<>(2, i));
				StreamRecord<Tuple2<Integer, Integer>> next3 = new StreamRecord<>(new Tuple2<>(1, i));
				StreamRecord<Tuple2<Integer, Integer>> next4 = new StreamRecord<>(new Tuple2<>(2, i));

				testHarness.processElement(next1);
				testHarness.processElement(next2);
				testHarness.processElement(next3);
				testHarness.processElement(next4);
			}

			testHarness.setProcessingTime(50);
			testHarness.setProcessingTime(100);
			testHarness.setProcessingTime(150);
			testHarness.setProcessingTime(200);

			int count1 = StatefulFunction.globalCounts.get(1);
			int count2 = StatefulFunction.globalCounts.get(2);

			assertTrue(count1 >= 2 && count1 <= 2 * numElements);
			assertEquals(count1, count2);

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
			new AggregatingProcessingTimeWindowOperator<>(
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

		private ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			assertNotNull(getRuntimeContext());

			// start with one, so the final count is correct and we test that we do not
			// initialize with 0 always by default
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("totalCount", Integer.class, 1));
		}

		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
			state.update(state.value() + 1);
			globalCounts.put(value1.f0, state.value());

			return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
		}
	}

	// ------------------------------------------------------------------------

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
}
