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

package org.apache.flink.streaming.runtime.operators.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.operators.TriggerTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("serial")
public class AggregatingAlignedProcessingTimeWindowOperatorTest {

	@SuppressWarnings("unchecked")
	private final ReduceFunction<String> mockFunction = mock(ReduceFunction.class);

	@SuppressWarnings("unchecked")
	private final KeySelector<String, String> mockKeySelector = mock(KeySelector.class);
	
	private final KeySelector<Integer, Integer> identitySelector = new KeySelector<Integer, Integer>() {
		@Override
		public Integer getKey(Integer value) {
			return value;
		}
	};
	
	private final ReduceFunction<Integer> sumFunction = new ReduceFunction<Integer>() {
		@Override
		public Integer reduce(Integer value1, Integer value2) {
			return value1 + value2;
		}
	};

	// ------------------------------------------------------------------------

	@After
	public void checkNoTriggerThreadsRunning() {
		// make sure that all the threads we trigger are shut down
		long deadline = System.currentTimeMillis() + 5000;
		while (TriggerTimer.TRIGGER_THREADS_GROUP.activeCount() > 0 && System.currentTimeMillis() < deadline) {
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException ignored) {}
		}

		assertTrue("Not all trigger threads where properly shut down",
				TriggerTimer.TRIGGER_THREADS_GROUP.activeCount() == 0);
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
			AbstractAlignedProcessingTimeWindowOperator<String, String, String> op;
			
			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 5000, 1000);
			assertEquals(5000, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(1000, op.getPaneSize());
			assertEquals(5, op.getNumPanesPerWindow());

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1000, 1000);
			assertEquals(1000, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(1000, op.getPaneSize());
			assertEquals(1, op.getNumPanesPerWindow());

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1500, 1000);
			assertEquals(1500, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(500, op.getPaneSize());
			assertEquals(3, op.getNumPanesPerWindow());

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1200, 1100);
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
			
			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");
			
			AbstractAlignedProcessingTimeWindowOperator<String, String, String> op;

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 5000, 1000);
			op.setup(mockOut, mockContext);
			op.open(new Configuration());
			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1000, 1000);
			op.setup(mockOut, mockContext);
			op.open(new Configuration());
			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1500, 1000);
			op.setup(mockOut, mockContext);
			op.open(new Configuration());
			assertTrue(op.getNextSlideTime() % 500 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AggregatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1200, 1100);
			op.setup(mockOut, mockContext);
			op.open(new Configuration());
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
		try {
			final int windowSize = 50;
			final CollectingOutput<Integer> out = new CollectingOutput<>(windowSize);

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");
			
			AggregatingProcessingTimeWindowOperator<Integer, Integer> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, identitySelector, windowSize, windowSize);

			op.setup(out, mockContext);
			op.open(new Configuration());

			final int numElements = 1000;

			for (int i = 0; i < numElements; i++) {
				op.processElement(new StreamRecord<Integer>(i));
				Thread.sleep(1);
			}

			op.close();
			op.dispose();

			// get and verify the result
			List<Integer> result = out.getElements();
			assertEquals(numElements, result.size());

			Collections.sort(result);
			for (int i = 0; i < numElements; i++) {
				assertEquals(i, result.get(i).intValue());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTumblingWindowDuplicateElements() {
		try {
			final int windowSize = 50;
			final CollectingOutput<Integer> out = new CollectingOutput<>(windowSize);

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");
			
			AggregatingProcessingTimeWindowOperator<Integer, Integer> op =
					new AggregatingProcessingTimeWindowOperator<>(
							sumFunction, identitySelector, windowSize, windowSize);

			op.setup(out, mockContext);
			op.open(new Configuration());

			final int numWindows = 10;

			long previousNextTime = 0;
			int window = 1;
			
			while (window <= numWindows) {
				long nextTime = op.getNextEvaluationTime();
				int val = ((int) nextTime) ^ ((int) (nextTime >>> 32));
				
				op.processElement(new StreamRecord<Integer>(val));
				
				if (nextTime != previousNextTime) {
					window++;
					previousNextTime = nextTime;
				}
				
				Thread.sleep(1);
			}

			op.close();
			op.dispose();
			
			List<Integer> result = out.getElements();
			
			// we have ideally one element per window. we may have more, when we emitted a value into the
			// successive window (corner case), so we can have twice the number of elements, in the worst case.
			assertTrue(result.size() >= numWindows && result.size() <= 2 * numWindows);

			// deduplicate for more accurate checks
			HashSet<Integer> set = new HashSet<>(result);
			assertTrue(set.size() == 10 || set.size() == 11);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingWindow() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>(50);

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			// tumbling window that triggers every 20 milliseconds
			AggregatingProcessingTimeWindowOperator<Integer, Integer> op =
					new AggregatingProcessingTimeWindowOperator<>(sumFunction, identitySelector, 150, 50);

			op.setup(out, mockContext);
			op.open(new Configuration());

			final int numElements = 1000;

			for (int i = 0; i < numElements; i++) {
				op.processElement(new StreamRecord<Integer>(i));
				Thread.sleep(1);
			}

			op.close();
			op.dispose();

			// get and verify the result
			List<Integer> result = out.getElements();
			
			// every element can occur between one and three times
			if (result.size() < numElements || result.size() > 3 * numElements) {
				System.out.println(result);
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
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingWindowSingleElements() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>(50);

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			// tumbling window that triggers every 20 milliseconds
			AggregatingProcessingTimeWindowOperator<Integer, Integer> op =
					new AggregatingProcessingTimeWindowOperator<>(sumFunction, identitySelector, 150, 50);

			op.setup(out, mockContext);
			op.open(new Configuration());
			
			op.processElement(new StreamRecord<Integer>(1));
			op.processElement(new StreamRecord<Integer>(2));

			// each element should end up in the output three times
			// wait until the elements have arrived 6 times in the output
			out.waitForNElements(6, 120000);
			
			List<Integer> result = out.getElements();
			assertEquals(6, result.size());
			
			Collections.sort(result);
			assertEquals(Arrays.asList(1, 1, 1, 2, 2, 2), result);
			
			op.close();
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testEmitTrailingDataOnClose() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>();

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");
			
			// the operator has a window time that is so long that it will not fire in this test
			final long oneYear = 365L * 24 * 60 * 60 * 1000;
			AggregatingProcessingTimeWindowOperator<Integer, Integer> op = 
					new AggregatingProcessingTimeWindowOperator<>(sumFunction, identitySelector, oneYear, oneYear);
			
			op.setup(out, mockContext);
			op.open(new Configuration());
			
			List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
			for (Integer i : data) {
				op.processElement(new StreamRecord<Integer>(i));
			}
			
			op.close();
			op.dispose();
			
			// get and verify the result
			List<Integer> result = out.getElements();
			Collections.sort(result);
			assertEquals(data, result);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPropagateExceptionsFromTrigger() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>();

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			ReduceFunction<Integer> failingFunction = new FailingFunction(100);

			AggregatingProcessingTimeWindowOperator<Integer, Integer> op =
					new AggregatingProcessingTimeWindowOperator<>(failingFunction, identitySelector, 200, 50);

			op.setup(out, mockContext);
			op.open(new Configuration());

			try {
				long nextWindowTime = op.getNextEvaluationTime();
				int val = 0;
				for (int num = 0; num < Integer.MAX_VALUE; num++) {
					op.processElement(new StreamRecord<Integer>(val++));
					Thread.sleep(1);
					
					// when the window has advanced, reset the value, to generate the same values
					// in the next pane again. This causes the aggregation on trigger to reduce values
					if (op.getNextEvaluationTime() != nextWindowTime) {
						nextWindowTime = op.getNextEvaluationTime();
						val = 0;
					}
				}
				fail("This should really have failed with an exception quite a while ago...");
			}
			catch (Exception e) {
				assertNotNull(e.getCause());
				assertTrue(e.getCause().getMessage().contains("Artificial Test Exception"));
			}
			
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPropagateExceptionsFromProcessElement() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>();

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			ReduceFunction<Integer> failingFunction = new FailingFunction(100);

			// the operator has a window time that is so long that it will not fire in this test
			final long hundredYears = 100L * 365 * 24 * 60 * 60 * 1000;
			AggregatingProcessingTimeWindowOperator<Integer, Integer> op =
					new AggregatingProcessingTimeWindowOperator<>(
							failingFunction, identitySelector, hundredYears, hundredYears);

			op.setup(out, mockContext);
			op.open(new Configuration());

			for (int i = 0; i < 100; i++) {
				op.processElement(new StreamRecord<Integer>(1));
			}
			
			try {
				op.processElement(new StreamRecord<Integer>(1));
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
	
	// ------------------------------------------------------------------------
	
	private void assertInvalidParameter(long windowSize, long windowSlide) {
		try {
			new AggregatingProcessingTimeWindowOperator<String, String>(
					mockFunction, mockKeySelector, windowSize, windowSlide);
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
	
	private static class FailingFunction implements ReduceFunction<Integer> {

		private final int failAfterElements;
		
		private int numElements;

		FailingFunction(int failAfterElements) {
			this.failAfterElements = failAfterElements;
		}

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			numElements++;

			if (numElements >= failAfterElements) {
				throw new Exception("Artificial Test Exception");
			}
			
			return value1 + value2;
		}
	}
}
