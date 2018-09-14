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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.flink.cep.operator.CepOperatorTestUtilities.getKeyedCepOpearator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for rescaling of CEP operators.
 */
public class CEPRescalingTest {

	@Test
	public void testCEPFunctionScalingUp() throws Exception {
		int maxParallelism = 10;

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		// valid pattern events belong to different keygroups
		// that will be shipped to different tasks when changing parallelism.

		Event startEvent1 = new Event(7, "start", 1.0);
		SubEvent middleEvent1 = new SubEvent(7, "foo", 1.0, 10.0);
		Event endEvent1 = new Event(7, "end", 1.0);

		int keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(startEvent1), maxParallelism);
		assertEquals(1, keygroup);
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 2, keygroup));

		Event startEvent2 = new Event(10, "start", 1.0);				// this will go to task index 2
		SubEvent middleEvent2 = new SubEvent(10, "foo", 1.0, 10.0);
		Event endEvent2 = new Event(10, "end", 1.0);

		keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(startEvent2), maxParallelism);
		assertEquals(9, keygroup);
		assertEquals(1, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 2, keygroup));

		// now we start the test, we go from parallelism 1 to 2.
		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = null;
		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness1 = null;
		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness2 = null;

		try {
			harness = getTestHarness(maxParallelism, 1, 0);
			harness.open();

			harness.processElement(
				new StreamRecord<>(startEvent1, 1));                        // valid element
			harness.processElement(new StreamRecord<>(new Event(7, "foobar", 1.0), 2));

			harness.processElement(
				new StreamRecord<>(startEvent2, 3));                        // valid element
			harness.processElement(
				new StreamRecord<Event>(middleEvent2, 4));                // valid element

			// take a snapshot with some elements in internal sorting queue
			OperatorSubtaskState snapshot = harness.snapshot(0, 0);
			harness.close();

			// initialize two sub-tasks with the previously snapshotted state to simulate scaling up

			// we know that the valid element will go to index 0,
			// so we initialize the two tasks and we put the rest of
			// the valid elements for the pattern on task 0.

			harness1 = getTestHarness(maxParallelism, 2, 0);

			harness1.setup();
			harness1.initializeState(snapshot);
			harness1.open();

			// if element timestamps are not correctly checkpointed/restored this will lead to
			// a pruning time underflow exception in NFA
			harness1.processWatermark(new Watermark(2));

			harness1.processElement(
				new StreamRecord<Event>(middleEvent1, 3));                // valid element
			harness1.processElement(
				new StreamRecord<>(endEvent1, 5));                        // valid element

			harness1.processWatermark(new Watermark(Long.MAX_VALUE));

			// watermarks and the result
			assertEquals(3, harness1.getOutput().size());
			verifyWatermark(harness1.getOutput().poll(), 2);
			verifyPattern(harness1.getOutput().poll(), startEvent1, middleEvent1, endEvent1);

			harness2 = getTestHarness(maxParallelism, 2, 1);

			harness2.setup();
			harness2.initializeState(snapshot);
			harness2.open();

			// now we move to the second parallel task
			harness2.processWatermark(new Watermark(2));

			harness2.processElement(new StreamRecord<>(endEvent2, 5));
			harness2.processElement(new StreamRecord<>(new Event(42, "start", 1.0), 4));

			harness2.processWatermark(new Watermark(Long.MAX_VALUE));

			assertEquals(3, harness2.getOutput().size());
			verifyWatermark(harness2.getOutput().poll(), 2);
			verifyPattern(harness2.getOutput().poll(), startEvent2, middleEvent2, endEvent2);
		} finally {
			closeSilently(harness);
			closeSilently(harness1);
			closeSilently(harness2);
		}
	}

	private static void closeSilently(OneInputStreamOperatorTestHarness<?, ?> harness) {
		if (harness != null) {
			try {
				harness.close();
			} catch (Throwable ignored) {
			}
		}
	}

	@Test
	public void testCEPFunctionScalingDown() throws Exception {
		int maxParallelism = 10;

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		// create some valid pattern events on predetermined key groups and task indices

		Event startEvent1 = new Event(7, "start", 1.0);					// this will go to task index 0
		SubEvent middleEvent1 = new SubEvent(7, "foo", 1.0, 10.0);
		Event endEvent1 = new Event(7, "end", 1.0);

		// verification of the key choice
		int keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(startEvent1), maxParallelism);
		assertEquals(1, keygroup);
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 3, keygroup));
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 2, keygroup));

		Event startEvent2 = new Event(45, "start", 1.0);				// this will go to task index 1
		SubEvent middleEvent2 = new SubEvent(45, "foo", 1.0, 10.0);
		Event endEvent2 = new Event(45, "end", 1.0);

		keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(startEvent2), maxParallelism);
		assertEquals(6, keygroup);
		assertEquals(1, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 3, keygroup));
		assertEquals(1, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 2, keygroup));

		Event startEvent3 = new Event(90, "start", 1.0);				// this will go to task index 0
		SubEvent middleEvent3 = new SubEvent(90, "foo", 1.0, 10.0);
		Event endEvent3 = new Event(90, "end", 1.0);

		keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(startEvent3), maxParallelism);
		assertEquals(2, keygroup);
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 3, keygroup));
		assertEquals(0, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 2, keygroup));

		Event startEvent4 = new Event(10, "start", 1.0);				// this will go to task index 2
		SubEvent middleEvent4 = new SubEvent(10, "foo", 1.0, 10.0);
		Event endEvent4 = new Event(10, "end", 1.0);

		keygroup = KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(startEvent4), maxParallelism);
		assertEquals(9, keygroup);
		assertEquals(2, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 3, keygroup));
		assertEquals(1, KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, 2, keygroup));

		// starting the test, we will go from parallelism of 3 to parallelism of 2

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness1 =
			getTestHarness(maxParallelism, 3, 0);
		harness1.open();

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness2 =
			getTestHarness(maxParallelism, 3, 1);
		harness2.open();

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness3 =
			getTestHarness(maxParallelism, 3, 2);
		harness3.open();

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness4 = null;
		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness5 = null;

		try {
			harness1.processWatermark(Long.MIN_VALUE);
			harness2.processWatermark(Long.MIN_VALUE);
			harness3.processWatermark(Long.MIN_VALUE);

			harness1.processElement(
				new StreamRecord<>(startEvent1, 1));                        // valid element
			harness1.processElement(new StreamRecord<>(new Event(7, "foobar", 1.0), 2));
			harness1.processElement(
				new StreamRecord<Event>(middleEvent1, 3));                    // valid element
			harness1.processElement(
				new StreamRecord<>(endEvent1, 5));                            // valid element

			// till here we have a valid sequence, so after creating the
			// new instance and sending it a watermark, we expect it to fire,
			// even with no new elements.

			harness1.processElement(new StreamRecord<>(startEvent3, 10));
			harness1.processElement(new StreamRecord<>(startEvent1, 10));

			harness2.processElement(new StreamRecord<>(startEvent2, 7));
			harness2.processElement(new StreamRecord<Event>(middleEvent2, 8));

			harness3.processElement(new StreamRecord<>(startEvent4, 15));
			harness3.processElement(new StreamRecord<Event>(middleEvent4, 16));
			harness3.processElement(new StreamRecord<>(endEvent4, 17));

			// so far we only have the initial watermark
			assertEquals(1, harness1.getOutput().size());
			verifyWatermark(harness1.getOutput().poll(), Long.MIN_VALUE);

			assertEquals(1, harness2.getOutput().size());
			verifyWatermark(harness2.getOutput().poll(), Long.MIN_VALUE);

			assertEquals(1, harness3.getOutput().size());
			verifyWatermark(harness3.getOutput().poll(), Long.MIN_VALUE);

			// we take a snapshot and make it look as a single operator
			// this will be the initial state of all downstream tasks.
			OperatorSubtaskState snapshot = AbstractStreamOperatorTestHarness.repackageState(
				harness2.snapshot(0, 0),
				harness1.snapshot(0, 0),
				harness3.snapshot(0, 0)
			);

			harness4 = getTestHarness(maxParallelism, 2, 0);
			harness4.setup();
			harness4.initializeState(snapshot);
			harness4.open();

			harness5 = getTestHarness(maxParallelism, 2, 1);
			harness5.setup();
			harness5.initializeState(snapshot);
			harness5.open();

			harness5.processElement(new StreamRecord<>(endEvent2, 11));
			harness5.processWatermark(new Watermark(12));

			verifyPattern(harness5.getOutput().poll(), startEvent2, middleEvent2, endEvent2);
			verifyWatermark(harness5.getOutput().poll(), 12);

			// if element timestamps are not correctly checkpointed/restored this will lead to
			// a pruning time underflow exception in NFA
			harness4.processWatermark(new Watermark(12));

			assertEquals(2, harness4.getOutput().size());
			verifyPattern(harness4.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
			verifyWatermark(harness4.getOutput().poll(), 12);

			harness4.processElement(
				new StreamRecord<Event>(middleEvent3, 15));            // valid element
			harness4.processElement(
				new StreamRecord<>(endEvent3, 16));                    // valid element

			harness4.processElement(
				new StreamRecord<Event>(middleEvent1, 15));            // valid element
			harness4.processElement(
				new StreamRecord<>(endEvent1, 16));                    // valid element

			harness4.processWatermark(new Watermark(Long.MAX_VALUE));
			harness5.processWatermark(new Watermark(Long.MAX_VALUE));

			// verify result
			assertEquals(3, harness4.getOutput().size());

			// check the order of the events in the output
			Queue<Object> output = harness4.getOutput();
			StreamRecord<?> resultRecord = (StreamRecord<?>) output.peek();
			assertTrue(resultRecord.getValue() instanceof Map);

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap =
				(Map<String, List<Event>>) resultRecord.getValue();
			if (patternMap.get("start").get(0).getId() == 7) {
				verifyPattern(harness4.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
				verifyPattern(harness4.getOutput().poll(), startEvent3, middleEvent3, endEvent3);
			} else {
				verifyPattern(harness4.getOutput().poll(), startEvent3, middleEvent3, endEvent3);
				verifyPattern(harness4.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
			}

			// after scaling down this should end up here
			assertEquals(2, harness5.getOutput().size());
			verifyPattern(harness5.getOutput().poll(), startEvent4, middleEvent4, endEvent4);
		} finally {
			closeSilently(harness1);
			closeSilently(harness2);
			closeSilently(harness3);
			closeSilently(harness4);
			closeSilently(harness5);
		}
	}

	private void verifyWatermark(Object outputObject, long timestamp) {
		assertTrue(outputObject instanceof Watermark);
		assertEquals(timestamp, ((Watermark) outputObject).getTimestamp());
	}

	private void verifyPattern(Object outputObject, Event start, SubEvent middle, Event end) {
		assertTrue(outputObject instanceof StreamRecord);

		StreamRecord<?> resultRecord = (StreamRecord<?>) outputObject;
		assertTrue(resultRecord.getValue() instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, List<Event>> patternMap = (Map<String, List<Event>>) resultRecord.getValue();
		assertEquals(start, patternMap.get("start").get(0));
		assertEquals(middle, patternMap.get("middle").get(0));
		assertEquals(end, patternMap.get("end").get(0));
	}

	private KeyedOneInputStreamOperatorTestHarness<Integer, Event, Map<String, List<Event>>> getTestHarness(
			int maxParallelism,
			int taskParallelism,
			int subtaskIdx) throws Exception {

		KeySelector<Event, Integer> keySelector = new TestKeySelector();
		KeyedOneInputStreamOperatorTestHarness<Integer, Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						getKeyedCepOpearator(
								false,
								new NFAFactory()),
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO,
						maxParallelism,
						taskParallelism,
						subtaskIdx);
		harness.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));
		return harness;
	}

	private static class NFAFactory implements NFACompiler.NFAFactory<Event> {

		private static final long serialVersionUID = 1173020762472766713L;

		private final boolean handleTimeout;

		private NFAFactory() {
			this(false);
		}

		private NFAFactory(boolean handleTimeout) {
			this.handleTimeout = handleTimeout;
		}

		@Override
		public NFA<Event> createNFA() {

			Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			})
				.followedBy("middle").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
					private static final long serialVersionUID = 6215754202506583964L;

					@Override
					public boolean filter(SubEvent value) throws Exception {
						return value.getVolume() > 5.0;
					}
				})
				.followedBy("end").where(new SimpleCondition<Event>() {
					private static final long serialVersionUID = 7056763917392056548L;

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("end");
					}
				})
				// add a window timeout to test whether timestamps of elements in the
				// priority queue in CEP operator are correctly checkpointed/restored
				.within(Time.milliseconds(10L));

			return NFACompiler.compileFactory(pattern, handleTimeout).createNFA();
		}
	}

	/**
	 * A simple {@link KeySelector} that returns as key the id of the {@link Event}
	 * provided as argument in the {@link #getKey(Event)}.
	 * */
	private static class TestKeySelector implements KeySelector<Event, Integer> {
		private static final long serialVersionUID = -4873366487571254798L;

		@Override
		public Integer getKey(Event value) throws Exception {
			return value.getId();
		}
	}
}
