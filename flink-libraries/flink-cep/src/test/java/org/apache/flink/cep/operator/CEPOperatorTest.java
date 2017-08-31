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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class CEPOperatorTest extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testKeyedCEPOperatorWatermarkForwarding() throws Exception {

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = getCepTestHarness(false);

		try {
			harness.open();

			Watermark expectedWatermark = new Watermark(42L);

			harness.processWatermark(expectedWatermark);

			verifyWatermark(harness.getOutput().poll(), 42L);
		} finally {
			harness.close();
		}
	}

	@Test
	public void testKeyedCEPOperatorCheckpointing() throws Exception {

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = getCepTestHarness(false);

		try {
			harness.open();

			Event startEvent = new Event(42, "start", 1.0);
			SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
			Event endEvent = new Event(42, "end", 1.0);

			harness.processElement(new StreamRecord<>(startEvent, 1L));
			harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));

			// simulate snapshot/restore with some elements in internal sorting queue
			OperatorStateHandles snapshot = harness.snapshot(0L, 0L);
			harness.close();

			harness = getCepTestHarness(false);

			harness.setup();
			harness.initializeState(snapshot);
			harness.open();

			harness.processWatermark(new Watermark(Long.MIN_VALUE));

			harness
				.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

			// if element timestamps are not correctly checkpointed/restored this will lead to
			// a pruning time underflow exception in NFA
			harness.processWatermark(new Watermark(2L));

			harness.processElement(new StreamRecord<Event>(middleEvent, 3L));
			harness.processElement(new StreamRecord<>(new Event(42, "start", 1.0), 4L));
			harness.processElement(new StreamRecord<>(endEvent, 5L));

			// simulate snapshot/restore with empty element queue but NFA state
			OperatorStateHandles snapshot2 = harness.snapshot(1L, 1L);
			harness.close();

			harness = getCepTestHarness(false);

			harness.setup();
			harness.initializeState(snapshot2);
			harness.open();

			harness.processWatermark(new Watermark(Long.MAX_VALUE));

			// get and verify the output

			Queue<Object> result = harness.getOutput();

			assertEquals(2, result.size());

			verifyPattern(result.poll(), startEvent, middleEvent, endEvent);
			verifyWatermark(result.poll(), Long.MAX_VALUE);
		} finally {
			harness.close();
		}
	}

	@Test
	public void testKeyedCEPOperatorCheckpointingWithRocksDB() throws Exception {

		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = getCepTestHarness(false);

		try {
			harness.setStateBackend(rocksDBStateBackend);

			harness.open();

			Event startEvent = new Event(42, "start", 1.0);
			SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
			Event endEvent = new Event(42, "end", 1.0);

			harness.processElement(new StreamRecord<>(startEvent, 1L));
			harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));

			// simulate snapshot/restore with some elements in internal sorting queue
			OperatorStateHandles snapshot = harness.snapshot(0L, 0L);
			harness.close();

			harness = getCepTestHarness(false);

			rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
			rocksDBStateBackend.setDbStoragePath(rocksDbPath);
			harness.setStateBackend(rocksDBStateBackend);

			harness.setup();
			harness.initializeState(snapshot);
			harness.open();

			harness.processWatermark(new Watermark(Long.MIN_VALUE));

			harness
				.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

			// if element timestamps are not correctly checkpointed/restored this will lead to
			// a pruning time underflow exception in NFA
			harness.processWatermark(new Watermark(2L));

			// simulate snapshot/restore with empty element queue but NFA state
			OperatorStateHandles snapshot2 = harness.snapshot(1L, 1L);
			harness.close();

			harness = getCepTestHarness(false);

			rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
			rocksDBStateBackend.setDbStoragePath(rocksDbPath);
			harness.setStateBackend(rocksDBStateBackend);
			harness.setup();
			harness.initializeState(snapshot2);
			harness.open();

			harness.processElement(new StreamRecord<Event>(middleEvent, 3L));
			harness.processElement(new StreamRecord<>(new Event(42, "start", 1.0), 4L));
			harness.processElement(new StreamRecord<>(endEvent, 5L));

			harness.processWatermark(new Watermark(Long.MAX_VALUE));

			// get and verify the output

			Queue<Object> result = harness.getOutput();

			assertEquals(2, result.size());

			verifyPattern(result.poll(), startEvent, middleEvent, endEvent);
			verifyWatermark(result.poll(), Long.MAX_VALUE);
		} finally {
			harness.close();
		}
	}

	/**
	 * Tests that the internal time of a CEP operator advances only given watermarks. See FLINK-5033
	 */
	@Test
	public void testKeyedAdvancingTimeWithoutElements() throws Exception {
		final KeySelector<Event, Integer> keySelector = new TestKeySelector();

		final Event startEvent = new Event(42, "start", 1.0);
		final long watermarkTimestamp1 = 5L;
		final long watermarkTimestamp2 = 13L;

		final Map<String, List<Event>> expectedSequence = new HashMap<>(2);
		expectedSequence.put("start", Collections.<Event>singletonList(startEvent));

		OneInputStreamOperatorTestHarness<Event, Either<Tuple2<Map<String, List<Event>>, Long>, Map<String, List<Event>>>> harness = new KeyedOneInputStreamOperatorTestHarness<>(
			new TimeoutKeyedCEPPatternOperator<>(
				Event.createTypeSerializer(),
				false,
				IntSerializer.INSTANCE,
				new NFAFactory(true),
				true),
			keySelector,
			BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup(
				new KryoSerializer<>(
					(Class<Either<Tuple2<Map<String, List<Event>>, Long>, Map<String, List<Event>>>>) (Object) Either.class,
					new ExecutionConfig()));
			harness.open();

			harness.processElement(new StreamRecord<>(startEvent, 3L));
			harness.processWatermark(new Watermark(watermarkTimestamp1));
			harness.processWatermark(new Watermark(watermarkTimestamp2));

			Queue<Object> result = harness.getOutput();

			assertEquals(3L, result.size());

			Object watermark1 = result.poll();

			assertTrue(watermark1 instanceof Watermark);

			assertEquals(watermarkTimestamp1, ((Watermark) watermark1).getTimestamp());

			Object resultObject = result.poll();

			assertTrue(resultObject instanceof StreamRecord);

			StreamRecord<Either<Tuple2<Map<String, List<Event>>, Long>, Map<String, List<Event>>>> streamRecord =
					(StreamRecord<Either<Tuple2<Map<String,List<Event>>,Long>,Map<String,List<Event>>>>) resultObject;

			assertTrue(streamRecord.getValue() instanceof Either.Left);

			Either.Left<Tuple2<Map<String, List<Event>>, Long>, Map<String, List<Event>>> left =
			(Either.Left<Tuple2<Map<String, List<Event>>, Long>, Map<String, List<Event>>>) streamRecord.getValue();

			Tuple2<Map<String, List<Event>>, Long> leftResult = left.left();

			assertEquals(watermarkTimestamp2, (long) leftResult.f1);
			assertEquals(expectedSequence, leftResult.f0);

			Object watermark2 = result.poll();

			assertTrue(watermark2 instanceof Watermark);

			assertEquals(watermarkTimestamp2, ((Watermark) watermark2).getTimestamp());
		} finally {
			harness.close();
		}
	}

	@Test
	public void testCEPOperatorCleanupEventTime() throws Exception {

		Event startEvent1 = new Event(42, "start", 1.0);
		Event startEvent2 = new Event(42, "start", 2.0);
		SubEvent middleEvent1 = new SubEvent(42, "foo1", 1.0, 10.0);
		SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
		SubEvent middleEvent3 = new SubEvent(42, "foo3", 1.0, 10.0);
		Event endEvent1 =  new Event(42, "end", 1.0);
		Event endEvent2 =  new Event(42, "end", 2.0);

		Event startEventK2 = new Event(43, "start", 1.0);

		KeyedCEPPatternOperator<Event, Integer> operator = getKeyedCepOpearator(false);
		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = getCepTestHarness(operator);

		try {
			harness.open();

			harness.processWatermark(new Watermark(Long.MIN_VALUE));

			harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));
			harness.processElement(new StreamRecord<Event>(middleEvent1, 2L));
			harness
				.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));
			harness.processElement(new StreamRecord<>(startEvent1, 1L));
			harness.processElement(new StreamRecord<>(startEventK2, 1L));

			// there must be 2 keys 42, 43 registered for the watermark callback
			// all the seen elements must be in the priority queues but no NFA yet.

			assertEquals(2L, harness.numEventTimeTimers());
			assertEquals(4L, operator.getPQSize(42));
			assertEquals(1L, operator.getPQSize(43));
			assertTrue(!operator.hasNonEmptyNFA(42));
			assertTrue(!operator.hasNonEmptyNFA(43));

			harness.processWatermark(new Watermark(2L));

			verifyWatermark(harness.getOutput().poll(), Long.MIN_VALUE);
			verifyWatermark(harness.getOutput().poll(), 2L);

			// still the 2 keys
			// one element in PQ for 42 (the barfoo) as it arrived early
			// for 43 the element entered the NFA and the PQ is empty

			assertEquals(2L, harness.numEventTimeTimers());
			assertTrue(operator.hasNonEmptyNFA(42));
			assertEquals(1L, operator.getPQSize(42));
			assertTrue(operator.hasNonEmptyNFA(43));
			assertTrue(!operator.hasNonEmptyPQ(43));

			harness.processElement(new StreamRecord<>(startEvent2, 4L));
			harness.processElement(new StreamRecord<Event>(middleEvent2, 5L));

			OperatorStateHandles snapshot = harness.snapshot(0L, 0L);
			harness.close();

			KeyedCEPPatternOperator<Event, Integer> operator2 = getKeyedCepOpearator(false);
			harness = getCepTestHarness(operator2);
			harness.setup();
			harness.initializeState(snapshot);
			harness.open();

			harness.processElement(new StreamRecord<>(endEvent1, 6L));
			harness.processWatermark(11L);
			harness.processWatermark(12L);

			// now we have 1 key because the 43 expired and was removed.
			// 42 is still there due to startEvent2
			assertEquals(1L, harness.numEventTimeTimers());
			assertTrue(operator2.hasNonEmptyNFA(42));
			assertTrue(!operator2.hasNonEmptyPQ(42));
			assertTrue(!operator2.hasNonEmptyNFA(43));
			assertTrue(!operator2.hasNonEmptyPQ(43));

			verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
			verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent2, endEvent1);
			verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent1);
			verifyWatermark(harness.getOutput().poll(), 11L);
			verifyWatermark(harness.getOutput().poll(), 12L);

			// this is a late event, because timestamp(12) = last watermark(12)
			harness.processElement(new StreamRecord<Event>(middleEvent3, 12L));
			harness.processElement(new StreamRecord<>(endEvent2, 13L));
			harness.processWatermark(20L);
			harness.processWatermark(21L);

			assertTrue(!operator2.hasNonEmptyNFA(42));
			assertTrue(!operator2.hasNonEmptyPQ(42));
			assertEquals(0L, harness.numEventTimeTimers());

			assertEquals(3, harness.getOutput().size());
			verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent2);

			verifyWatermark(harness.getOutput().poll(), 20L);
			verifyWatermark(harness.getOutput().poll(), 21L);
		} finally {
			harness.close();
		}
	}

	@Test
	public void testCEPOperatorCleanupEventTimeWithSameElements() throws Exception {

		Event startEvent = new Event(41, "c", 1.0);
		Event middle1Event1 = new Event(41, "a", 2.0);
		Event middle1Event2 = new Event(41, "a", 3.0);
		Event middle1Event3 = new Event(41, "a", 4.0);
		Event middle2Event1 = new Event(41, "b", 5.0);

		KeyedCEPPatternOperator<Event, Integer> operator = new KeyedCEPPatternOperator<>(
				Event.createTypeSerializer(),
				false,
				IntSerializer.INSTANCE,
				new ComplexNFAFactory(),
				true);
		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = getCepTestHarness(operator);

		try {
			harness.open();

			harness.processWatermark(new Watermark(Long.MIN_VALUE));

			harness.processElement(new StreamRecord<>(middle2Event1, 6));
			harness.processElement(new StreamRecord<>(middle1Event3, 7));
			harness.processElement(new StreamRecord<>(startEvent, 1));
			harness.processElement(new StreamRecord<>(middle1Event1, 3));
			harness.processElement(new StreamRecord<>(middle1Event2, 3));
			harness.processElement(new StreamRecord<>(middle1Event1, 3));
			harness.processElement(new StreamRecord<>(new Event(41, "d", 6.0), 5));

			assertEquals(1L, harness.numEventTimeTimers());
			assertEquals(7L, operator.getPQSize(41));
			assertTrue(!operator.hasNonEmptyNFA(41));

			harness.processWatermark(new Watermark(2L));

			verifyWatermark(harness.getOutput().poll(), Long.MIN_VALUE);
			verifyWatermark(harness.getOutput().poll(), 2L);

			assertEquals(1L, harness.numEventTimeTimers());
			assertEquals(6L, operator.getPQSize(41));
			assertTrue(operator.hasNonEmptyNFA(41)); // processed the first element

			harness.processWatermark(new Watermark(8L));

			List<List<Event>> resultingPatterns = new ArrayList<>();
			while (!harness.getOutput().isEmpty()) {
				Object o = harness.getOutput().poll();
				if (!(o instanceof Watermark)) {
					StreamRecord<Map<String, List<Event>>> el =
						(StreamRecord<Map<String, List<Event>>>) o;
					List<Event> res = new ArrayList<>();
					for (List<Event> le : el.getValue().values()) {
						res.addAll(le);
					}
					resultingPatterns.add(res);
				} else {
					verifyWatermark(o, 8L);
				}
			}

			compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(startEvent, middle1Event1),

				Lists.newArrayList(startEvent, middle1Event1, middle1Event2),
				Lists.newArrayList(startEvent, middle2Event1, middle1Event3),

				Lists.newArrayList(startEvent, middle1Event1, middle1Event2, middle1Event1),
				Lists.newArrayList(startEvent, middle1Event1, middle2Event1, middle1Event3),

				Lists.newArrayList(startEvent, middle1Event1, middle1Event1, middle1Event2,
					middle1Event3),
				Lists.newArrayList(startEvent, middle1Event1, middle1Event2, middle2Event1,
					middle1Event3),

				Lists.newArrayList(startEvent, middle1Event1, middle1Event1, middle1Event2,
					middle2Event1, middle1Event3)
			));

			assertEquals(1L, harness.numEventTimeTimers());
			assertEquals(0L, operator.getPQSize(41));
			assertTrue(operator.hasNonEmptyNFA(41));

			harness.processWatermark(new Watermark(17L));
			verifyWatermark(harness.getOutput().poll(), 17L);

			assertTrue(!operator.hasNonEmptyNFA(41));
			assertTrue(!operator.hasNonEmptyPQ(41));
			assertEquals(0L, harness.numEventTimeTimers());
		} finally {
			harness.close();
		}
	}

	@Test
	public void testCEPOperatorCleanupProcessingTime() throws Exception {

		Event startEvent1 = new Event(42, "start", 1.0);
		Event startEvent2 = new Event(42, "start", 2.0);
		SubEvent middleEvent1 = new SubEvent(42, "foo1", 1.0, 10.0);
		SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
		SubEvent middleEvent3 = new SubEvent(42, "foo3", 1.0, 10.0);
		Event endEvent1 =  new Event(42, "end", 1.0);
		Event endEvent2 =  new Event(42, "end", 2.0);

		Event startEventK2 = new Event(43, "start", 1.0);

		KeyedCEPPatternOperator<Event, Integer> operator = getKeyedCepOpearator(true);
		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = getCepTestHarness(operator);

		try {
			harness.open();

			harness.setProcessingTime(0L);

			harness.processElement(new StreamRecord<>(startEvent1, 1L));
			harness.processElement(new StreamRecord<>(startEventK2, 1L));
			harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));
			harness.processElement(new StreamRecord<Event>(middleEvent1, 2L));
			harness
				.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

			assertTrue(!operator.hasNonEmptyPQ(42));
			assertTrue(!operator.hasNonEmptyPQ(43));
			assertTrue(operator.hasNonEmptyNFA(42));
			assertTrue(operator.hasNonEmptyNFA(43));

			harness.setProcessingTime(3L);

			harness.processElement(new StreamRecord<>(startEvent2, 3L));
			harness.processElement(new StreamRecord<Event>(middleEvent2, 4L));

			OperatorStateHandles snapshot = harness.snapshot(0L, 0L);
			harness.close();

			KeyedCEPPatternOperator<Event, Integer> operator2 = getKeyedCepOpearator(true);
			harness = getCepTestHarness(operator2);
			harness.setup();
			harness.initializeState(snapshot);
			harness.open();

			harness.setProcessingTime(3L);
			harness.processElement(new StreamRecord<>(endEvent1, 5L));

			verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
			verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent2, endEvent1);
			verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent1);

			harness.setProcessingTime(11L);

			harness.processElement(new StreamRecord<Event>(middleEvent3, 11L));
			harness.processElement(new StreamRecord<>(endEvent2, 12L));

			verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent2);
			verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent3, endEvent2);

			harness.setProcessingTime(21L);

			assertTrue(operator2.hasNonEmptyNFA(42));

			harness.processElement(new StreamRecord<>(startEvent1, 21L));
			assertTrue(operator2.hasNonEmptyNFA(42));

			harness.setProcessingTime(49L);

			// TODO: 3/13/17 we have to have another event in order to clean up
			harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));

			// the pattern expired
			assertTrue(!operator2.hasNonEmptyNFA(42));

			assertEquals(0L, harness.numEventTimeTimers());
			assertTrue(!operator2.hasNonEmptyPQ(42));
			assertTrue(!operator2.hasNonEmptyPQ(43));
		} finally {
			harness.close();
		}
	}

	@Test
	public void testCEPOperatorSerializationWRocksDB() throws Exception {
		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		final Event startEvent1 = new Event(40, "start", 1.0);
		final Event startEvent2 = new Event(40, "start", 2.0);
		final SubEvent middleEvent1 = new SubEvent(40, "foo1", 1.0, 10);
		final SubEvent middleEvent2 = new SubEvent(40, "foo2", 2.0, 10);
		final SubEvent middleEvent3 = new SubEvent(40, "foo3", 3.0, 10);
		final SubEvent middleEvent4 = new SubEvent(40, "foo4", 1.0, 10);
		final Event nextOne = new Event(40, "next-one", 1.0);
		final Event endEvent = new Event(40, "end", 1.0);

		final Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").subtype(SubEvent.class).where(new IterativeCondition<SubEvent>() {

			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter (SubEvent value, Context < SubEvent > ctx) throws Exception {
				if (!value.getName().startsWith("foo")) {
					return false;
				}

				double sum = 0.0;
				for (Event event : ctx.getEventsForPattern("middle")) {
					sum += event.getPrice();
				}
				sum += value.getPrice();
				return Double.compare(sum, 5.0) < 0;
			}
		}).oneOrMore().allowCombinations().followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 7056763917392056548L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		KeyedCEPPatternOperator<Event, Integer> operator = new KeyedCEPPatternOperator<>(
				Event.createTypeSerializer(),
				false,
				IntSerializer.INSTANCE,
				new NFACompiler.NFAFactory<Event>() {

					private static final long serialVersionUID = 477082663248051994L;

					@Override
					public NFA<Event> createNFA() {
						return NFACompiler.compile(pattern, Event.createTypeSerializer(), false);
					}
				},
				true);

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = getCepTestHarness(operator);

		try {
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();

			harness.processWatermark(0L);
			harness.processElement(new StreamRecord<>(startEvent1, 1));
			harness.processElement(new StreamRecord<Event>(middleEvent1, 2));
			harness.processWatermark(2L);
			harness.processElement(new StreamRecord<Event>(middleEvent3, 5));
			harness.processElement(new StreamRecord<Event>(middleEvent2, 3));
			harness.processElement(new StreamRecord<>(startEvent2, 4));
			harness.processWatermark(5L);
			harness.processElement(new StreamRecord<>(nextOne, 7));
			harness.processElement(new StreamRecord<>(endEvent, 8));
			harness.processElement(new StreamRecord<Event>(middleEvent4, 6));
			harness.processWatermark(100L);

			List<List<Event>> resultingPatterns = new ArrayList<>();
			while (!harness.getOutput().isEmpty()) {
				Object o = harness.getOutput().poll();
				if (!(o instanceof Watermark)) {
					StreamRecord<Map<String, List<Event>>> el =
						(StreamRecord<Map<String, List<Event>>>) o;
					List<Event> res = new ArrayList<>();
					for (List<Event> le : el.getValue().values()) {
						res.addAll(le);
					}
					resultingPatterns.add(res);
				}
			}

			compareMaps(resultingPatterns,
				Lists.<List<Event>>newArrayList(
					Lists.newArrayList(startEvent1, endEvent, middleEvent1, middleEvent2,
						middleEvent4),
					Lists.newArrayList(startEvent1, endEvent, middleEvent2, middleEvent1),
					Lists.newArrayList(startEvent1, endEvent, middleEvent3, middleEvent1),
					Lists.newArrayList(startEvent2, endEvent, middleEvent3, middleEvent4),
					Lists.newArrayList(startEvent1, endEvent, middleEvent4, middleEvent1),
					Lists.newArrayList(startEvent1, endEvent, middleEvent1),
					Lists.newArrayList(startEvent2, endEvent, middleEvent3)
				)
			);
		} finally {
			harness.close();
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

	private OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> getCepTestHarness(boolean isProcessingTime) throws Exception {
		KeySelector<Event, Integer> keySelector = new TestKeySelector();

		return new KeyedOneInputStreamOperatorTestHarness<>(
			getKeyedCepOpearator(isProcessingTime),
			keySelector,
			BasicTypeInfo.INT_TYPE_INFO);
	}

	private OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> getCepTestHarness(
			KeyedCEPPatternOperator<Event, Integer> cepOperator) throws Exception {
		KeySelector<Event, Integer> keySelector = new TestKeySelector();

		return new KeyedOneInputStreamOperatorTestHarness<>(
			cepOperator,
			keySelector,
			BasicTypeInfo.INT_TYPE_INFO);
	}

	private KeyedCEPPatternOperator<Event, Integer> getKeyedCepOpearator(
			boolean isProcessingTime) {

		return new KeyedCEPPatternOperator<>(
			Event.createTypeSerializer(),
			isProcessingTime,
			IntSerializer.INSTANCE,
			new NFAFactory(),
			true);
	}

	private void compareMaps(List<List<Event>> actual, List<List<Event>> expected) {
		Assert.assertEquals(expected.size(), actual.size());

		for (List<Event> p: actual) {
			Collections.sort(p, new EventComparator());
		}

		for (List<Event> p: expected) {
			Collections.sort(p, new EventComparator());
		}

		Collections.sort(actual, new ListEventComparator());
		Collections.sort(expected, new ListEventComparator());
		Assert.assertArrayEquals(expected.toArray(), actual.toArray());
	}


	private class ListEventComparator implements Comparator<List<Event>> {

		@Override
		public int compare(List<Event> o1, List<Event> o2) {
			int sizeComp = Integer.compare(o1.size(), o2.size());
			if (sizeComp == 0) {
				EventComparator comp = new EventComparator();
				for (int i = 0; i < o1.size(); i++) {
					int eventComp = comp.compare(o1.get(i), o2.get(i));
					if (eventComp != 0) {
						return eventComp;
					}
				}
				return 0;
			} else {
				return sizeComp;
			}
		}
	}

	private class EventComparator implements Comparator<Event> {

		@Override
		public int compare(Event o1, Event o2) {
			int nameComp = o1.getName().compareTo(o2.getName());
			int priceComp = Double.compare(o1.getPrice(), o2.getPrice());
			int idComp = Integer.compare(o1.getId(), o2.getId());
			if (nameComp == 0) {
				if (priceComp == 0) {
					return idComp;
				} else {
					return priceComp;
				}
			} else {
				return nameComp;
			}
		}
	}

	private static class TestKeySelector implements KeySelector<Event, Integer> {

		private static final long serialVersionUID = -4873366487571254798L;

		@Override
		public Integer getKey(Event value) throws Exception {
			return value.getId();
		}
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
					.followedByAny("middle").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
						private static final long serialVersionUID = 6215754202506583964L;

						@Override
						public boolean filter(SubEvent value) throws Exception {
							return value.getVolume() > 5.0;
						}
					})
					.followedByAny("end").where(new SimpleCondition<Event>() {
						private static final long serialVersionUID = 7056763917392056548L;

						@Override
						public boolean filter(Event value) throws Exception {
							return value.getName().equals("end");
						}
					})
					// add a window timeout to test whether timestamps of elements in the
					// priority queue in CEP operator are correctly checkpointed/restored
					.within(Time.milliseconds(10L));

			return NFACompiler.compile(pattern, Event.createTypeSerializer(), handleTimeout);
		}
	}

	private static class ComplexNFAFactory implements NFACompiler.NFAFactory<Event> {

		private static final long serialVersionUID = 1173020762472766713L;

		private final boolean handleTimeout;

		private ComplexNFAFactory() {
			this(false);
		}

		private ComplexNFAFactory(boolean handleTimeout) {
			this.handleTimeout = handleTimeout;
		}

		@Override
		public NFA<Event> createNFA() {

			Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("c");
				}
			}).followedBy("middle1").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("a");
				}
			}).oneOrMore().optional().followedBy("middle2").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("b");
				}
			}).optional().followedBy("end").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("a");
				}
			}).within(Time.milliseconds(10L));

			return NFACompiler.compile(pattern, Event.createTypeSerializer(), handleTimeout);
		}
	}
}
