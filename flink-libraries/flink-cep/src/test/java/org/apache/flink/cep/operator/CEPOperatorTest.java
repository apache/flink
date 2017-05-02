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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class CEPOperatorTest extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testKeyedCEPOperatorWatermarkForwarding() throws Exception {

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = getCepTestHarness(false);

		harness.open();

		Watermark expectedWatermark = new Watermark(42L);

		harness.processWatermark(expectedWatermark);

		verifyWatermark(harness.getOutput().poll(), 42L);

		harness.close();
	}

	@Test
	public void testKeyedCEPOperatorCheckpointing() throws Exception {

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = getCepTestHarness(false);

		harness.open();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(42, "end", 1.0);

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

		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

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

		harness.close();
	}

	@Test
	public void testKeyedCEPOperatorCheckpointingWithRocksDB() throws Exception {

		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = getCepTestHarness(false);

		harness.setStateBackend(rocksDBStateBackend);

		harness.open();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(42, "end", 1.0);

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

		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

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

		harness.close();
	}

	/**
	 * Tests that the internal time of a CEP operator advances only given watermarks. See FLINK-5033
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testKeyedAdvancingTimeWithoutElements() throws Exception {
		final KeySelector<Event, Integer> keySelector = new TestKeySelector();

		final Event startEvent = new Event(42, "start", 1.0);
		final long watermarkTimestamp1 = 5L;
		final long watermarkTimestamp2 = 13L;

		final Map<String, Event> expectedSequence = new HashMap<>(2);
		expectedSequence.put("start", startEvent);

		OneInputStreamOperatorTestHarness<Event, Either<Tuple2<Map<String, Event>, Long>, Map<String, Event>>> harness = new KeyedOneInputStreamOperatorTestHarness<>(
			new TimeoutKeyedCEPPatternOperator<>(
				Event.createTypeSerializer(),
				false,
				keySelector,
				IntSerializer.INSTANCE,
				new NFAFactory(true),
				null,
				true),
			keySelector,
			BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup(
				new KryoSerializer<>(
					(Class<Either<Tuple2<Map<String, Event>, Long>, Map<String, Event>>>) (Object) Either.class,
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

			StreamRecord<Either<Tuple2<Map<String, Event>, Long>, Map<String, Event>>> streamRecord = (StreamRecord<Either<Tuple2<Map<String,Event>,Long>,Map<String,Event>>>) resultObject;

			assertTrue(streamRecord.getValue() instanceof Either.Left);

			Either.Left<Tuple2<Map<String, Event>, Long>, Map<String, Event>> left = (Either.Left<Tuple2<Map<String, Event>, Long>, Map<String, Event>>) streamRecord.getValue();

			Tuple2<Map<String, Event>, Long> leftResult = left.left();

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

		TestKeySelector keySelector = new TestKeySelector();
		KeyedCEPPatternOperator<Event, Integer> operator = getKeyedCepOpearator(false, keySelector);
		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = getCepTestHarness(operator);

		harness.open();

		harness.processWatermark(new Watermark(Long.MIN_VALUE));

		harness.processElement(new StreamRecord<>(startEvent1, 1L));
		harness.processElement(new StreamRecord<>(startEventK2, 1L));
		harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));
		harness.processElement(new StreamRecord<Event>(middleEvent1, 2L));
		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

		// there must be 2 keys 42, 43 registered for the watermark callback
		// all the seen elements must be in the priority queues but no NFA yet.

		assertEquals(2L, harness.numKeysForWatermarkCallback());
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

		assertEquals(2L, harness.numKeysForWatermarkCallback());
		assertTrue(operator.hasNonEmptyNFA(42));
		assertEquals(1L, operator.getPQSize(42));
		assertTrue(operator.hasNonEmptyNFA(43));
		assertTrue(!operator.hasNonEmptyPQ(43));

		harness.processElement(new StreamRecord<>(startEvent2, 4L));
		harness.processElement(new StreamRecord<Event>(middleEvent2, 5L));
		harness.processElement(new StreamRecord<>(endEvent1, 6L));
		harness.processWatermark(11L);
		harness.processWatermark(12L);

		// now we have 1 key because the 43 expired and was removed.
		// 42 is still there due to startEvent2
		assertEquals(1L, harness.numKeysForWatermarkCallback());
		assertTrue(operator.hasNonEmptyNFA(42));
		assertTrue(!operator.hasNonEmptyPQ(42));
		assertTrue(!operator.hasNonEmptyNFA(43));
		assertTrue(!operator.hasNonEmptyPQ(43));

		verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
		verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent2, endEvent1);
		verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent1);
		verifyWatermark(harness.getOutput().poll(), 11L);
		verifyWatermark(harness.getOutput().poll(), 12L);

		harness.processElement(new StreamRecord<Event>(middleEvent3, 12L));
		harness.processElement(new StreamRecord<>(endEvent2, 13L));
		harness.processWatermark(20L);
		harness.processWatermark(21L);

		assertTrue(!operator.hasNonEmptyNFA(42));
		assertTrue(!operator.hasNonEmptyPQ(42));
		assertEquals(0L, harness.numKeysForWatermarkCallback());

		verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent2);
		verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent3, endEvent2);
		verifyWatermark(harness.getOutput().poll(), 20L);
		verifyWatermark(harness.getOutput().poll(), 21L);

		harness.close();
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

		TestKeySelector keySelector = new TestKeySelector();
		KeyedCEPPatternOperator<Event, Integer> operator = getKeyedCepOpearator(true, keySelector);
		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = getCepTestHarness(operator);

		harness.open();

		harness.setProcessingTime(0L);

		harness.processElement(new StreamRecord<>(startEvent1, 1L));
		harness.processElement(new StreamRecord<>(startEventK2, 1L));
		harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));
		harness.processElement(new StreamRecord<Event>(middleEvent1, 2L));
		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

		assertTrue(!operator.hasNonEmptyPQ(42));
		assertTrue(!operator.hasNonEmptyPQ(43));
		assertTrue(operator.hasNonEmptyNFA(42));
		assertTrue(operator.hasNonEmptyNFA(43));

		harness.setProcessingTime(3L);

		harness.processElement(new StreamRecord<>(startEvent2, 3L));
		harness.processElement(new StreamRecord<Event>(middleEvent2, 4L));
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

		assertTrue(operator.hasNonEmptyNFA(42));

		harness.processElement(new StreamRecord<>(startEvent1, 21L));
		assertTrue(operator.hasNonEmptyNFA(42));

		harness.setProcessingTime(49L);

		// TODO: 3/13/17 we have to have another event in order to clean up
		harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));

		// the pattern expired
		assertTrue(!operator.hasNonEmptyNFA(42));

		assertEquals(0L, harness.numKeysForWatermarkCallback());
		assertTrue(!operator.hasNonEmptyPQ(42));
		assertTrue(!operator.hasNonEmptyPQ(43));

		harness.close();
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
		Map<String, Event> patternMap = (Map<String, Event>) resultRecord.getValue();
		assertEquals(start, patternMap.get("start"));
		assertEquals(middle, patternMap.get("middle"));
		assertEquals(end, patternMap.get("end"));
	}

	private OneInputStreamOperatorTestHarness<Event, Map<String, Event>> getCepTestHarness(boolean isProcessingTime) throws Exception {
		KeySelector<Event, Integer> keySelector = new TestKeySelector();

		return new KeyedOneInputStreamOperatorTestHarness<>(
			getKeyedCepOpearator(isProcessingTime, keySelector),
			keySelector,
			BasicTypeInfo.INT_TYPE_INFO);
	}

	private OneInputStreamOperatorTestHarness<Event, Map<String, Event>> getCepTestHarness(
			KeyedCEPPatternOperator<Event, Integer> cepOperator) throws Exception {
		KeySelector<Event, Integer> keySelector = new TestKeySelector();

		return new KeyedOneInputStreamOperatorTestHarness<>(
			cepOperator,
			keySelector,
			BasicTypeInfo.INT_TYPE_INFO);
	}

	private KeyedCEPPatternOperator<Event, Integer> getKeyedCepOpearator(
			boolean isProcessingTime,
			KeySelector<Event, Integer> keySelector) {

		return new KeyedCEPPatternOperator<>(
			Event.createTypeSerializer(),
			isProcessingTime,
			keySelector,
			IntSerializer.INSTANCE,
			new NFAFactory(),
			null,
			true);
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
}
