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
import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
import java.util.concurrent.ConcurrentLinkedQueue;

public class CEPOperatorTest extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testCEPOperatorWatermarkForwarding() throws Exception {
		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new OneInputStreamOperatorTestHarness<>(
			new CEPPatternOperator<>(
				Event.createTypeSerializer(),
				false,
				new NFAFactory())
		);

		harness.open();

		Watermark expectedWatermark = new Watermark(42L);

		harness.processWatermark(expectedWatermark);

		Object watermark = harness.getOutput().poll();

		assertTrue(watermark instanceof Watermark);
		assertEquals(expectedWatermark, watermark);

		harness.close();
	}

	@Test
	public void testKeyedCEPOperatorWatermarkForwarding() throws Exception {
		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
					Event.createTypeSerializer(),
					false,
					keySelector,
					IntSerializer.INSTANCE,
					new NFAFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		harness.open();

		Watermark expectedWatermark = new Watermark(42L);

		harness.processWatermark(expectedWatermark);

		Object watermark = harness.getOutput().poll();

		assertTrue(watermark instanceof Watermark);
		assertEquals(expectedWatermark, watermark);

		harness.close();
	}

	@Test
	public void testCEPOperatorCheckpointing() throws Exception {
		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new OneInputStreamOperatorTestHarness<>(
				new CEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						new NFAFactory()));

		harness.open();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(42, "end", 1.0);

		harness.processElement(new StreamRecord<Event>(startEvent, 1));
		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));

		// simulate snapshot/restore with some elements in internal sorting queue
		StreamStateHandle snapshot = harness.snapshotLegacy(0, 0);
		harness.close();

		harness = new OneInputStreamOperatorTestHarness<>(
				new CEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						new NFAFactory()));

		harness.setup();
		harness.restore(snapshot);
		harness.open();

		harness.processWatermark(new Watermark(Long.MIN_VALUE));

		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));

		// if element timestamps are not correctly checkpointed/restored this will lead to
		// a pruning time underflow exception in NFA
		harness.processWatermark(new Watermark(2));

		// simulate snapshot/restore with empty element queue but NFA state
		StreamStateHandle snapshot2 = harness.snapshotLegacy(1, 1);
		harness.close();

		harness = new OneInputStreamOperatorTestHarness<>(
				new CEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						new NFAFactory()));

		harness.setup();
		harness.restore(snapshot2);
		harness.open();

		harness.processElement(new StreamRecord<Event>(middleEvent, 3));
		harness.processElement(new StreamRecord<Event>(new Event(42, "start", 1.0), 4));
		harness.processElement(new StreamRecord<Event>(endEvent, 5));

		harness.processWatermark(new Watermark(Long.MAX_VALUE));

		ConcurrentLinkedQueue<Object> result = harness.getOutput();

		// watermark and the result
		assertEquals(2, result.size());

		Object resultObject = result.poll();
		assertTrue(resultObject instanceof StreamRecord);
		StreamRecord<?> resultRecord = (StreamRecord<?>) resultObject;
		assertTrue(resultRecord.getValue() instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap = (Map<String, Event>) resultRecord.getValue();

		assertEquals(startEvent, patternMap.get("start"));
		assertEquals(middleEvent, patternMap.get("middle"));
		assertEquals(endEvent, patternMap.get("end"));

		harness.close();
	}

	@Test
	public void testKeyedCEPOperatorCheckpointing() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		harness.open();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(42, "end", 1.0);

		harness.processElement(new StreamRecord<Event>(startEvent, 1));
		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));

		// simulate snapshot/restore with some elements in internal sorting queue
		StreamStateHandle snapshot = harness.snapshotLegacy(0, 0);
		harness.close();

		harness = new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		harness.setup();
		harness.restore(snapshot);
		harness.open();

		harness.processWatermark(new Watermark(Long.MIN_VALUE));

		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));

		// if element timestamps are not correctly checkpointed/restored this will lead to
		// a pruning time underflow exception in NFA
		harness.processWatermark(new Watermark(2));

		// simulate snapshot/restore with empty element queue but NFA state
		StreamStateHandle snapshot2 = harness.snapshotLegacy(1, 1);
		harness.close();

		harness = new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		harness.setup();
		harness.restore(snapshot2);
		harness.open();

		harness.processElement(new StreamRecord<Event>(middleEvent, 3));
		harness.processElement(new StreamRecord<Event>(new Event(42, "start", 1.0), 4));
		harness.processElement(new StreamRecord<Event>(endEvent, 5));

		harness.processWatermark(new Watermark(Long.MAX_VALUE));

		ConcurrentLinkedQueue<Object> result = harness.getOutput();

		// watermark and the result
		assertEquals(2, result.size());

		Object resultObject = result.poll();
		assertTrue(resultObject instanceof StreamRecord);
		StreamRecord<?> resultRecord = (StreamRecord<?>) resultObject;
		assertTrue(resultRecord.getValue() instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap = (Map<String, Event>) resultRecord.getValue();

		assertEquals(startEvent, patternMap.get("start"));
		assertEquals(middleEvent, patternMap.get("middle"));
		assertEquals(endEvent, patternMap.get("end"));

		harness.close();
	}

	@Test
	public void testKeyedCEPOperatorCheckpointingWithRocksDB() throws Exception {

		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		harness.setStateBackend(rocksDBStateBackend);

		harness.open();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(42, "end", 1.0);

		harness.processElement(new StreamRecord<Event>(startEvent, 1));
		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));

		// simulate snapshot/restore with some elements in internal sorting queue
		StreamStateHandle snapshot = harness.snapshotLegacy(0, 0);
		harness.close();

		harness = new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);
		harness.setStateBackend(rocksDBStateBackend);

		harness.setup();
		harness.restore(snapshot);
		harness.open();

		harness.processWatermark(new Watermark(Long.MIN_VALUE));

		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));

		// if element timestamps are not correctly checkpointed/restored this will lead to
		// a pruning time underflow exception in NFA
		harness.processWatermark(new Watermark(2));

		// simulate snapshot/restore with empty element queue but NFA state
		StreamStateHandle snapshot2 = harness.snapshotLegacy(1, 1);
		harness.close();

		harness = new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);
		harness.setStateBackend(rocksDBStateBackend);
		harness.setup();
		harness.restore(snapshot2);
		harness.open();

		harness.processElement(new StreamRecord<Event>(middleEvent, 3));
		harness.processElement(new StreamRecord<Event>(new Event(42, "start", 1.0), 4));
		harness.processElement(new StreamRecord<Event>(endEvent, 5));

		harness.processWatermark(new Watermark(Long.MAX_VALUE));

		ConcurrentLinkedQueue<Object> result = harness.getOutput();

		// watermark and the result
		assertEquals(2, result.size());

		Object resultObject = result.poll();
		assertTrue(resultObject instanceof StreamRecord);
		StreamRecord<?> resultRecord = (StreamRecord<?>) resultObject;
		assertTrue(resultRecord.getValue() instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap = (Map<String, Event>) resultRecord.getValue();

		assertEquals(startEvent, patternMap.get("start"));
		assertEquals(middleEvent, patternMap.get("middle"));
		assertEquals(endEvent, patternMap.get("end"));

		harness.close();
	}

	/**
	 * Tests that the internal time of a CEP operator advances only given watermarks. See FLINK-5033
	 */
	@Test
	public void testKeyedAdvancingTimeWithoutElements() throws Exception {
		final KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};
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
				new NFAFactory(true)),
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

			assertEquals(3, result.size());

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

	/**
	 * Tests that the internal time of a CEP operator advances only given watermarks. See FLINK-5033
	 */
	@Test
	public void testAdvancingTimeWithoutElements() throws Exception {
		final Event startEvent = new Event(42, "start", 1.0);
		final long watermarkTimestamp1 = 5L;
		final long watermarkTimestamp2 = 13L;

		final Map<String, Event> expectedSequence = new HashMap<>(2);
		expectedSequence.put("start", startEvent);

		OneInputStreamOperatorTestHarness<Event, Either<Tuple2<Map<String, Event>, Long>, Map<String, Event>>> harness = new OneInputStreamOperatorTestHarness<>(
			new TimeoutCEPPatternOperator<>(
				Event.createTypeSerializer(),
				false,
				new NFAFactory(true))
		);

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

			assertEquals(3, result.size());

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

			Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
						private static final long serialVersionUID = 5726188262756267490L;

						@Override
						public boolean filter(Event value) throws Exception {
							return value.getName().equals("start");
						}
					})
					.followedBy("middle").subtype(SubEvent.class).where(new FilterFunction<SubEvent>() {
						private static final long serialVersionUID = 6215754202506583964L;

						@Override
						public boolean filter(SubEvent value) throws Exception {
							return value.getVolume() > 5.0;
						}
					})
					.followedBy("end").where(new FilterFunction<Event>() {
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
