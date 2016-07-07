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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import java.util.Map;
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

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new OneInputStreamOperatorTestHarness<>(
			new KeyedCEPPatternOperator<>(
				Event.createTypeSerializer(),
				false,
				keySelector,
				IntSerializer.INSTANCE,
			new NFAFactory())
		);

		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);

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
		StreamTaskState snapshot = harness.snapshot(0, 0);

		harness = new OneInputStreamOperatorTestHarness<>(
				new CEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						new NFAFactory()));

		harness.setup();
		harness.restore(snapshot, 1);
		harness.open();

		harness.processWatermark(new Watermark(Long.MIN_VALUE));

		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));

		// if element timestamps are not correctly checkpointed/restored this will lead to
		// a pruning time underflow exception in NFA
		harness.processWatermark(new Watermark(2));

		// simulate snapshot/restore with empty element queue but NFA state
		StreamTaskState snapshot2 = harness.snapshot(1, 1);

		harness = new OneInputStreamOperatorTestHarness<>(
				new CEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						new NFAFactory()));

		harness.setup();
		harness.restore(snapshot2, 2);
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

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new OneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()));

		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);

		harness.open();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(42, "end", 1.0);

		harness.processElement(new StreamRecord<Event>(startEvent, 1));
		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));

		// simulate snapshot/restore with some elements in internal sorting queue
		StreamTaskState snapshot = harness.snapshot(0, 0);

		harness = new OneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()));

		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);
		harness.setup();
		harness.restore(snapshot, 1);
		harness.open();

		harness.processWatermark(new Watermark(Long.MIN_VALUE));

		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));

		// if element timestamps are not correctly checkpointed/restored this will lead to
		// a pruning time underflow exception in NFA
		harness.processWatermark(new Watermark(2));

		// simulate snapshot/restore with empty element queue but NFA state
		StreamTaskState snapshot2 = harness.snapshot(1, 1);

		harness = new OneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()));

		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);
		harness.setup();
		harness.restore(snapshot2, 2);
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
		String rocksDbBackups = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDBStateBackend =
				new RocksDBStateBackend(rocksDbBackups, new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new OneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()));

		harness.setStateBackend(rocksDBStateBackend);
		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);

		harness.open();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(42, "end", 1.0);

		harness.processElement(new StreamRecord<Event>(startEvent, 1));
		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));

		// simulate snapshot/restore with some elements in internal sorting queue
		StreamTaskState snapshot = harness.snapshot(0, 0);

		harness = new OneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()));

		rocksDBStateBackend =
				new RocksDBStateBackend(rocksDbBackups, new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);
		harness.setStateBackend(rocksDBStateBackend);
		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);
		harness.setup();
		harness.restore(snapshot, 1);
		harness.open();

		harness.processWatermark(new Watermark(Long.MIN_VALUE));

		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));

		// if element timestamps are not correctly checkpointed/restored this will lead to
		// a pruning time underflow exception in NFA
		harness.processWatermark(new Watermark(2));

		// simulate snapshot/restore with empty element queue but NFA state
		StreamTaskState snapshot2 = harness.snapshot(1, 1);

		harness = new OneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()));

		rocksDBStateBackend =
				new RocksDBStateBackend(rocksDbBackups, new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);
		harness.setStateBackend(rocksDBStateBackend);
		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);
		harness.setup();
		harness.restore(snapshot2, 2);
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

	private static class NFAFactory implements NFACompiler.NFAFactory<Event> {

		private static final long serialVersionUID = 1173020762472766713L;

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
					.within(Time.milliseconds(10));

			return NFACompiler.compile(pattern, Event.createTypeSerializer(), false);
		}
	}
}
