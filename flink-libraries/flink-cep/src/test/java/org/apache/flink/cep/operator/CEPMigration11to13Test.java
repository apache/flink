/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CEPMigration11to13Test {

	private static String getResourceFilename(String filename) {
		ClassLoader cl = CEPMigration11to13Test.class.getClassLoader();
		URL resource = cl.getResource(filename);
		if (resource == null) {
			throw new NullPointerException("Missing snapshot resource.");
		}
		return resource.getFile();
	}

	@Test
	public void testKeyedCEPOperatorMigratation() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		final Event startEvent = new Event(42, "start", 1.0);
		final SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		final Event endEvent = new Event(42, "end", 1.0);

		// uncomment these lines for regenerating the snapshot on Flink 1.1
		/*
		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new OneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						keySelector,
						IntSerializer.INSTANCE,
						new NFAFactory()));
		harness.configureForKeyedStream(keySelector, BasicTypeInfo.INT_TYPE_INFO);
		harness.open();
		harness.processElement(new StreamRecord<Event>(startEvent, 1));
		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));
		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));
		harness.processWatermark(new Watermark(2));

		harness.processElement(new StreamRecord<Event>(middleEvent, 3));

		// simulate snapshot/restore with empty element queue but NFA state
		StreamTaskState snapshot = harness.snapshot(1, 1);
		FileOutputStream out = new FileOutputStream(
				"src/test/resources/cep-keyed-1_1-snapshot");
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeObject(snapshot);
		out.close();
		harness.close();
		*/

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						new KeyedCEPPatternOperator<>(
								Event.createTypeSerializer(),
								false,
								IntSerializer.INSTANCE,
								new NFAFactory(),
								true),
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();
			harness
				.initializeStateFromLegacyCheckpoint(getResourceFilename("cep-keyed-1_1-snapshot"));
			harness.open();

			harness.processElement(new StreamRecord<>(new Event(42, "start", 1.0), 4));
			harness.processElement(new StreamRecord<>(endEvent, 5));

			harness.processWatermark(new Watermark(20));

			ConcurrentLinkedQueue<Object> result = harness.getOutput();

			// watermark and the result
			assertEquals(2, result.size());

			Object resultObject = result.poll();
			assertTrue(resultObject instanceof StreamRecord);
			StreamRecord<?> resultRecord = (StreamRecord<?>) resultObject;
			assertTrue(resultRecord.getValue() instanceof Map);

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap =
				(Map<String, List<Event>>) resultRecord.getValue();

			assertEquals(startEvent, patternMap.get("start").get(0));
			assertEquals(middleEvent, patternMap.get("middle").get(0));
			assertEquals(endEvent, patternMap.get("end").get(0));

			// and now go for a checkpoint with the new serializers

			final Event startEvent1 = new Event(42, "start", 2.0);
			final SubEvent middleEvent1 = new SubEvent(42, "foo", 1.0, 11.0);
			final Event endEvent1 = new Event(42, "end", 2.0);

			harness.processElement(new StreamRecord<Event>(startEvent1, 21));
			harness.processElement(new StreamRecord<Event>(middleEvent1, 23));

			// simulate snapshot/restore with some elements in internal sorting queue
			OperatorStateHandles snapshot = harness.snapshot(1L, 1L);
			harness.close();

			harness = new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
					Event.createTypeSerializer(),
					false,
					IntSerializer.INSTANCE,
					new NFAFactory(),
					true),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

			harness.setup();
			harness.initializeState(snapshot);
			harness.open();

			harness.processElement(new StreamRecord<>(endEvent1, 25));

			harness.processWatermark(new Watermark(50));

			result = harness.getOutput();

			// watermark and the result
			assertEquals(2, result.size());

			Object resultObject1 = result.poll();
			assertTrue(resultObject1 instanceof StreamRecord);
			StreamRecord<?> resultRecord1 = (StreamRecord<?>) resultObject1;
			assertTrue(resultRecord1.getValue() instanceof Map);

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap1 =
				(Map<String, List<Event>>) resultRecord1.getValue();

			assertEquals(startEvent1, patternMap1.get("start").get(0));
			assertEquals(middleEvent1, patternMap1.get("middle").get(0));
			assertEquals(endEvent1, patternMap1.get("end").get(0));
		} finally {
			harness.close();
		}
	}

	@Test
	public void testNonKeyedCEPFunctionMigration() throws Exception {

		final Event startEvent = new Event(42, "start", 1.0);
		final SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		final Event endEvent=  new Event(42, "end", 1.0);

		// uncomment these lines for regenerating the snapshot on Flink 1.1
		/*
		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness = new OneInputStreamOperatorTestHarness<>(
				new CEPPatternOperator<>(
						Event.createTypeSerializer(),
						false,
						new NFAFactory()));
		harness.open();
		harness.processElement(new StreamRecord<Event>(startEvent, 1));
		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));
		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));
		harness.processWatermark(new Watermark(2));

		harness.processElement(new StreamRecord<Event>(middleEvent, 3));

		// simulate snapshot/restore with empty element queue but NFA state
		StreamTaskState snapshot = harness.snapshot(1, 1);
		FileOutputStream out = new FileOutputStream(
				"src/test/resources/cep-non-keyed-1.1-snapshot");
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeObject(snapshot);
		out.close();
		harness.close();
		*/

		NullByteKeySelector keySelector = new NullByteKeySelector();

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<Byte, Event, Map<String, List<Event>>>(
						new KeyedCEPPatternOperator<>(
								Event.createTypeSerializer(),
								false,
								ByteSerializer.INSTANCE,
								new NFAFactory(),
								false),
						keySelector,
						BasicTypeInfo.BYTE_TYPE_INFO);

		try {
			harness.setup();
			harness.initializeStateFromLegacyCheckpoint(
				getResourceFilename("cep-non-keyed-1.1-snapshot"));
			harness.open();

			harness.processElement(new StreamRecord<>(new Event(42, "start", 1.0), 4));
			harness.processElement(new StreamRecord<>(endEvent, 5));

			harness.processWatermark(new Watermark(20));

			ConcurrentLinkedQueue<Object> result = harness.getOutput();

			// watermark and the result
			assertEquals(2, result.size());

			Object resultObject = result.poll();
			assertTrue(resultObject instanceof StreamRecord);
			StreamRecord<?> resultRecord = (StreamRecord<?>) resultObject;
			assertTrue(resultRecord.getValue() instanceof Map);

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap =
				(Map<String, List<Event>>) resultRecord.getValue();

			assertEquals(startEvent, patternMap.get("start").get(0));
			assertEquals(middleEvent, patternMap.get("middle").get(0));
			assertEquals(endEvent, patternMap.get("end").get(0));

			// and now go for a checkpoint with the new serializers

			final Event startEvent1 = new Event(42, "start", 2.0);
			final SubEvent middleEvent1 = new SubEvent(42, "foo", 1.0, 11.0);
			final Event endEvent1 = new Event(42, "end", 2.0);

			harness.processElement(new StreamRecord<Event>(startEvent1, 21));
			harness.processElement(new StreamRecord<Event>(middleEvent1, 23));

			// simulate snapshot/restore with some elements in internal sorting queue
			OperatorStateHandles snapshot = harness.snapshot(1L, 1L);
			harness.close();

			harness = new KeyedOneInputStreamOperatorTestHarness<Byte, Event, Map<String, List<Event>>>(
				new KeyedCEPPatternOperator<>(
					Event.createTypeSerializer(),
					false,
					ByteSerializer.INSTANCE,
					new NFAFactory(),
					false),
				keySelector,
				BasicTypeInfo.BYTE_TYPE_INFO);

			harness.setup();
			harness.initializeState(snapshot);
			harness.open();

			harness.processElement(new StreamRecord<>(endEvent1, 25));

			harness.processWatermark(new Watermark(50));

			result = harness.getOutput();

			// watermark and the result
			assertEquals(2, result.size());

			Object resultObject1 = result.poll();
			assertTrue(resultObject1 instanceof StreamRecord);
			StreamRecord<?> resultRecord1 = (StreamRecord<?>) resultObject1;
			assertTrue(resultRecord1.getValue() instanceof Map);

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap1 =
				(Map<String, List<Event>>) resultRecord1.getValue();

			assertEquals(startEvent1, patternMap1.get("start").get(0));
			assertEquals(middleEvent1, patternMap1.get("middle").get(0));
			assertEquals(endEvent1, patternMap1.get("end").get(0));
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

			Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new StartFilter())
					.followedBy("middle").subtype(SubEvent.class).where(new MiddleFilter())
					.followedBy("end").where(new EndFilter())
					// add a window timeout to test whether timestamps of elements in the
					// priority queue in CEP operator are correctly checkpointed/restored
					.within(Time.milliseconds(10L));

			return NFACompiler.compile(pattern, Event.createTypeSerializer(), handleTimeout);
		}
	}

	private static class StartFilter extends SimpleCondition<Event> {
		private static final long serialVersionUID = 5726188262756267490L;

		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("start");
		}
	}

	private static class MiddleFilter extends SimpleCondition<SubEvent> {
		private static final long serialVersionUID = 6215754202506583964L;

		@Override
		public boolean filter(SubEvent value) throws Exception {
			return value.getVolume() > 5.0;
		}
	}

	private static class EndFilter extends SimpleCondition<Event> {
		private static final long serialVersionUID = 7056763917392056548L;

		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("end");
		}
	}
}
