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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CEPMigration12to13Test {

	private static String getResourceFilename(String filename) {
		ClassLoader cl = CEPMigration12to13Test.class.getClassLoader();
		URL resource = cl.getResource(filename);
		if (resource == null) {
			throw new NullPointerException("Missing snapshot resource.");
		}
		return resource.getFile();
	}

	@Test
	public void testMigrationAfterBranchingPattern() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		final Event startEvent = new Event(42, "start", 1.0);
		final SubEvent middleEvent1 = new SubEvent(42, "foo1", 1.0, 10.0);
		final SubEvent middleEvent2 = new SubEvent(42, "foo2", 2.0, 10.0);
		final Event endEvent = new Event(42, "end", 1.0);

		// uncomment these lines for regenerating the snapshot on Flink 1.2
//		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness =
//			new KeyedOneInputStreamOperatorTestHarness<>(
//				new KeyedCEPPatternOperator<>(
//					Event.createTypeSerializer(),
//					false,
//					keySelector,
//					IntSerializer.INSTANCE,
//					new NFAFactory(),
//					true),
//				keySelector,
//				BasicTypeInfo.INT_TYPE_INFO);
//
//		harness.setup();
//		harness.open();
//		harness.processElement(new StreamRecord<Event>(startEvent, 1));
//		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));
//		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));
//		harness.processElement(new StreamRecord<Event>(middleEvent1, 2));
//		harness.processElement(new StreamRecord<Event>(middleEvent2, 3));
//		harness.processWatermark(new Watermark(5));
//		// simulate snapshot/restore with empty element queue but NFA state
//		OperatorStateHandles snapshot = harness.snapshot(1, 1);
//		FileOutputStream out = new FileOutputStream(
//				"src/test/resources/cep-branching-snapshot-1.2");
//		ObjectOutputStream oos = new ObjectOutputStream(out);
//		oos.writeObject(snapshot.getOperatorChainIndex());
//		oos.writeObject(snapshot.getLegacyOperatorState());
//		oos.writeObject(snapshot.getManagedKeyedState());
//		oos.writeObject(snapshot.getRawKeyedState());
//		oos.writeObject(snapshot.getManagedOperatorState());
//		oos.writeObject(snapshot.getRawOperatorState());
//		out.close();
//		harness.close();

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness =
			new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
					Event.createTypeSerializer(),
					false,
					keySelector,
					IntSerializer.INSTANCE,
					new NFAFactory(),
					null,
					true),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		harness.setup();
		final ObjectInputStream ois = new ObjectInputStream(new FileInputStream(getResourceFilename(
			"cep-branching-snapshot-1.2")));
		final OperatorStateHandles snapshot = new OperatorStateHandles(
			(int) ois.readObject(),
			(StreamStateHandle) ois.readObject(),
			(Collection<KeyedStateHandle>) ois.readObject(),
			(Collection<KeyedStateHandle>) ois.readObject(),
			(Collection<OperatorStateHandle>) ois.readObject(),
			(Collection<OperatorStateHandle>) ois.readObject()
		);
		harness.initializeState(snapshot);
		harness.open();

		harness.processElement(new StreamRecord<>(new Event(42, "start", 1.0), 4));
		harness.processElement(new StreamRecord<>(endEvent, 5));

		harness.processWatermark(new Watermark(20));

		ConcurrentLinkedQueue<Object> result = harness.getOutput();

		// watermark and 2 results
		assertEquals(3, result.size());

		Object resultObject1 = result.poll();
		assertTrue(resultObject1 instanceof StreamRecord);
		StreamRecord<?> resultRecord1 = (StreamRecord<?>) resultObject1;
		assertTrue(resultRecord1.getValue() instanceof Map);

		Object resultObject2 = result.poll();
		assertTrue(resultObject2 instanceof StreamRecord);
		StreamRecord<?> resultRecord2 = (StreamRecord<?>) resultObject2;
		assertTrue(resultRecord2.getValue() instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap1 = (Map<String, Event>) resultRecord1.getValue();

		assertEquals(startEvent, patternMap1.get("start"));
		assertEquals(middleEvent1, patternMap1.get("middle"));
		assertEquals(endEvent, patternMap1.get("end"));

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap2 = (Map<String, Event>) resultRecord2.getValue();

		assertEquals(startEvent, patternMap2.get("start"));
		assertEquals(middleEvent2, patternMap2.get("middle"));
		assertEquals(endEvent, patternMap2.get("end"));

		harness.close();
	}

	@Test
	public void testStartingNewPatternAfterMigration() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		final Event startEvent1 = new Event(42, "start", 1.0);
		final SubEvent middleEvent1 = new SubEvent(42, "foo1", 1.0, 10.0);
		final Event startEvent2 = new Event(42, "start", 5.0);
		final SubEvent middleEvent2 = new SubEvent(42, "foo2", 2.0, 10.0);
		final Event endEvent = new Event(42, "end", 1.0);

		// uncomment these lines for regenerating the snapshot on Flink 1.2
//		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness =
//			new KeyedOneInputStreamOperatorTestHarness<>(
//				new KeyedCEPPatternOperator<>(
//					Event.createTypeSerializer(),
//					false,
//					keySelector,
//					IntSerializer.INSTANCE,
//					new NFAFactory(),
//					true),
//				keySelector,
//				BasicTypeInfo.INT_TYPE_INFO);
//
//		harness.setup();
//		harness.open();
//		harness.processElement(new StreamRecord<Event>(startEvent1, 1));
//		harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));
//		harness.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));
//		harness.processElement(new StreamRecord<Event>(middleEvent1, 2));
//		harness.processWatermark(new Watermark(5));
//		// simulate snapshot/restore with empty element queue but NFA state
//		OperatorStateHandles snapshot = harness.snapshot(1, 1);
//		FileOutputStream out = new FileOutputStream(
//				"src/test/resources/cep-starting-snapshot-1.2");
//		ObjectOutputStream oos = new ObjectOutputStream(out);
//		oos.writeObject(snapshot.getOperatorChainIndex());
//		oos.writeObject(snapshot.getLegacyOperatorState());
//		oos.writeObject(snapshot.getManagedKeyedState());
//		oos.writeObject(snapshot.getRawKeyedState());
//		oos.writeObject(snapshot.getManagedOperatorState());
//		oos.writeObject(snapshot.getRawOperatorState());
//		out.close();
//		harness.close();

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness =
			new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
					Event.createTypeSerializer(),
					false,
					keySelector,
					IntSerializer.INSTANCE,
					new NFAFactory(),
					null,
					true),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		harness.setup();
		final ObjectInputStream ois = new ObjectInputStream(new FileInputStream(getResourceFilename(
			"cep-starting-snapshot-1.2")));
		final OperatorStateHandles snapshot = new OperatorStateHandles(
			(int) ois.readObject(),
			(StreamStateHandle) ois.readObject(),
			(Collection<KeyedStateHandle>) ois.readObject(),
			(Collection<KeyedStateHandle>) ois.readObject(),
			(Collection<OperatorStateHandle>) ois.readObject(),
			(Collection<OperatorStateHandle>) ois.readObject()
		);
		harness.initializeState(snapshot);
		harness.open();

		harness.processElement(new StreamRecord<>(startEvent2, 5));
		harness.processElement(new StreamRecord<Event>(middleEvent2, 6));
		harness.processElement(new StreamRecord<>(endEvent, 7));

		harness.processWatermark(new Watermark(20));

		ConcurrentLinkedQueue<Object> result = harness.getOutput();

		// watermark and 3 results
		assertEquals(4, result.size());

		Object resultObject1 = result.poll();
		assertTrue(resultObject1 instanceof StreamRecord);
		StreamRecord<?> resultRecord1 = (StreamRecord<?>) resultObject1;
		assertTrue(resultRecord1.getValue() instanceof Map);

		Object resultObject2 = result.poll();
		assertTrue(resultObject2 instanceof StreamRecord);
		StreamRecord<?> resultRecord2 = (StreamRecord<?>) resultObject2;
		assertTrue(resultRecord2.getValue() instanceof Map);

		Object resultObject3 = result.poll();
		assertTrue(resultObject3 instanceof StreamRecord);
		StreamRecord<?> resultRecord3 = (StreamRecord<?>) resultObject3;
		assertTrue(resultRecord3.getValue() instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap1 = (Map<String, Event>) resultRecord1.getValue();

		assertEquals(startEvent1, patternMap1.get("start"));
		assertEquals(middleEvent1, patternMap1.get("middle"));
		assertEquals(endEvent, patternMap1.get("end"));

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap2 = (Map<String, Event>) resultRecord2.getValue();

		assertEquals(startEvent1, patternMap2.get("start"));
		assertEquals(middleEvent2, patternMap2.get("middle"));
		assertEquals(endEvent, patternMap2.get("end"));

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap3 = (Map<String, Event>) resultRecord3.getValue();

		assertEquals(startEvent2, patternMap3.get("start"));
		assertEquals(middleEvent2, patternMap3.get("middle"));
		assertEquals(endEvent, patternMap3.get("end"));

		harness.close();
	}

	@Test
	public void testSinglePatternAfterMigration() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		final Event startEvent1 = new Event(42, "start", 1.0);

		// uncomment these lines for regenerating the snapshot on Flink 1.2
//		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness =
//			new KeyedOneInputStreamOperatorTestHarness<>(
//				new KeyedCEPPatternOperator<>(
//					Event.createTypeSerializer(),
//					false,
//					keySelector,
//					IntSerializer.INSTANCE,
//					new SinglePatternNFAFactory(),
//					true),
//				keySelector,
//				BasicTypeInfo.INT_TYPE_INFO);
//
//		harness.setup();
//		harness.open();
//		harness.processWatermark(new Watermark(5));
//		// simulate snapshot/restore with empty element queue but NFA state
//		OperatorStateHandles snapshot = harness.snapshot(1, 1);
//		FileOutputStream out = new FileOutputStream(
//				"src/test/resources/cep-single-pattern-snapshot-1.2");
//		ObjectOutputStream oos = new ObjectOutputStream(out);
//		oos.writeObject(snapshot.getOperatorChainIndex());
//		oos.writeObject(snapshot.getLegacyOperatorState());
//		oos.writeObject(snapshot.getManagedKeyedState());
//		oos.writeObject(snapshot.getRawKeyedState());
//		oos.writeObject(snapshot.getManagedOperatorState());
//		oos.writeObject(snapshot.getRawOperatorState());
//		out.close();
//		harness.close();

		OneInputStreamOperatorTestHarness<Event, Map<String, Event>> harness =
			new KeyedOneInputStreamOperatorTestHarness<>(
				new KeyedCEPPatternOperator<>(
					Event.createTypeSerializer(),
					false,
					keySelector,
					IntSerializer.INSTANCE,
					new SinglePatternNFAFactory(),
					null,
					true),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		harness.setup();
		final ObjectInputStream ois = new ObjectInputStream(new FileInputStream(getResourceFilename(
			"cep-single-pattern-snapshot-1.2")));
		final OperatorStateHandles snapshot = new OperatorStateHandles(
			(int) ois.readObject(),
			(StreamStateHandle) ois.readObject(),
			(Collection<KeyedStateHandle>) ois.readObject(),
			(Collection<KeyedStateHandle>) ois.readObject(),
			(Collection<OperatorStateHandle>) ois.readObject(),
			(Collection<OperatorStateHandle>) ois.readObject()
		);
		harness.initializeState(snapshot);
		harness.open();

		harness.processElement(new StreamRecord<>(startEvent1, 5));

		harness.processWatermark(new Watermark(20));

		ConcurrentLinkedQueue<Object> result = harness.getOutput();

		// watermark and the result
		assertEquals(2, result.size());

		Object resultObject = result.poll();
		assertTrue(resultObject instanceof StreamRecord);
		StreamRecord<?> resultRecord = (StreamRecord<?>) resultObject;
		assertTrue(resultRecord.getValue() instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, Event> patternMap = (Map<String, Event>) resultRecord.getValue();

		assertEquals(startEvent1, patternMap.get("start"));

		harness.close();
	}

	private static class SinglePatternNFAFactory implements NFACompiler.NFAFactory<Event> {

		private static final long serialVersionUID = 1173020762472766713L;

		private final boolean handleTimeout;

		private SinglePatternNFAFactory() {
			this(false);
		}

		private SinglePatternNFAFactory(boolean handleTimeout) {
			this.handleTimeout = handleTimeout;
		}

		@Override
		public NFA<Event> createNFA() {

			Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new StartFilter())
				.within(Time.milliseconds(10L));

			return NFACompiler.compile(pattern, Event.createTypeSerializer(), handleTimeout);
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
				.followedBy("middle")
				.subtype(SubEvent.class)
				.where(new MiddleFilter())
				.followedBy("end")
				.where(new EndFilter())
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
