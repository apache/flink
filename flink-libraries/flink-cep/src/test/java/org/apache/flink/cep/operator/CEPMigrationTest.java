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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.streaming.util.migration.MigrationTestUtil;
import org.apache.flink.streaming.util.migration.MigrationVersion;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.cep.operator.CepOperatorTestUtilities.getKeyedCepOpearator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for checking whether CEP operator can restore from snapshots that were done
 * using previous Flink versions.
 *
 * <p>For regenerating the binary snapshot file of previous versions you have to run the
 * {@code write*()} method on the corresponding Flink release-* branch.
 */
@RunWith(Parameterized.class)
public class CEPMigrationTest {

	/**
	 * TODO change this to the corresponding savepoint version to be written (e.g. {@link MigrationVersion#v1_3} for 1.3)
	 * TODO and remove all @Ignore annotations on write*Snapshot() methods to generate savepoints
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = null;

	private final MigrationVersion migrateVersion;

	@Parameterized.Parameters(name = "Migration Savepoint: {0}")
	public static Collection<MigrationVersion> parameters () {
		return Arrays.asList(MigrationVersion.v1_3, MigrationVersion.v1_4, MigrationVersion.v1_5);
	}

	public CEPMigrationTest(MigrationVersion migrateVersion) {
		this.migrateVersion = migrateVersion;
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeAfterBranchingPatternSnapshot() throws Exception {

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

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<>(
					getKeyedCepOpearator(false, new NFAFactory()),
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();
			harness.open();

			harness.processElement(new StreamRecord<Event>(startEvent, 1));
			harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));
			harness
				.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));
			harness.processElement(new StreamRecord<Event>(middleEvent1, 2));
			harness.processElement(new StreamRecord<Event>(middleEvent2, 3));

			harness.processWatermark(new Watermark(5));

			// do snapshot and save to file
			OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
			OperatorSnapshotUtil.writeStateHandle(snapshot,
				"src/test/resources/cep-migration-after-branching-flink" + flinkGenerateSavepointVersion + "-snapshot");
		} finally {
			harness.close();
		}
	}

	@Test
	public void testRestoreAfterBranchingPattern() throws Exception {

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

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						getKeyedCepOpearator(false, new NFAFactory()),
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();

			MigrationTestUtil.restoreFromSnapshot(
				harness,
				OperatorSnapshotUtil.getResourceFilename("cep-migration-after-branching-flink" + migrateVersion + "-snapshot"),
				migrateVersion);

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
			Map<String, List<Event>> patternMap1 =
				(Map<String, List<Event>>) resultRecord1.getValue();

			assertEquals(startEvent, patternMap1.get("start").get(0));
			assertEquals(middleEvent1, patternMap1.get("middle").get(0));
			assertEquals(endEvent, patternMap1.get("end").get(0));

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap2 =
				(Map<String, List<Event>>) resultRecord2.getValue();

			assertEquals(startEvent, patternMap2.get("start").get(0));
			assertEquals(middleEvent2, patternMap2.get("middle").get(0));
			assertEquals(endEvent, patternMap2.get("end").get(0));

			// and now go for a checkpoint with the new serializers

			final Event startEvent1 = new Event(42, "start", 2.0);
			final SubEvent middleEvent3 = new SubEvent(42, "foo", 1.0, 11.0);
			final Event endEvent1 = new Event(42, "end", 2.0);

			harness.processElement(new StreamRecord<Event>(startEvent1, 21));
			harness.processElement(new StreamRecord<Event>(middleEvent3, 23));

			// simulate snapshot/restore with some elements in internal sorting queue
			OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
			harness.close();

			harness = new KeyedOneInputStreamOperatorTestHarness<>(
				getKeyedCepOpearator(false, new NFAFactory()),
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

			Object resultObject3 = result.poll();
			assertTrue(resultObject3 instanceof StreamRecord);
			StreamRecord<?> resultRecord3 = (StreamRecord<?>) resultObject3;
			assertTrue(resultRecord3.getValue() instanceof Map);

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap3 =
				(Map<String, List<Event>>) resultRecord3.getValue();

			assertEquals(startEvent1, patternMap3.get("start").get(0));
			assertEquals(middleEvent3, patternMap3.get("middle").get(0));
			assertEquals(endEvent1, patternMap3.get("end").get(0));
		} finally {
			harness.close();
		}
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeStartingNewPatternAfterMigrationSnapshot() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		final Event startEvent1 = new Event(42, "start", 1.0);
		final SubEvent middleEvent1 = new SubEvent(42, "foo1", 1.0, 10.0);

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<>(
					getKeyedCepOpearator(false, new NFAFactory()),
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();
			harness.open();
			harness.processElement(new StreamRecord<Event>(startEvent1, 1));
			harness.processElement(new StreamRecord<Event>(new Event(42, "foobar", 1.0), 2));
			harness
				.processElement(new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3));
			harness.processElement(new StreamRecord<Event>(middleEvent1, 2));
			harness.processWatermark(new Watermark(5));

			// do snapshot and save to file
			OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
			OperatorSnapshotUtil.writeStateHandle(snapshot,
				"src/test/resources/cep-migration-starting-new-pattern-flink" + flinkGenerateSavepointVersion + "-snapshot");
		} finally {
			harness.close();
		}
	}

	@Test
	public void testRestoreStartingNewPatternAfterMigration() throws Exception {

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

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<>(
					getKeyedCepOpearator(false, new NFAFactory()),
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();

			MigrationTestUtil.restoreFromSnapshot(
				harness,
				OperatorSnapshotUtil.getResourceFilename("cep-migration-starting-new-pattern-flink" + migrateVersion + "-snapshot"),
				migrateVersion);

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
			Map<String, List<Event>> patternMap1 =
				(Map<String, List<Event>>) resultRecord1.getValue();

			assertEquals(startEvent1, patternMap1.get("start").get(0));
			assertEquals(middleEvent1, patternMap1.get("middle").get(0));
			assertEquals(endEvent, patternMap1.get("end").get(0));

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap2 =
				(Map<String, List<Event>>) resultRecord2.getValue();

			assertEquals(startEvent1, patternMap2.get("start").get(0));
			assertEquals(middleEvent2, patternMap2.get("middle").get(0));
			assertEquals(endEvent, patternMap2.get("end").get(0));

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap3 =
				(Map<String, List<Event>>) resultRecord3.getValue();

			assertEquals(startEvent2, patternMap3.get("start").get(0));
			assertEquals(middleEvent2, patternMap3.get("middle").get(0));
			assertEquals(endEvent, patternMap3.get("end").get(0));

			// and now go for a checkpoint with the new serializers

			final Event startEvent3 = new Event(42, "start", 2.0);
			final SubEvent middleEvent3 = new SubEvent(42, "foo", 1.0, 11.0);
			final Event endEvent1 = new Event(42, "end", 2.0);

			harness.processElement(new StreamRecord<Event>(startEvent3, 21));
			harness.processElement(new StreamRecord<Event>(middleEvent3, 23));

			// simulate snapshot/restore with some elements in internal sorting queue
			OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
			harness.close();

			harness = new KeyedOneInputStreamOperatorTestHarness<>(
				getKeyedCepOpearator(false, new NFAFactory()),
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

			Object resultObject4 = result.poll();
			assertTrue(resultObject4 instanceof StreamRecord);
			StreamRecord<?> resultRecord4 = (StreamRecord<?>) resultObject4;
			assertTrue(resultRecord4.getValue() instanceof Map);

			@SuppressWarnings("unchecked")
			Map<String, List<Event>> patternMap4 =
				(Map<String, List<Event>>) resultRecord4.getValue();

			assertEquals(startEvent3, patternMap4.get("start").get(0));
			assertEquals(middleEvent3, patternMap4.get("middle").get(0));
			assertEquals(endEvent1, patternMap4.get("end").get(0));
		} finally {
			harness.close();
		}
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeSinglePatternAfterMigrationSnapshot() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		final Event startEvent1 = new Event(42, "start", 1.0);

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						getKeyedCepOpearator(false, new SinglePatternNFAFactory()),
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();
			harness.open();
			harness.processWatermark(new Watermark(5));

			// do snapshot and save to file
			OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
			OperatorSnapshotUtil.writeStateHandle(snapshot,
				"src/test/resources/cep-migration-single-pattern-afterwards-flink" + flinkGenerateSavepointVersion + "-snapshot");
		} finally {
			harness.close();
		}
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

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						getKeyedCepOpearator(false, new SinglePatternNFAFactory()),
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();

			MigrationTestUtil.restoreFromSnapshot(
				harness,
				OperatorSnapshotUtil.getResourceFilename("cep-migration-single-pattern-afterwards-flink" + migrateVersion + "-snapshot"),
				migrateVersion);

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
			Map<String, List<Event>> patternMap =
				(Map<String, List<Event>>) resultRecord.getValue();

			assertEquals(startEvent1, patternMap.get("start").get(0));
		} finally {
			harness.close();
		}
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeAndOrSubtypConditionsPatternAfterMigrationSnapshot() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		final Event startEvent1 = new SubEvent(42, "start", 1.0, 6.0);

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
			new KeyedOneInputStreamOperatorTestHarness<>(
				getKeyedCepOpearator(false, new NFAComplexConditionsFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();
			harness.open();
			harness.processElement(new StreamRecord<>(startEvent1, 5));
			harness.processWatermark(new Watermark(6));

			// do snapshot and save to file
			OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
			OperatorSnapshotUtil.writeStateHandle(snapshot,
				"src/test/resources/cep-migration-conditions-flink" + flinkGenerateSavepointVersion + "-snapshot");
		} finally {
			harness.close();
		}
	}

	@Test
	public void testAndOrSubtypeConditionsAfterMigration() throws Exception {

		KeySelector<Event, Integer> keySelector = new KeySelector<Event, Integer>() {
			private static final long serialVersionUID = -4873366487571254798L;

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		};

		final Event startEvent1 = new SubEvent(42, "start", 1.0, 6.0);

		OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
			new KeyedOneInputStreamOperatorTestHarness<>(
				getKeyedCepOpearator(false, new NFAComplexConditionsFactory()),
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);

		try {
			harness.setup();

			MigrationTestUtil.restoreFromSnapshot(
				harness,
				OperatorSnapshotUtil.getResourceFilename("cep-migration-conditions-flink" + migrateVersion + "-snapshot"),
				migrateVersion);

			harness.open();

			final Event endEvent = new SubEvent(42, "end", 1.0, 2.0);
			harness.processElement(new StreamRecord<>(endEvent, 9));
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

			assertEquals(startEvent1, patternMap.get("start").get(0));
			assertEquals(endEvent, patternMap.get("start").get(1));
		} finally {
			harness.close();
		}
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

			return NFACompiler.compileFactory(pattern, handleTimeout).createNFA();
		}
	}

	private static class NFAComplexConditionsFactory implements NFACompiler.NFAFactory<Event> {

		private static final long serialVersionUID = 1173020762472766713L;

		private final boolean handleTimeout;

		private NFAComplexConditionsFactory() {
			this(false);
		}

		private NFAComplexConditionsFactory(boolean handleTimeout) {
			this.handleTimeout = handleTimeout;
		}

		@Override
		public NFA<Event> createNFA() {

			Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
				.subtype(SubEvent.class)
				.where(new MiddleFilter())
				.or(new SubEventEndFilter())
				.times(2)
				.within(Time.milliseconds(10L));

			return NFACompiler.compileFactory(pattern, handleTimeout).createNFA();
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
					.followedByAny("middle")
					.subtype(SubEvent.class)
					.where(new MiddleFilter())
					.followedByAny("end")
					.where(new EndFilter())
					// add a window timeout to test whether timestamps of elements in the
					// priority queue in CEP operator are correctly checkpointed/restored
					.within(Time.milliseconds(10L));

			return NFACompiler.compileFactory(pattern, handleTimeout).createNFA();
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

	private static class SubEventEndFilter extends SimpleCondition<SubEvent> {
		private static final long serialVersionUID = 7056763917392056548L;

		@Override
		public boolean filter(SubEvent value) throws Exception {
			return value.getName().equals("end");
		}
	}
}
