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
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.util.Collector;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for checking whether {@link WindowOperator} can restore from snapshots that were done
 * using previous Flink versions' {@link WindowOperator}.
 *
 * <p>This also checks whether {@link WindowOperator} can restore from a checkpoint of the
 * aligned processing-time windows operator of previous Flink versions.
 *
 * <p>For regenerating the binary snapshot file you have to run the {@code write*()} method on
 * the corresponding Flink release-* branch.
 */
@RunWith(Parameterized.class)
public class WindowOperatorMigrationTest {

	@Parameterized.Parameters(name = "Migration Savepoint: {0}")
	public static Collection<MigrationVersion> parameters () {
		return Arrays.asList(
			MigrationVersion.v1_3,
			MigrationVersion.v1_4,
			MigrationVersion.v1_5,
			MigrationVersion.v1_6,
			MigrationVersion.v1_7,
			MigrationVersion.v1_8,
			MigrationVersion.v1_9);
	}

	private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
			TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

	/**
	 * TODO change this to the corresponding savepoint version to be written (e.g. {@link MigrationVersion#v1_3} for 1.3)
	 * TODO and remove all @Ignore annotations on write*Snapshot() methods to generate savepoints
	 * TODO Note: You should generate the savepoint based on the release branch instead of the master.
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = null;

	private final MigrationVersion testMigrateVersion;

	public WindowOperatorMigrationTest(MigrationVersion testMigrateVersion) {
		this.testMigrateVersion = testMigrateVersion;
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeSessionWindowsWithCountTriggerSnapshot() throws Exception {
		final int sessionSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<String>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new SessionWindowFunction()),
				PurgingTrigger.of(CountTrigger.of(4)),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 3500));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));

		// do snapshot and save to file
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);

		OperatorSnapshotUtil.writeStateHandle(
			snapshot,
			"src/test/resources/win-op-migration-test-session-with-stateful-trigger-flink" + flinkGenerateSavepointVersion + "-snapshot");

		testHarness.close();
	}

	@Test
	public void testRestoreSessionWindowsWithCountTrigger() throws Exception {

		final int sessionSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<String>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new SessionWindowFunction()),
				PurgingTrigger.of(CountTrigger.of(4)),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"win-op-migration-test-session-with-stateful-trigger-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 2500));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 6000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 6500));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 7000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		// add an element that merges the two "key1" sessions, they should now have count 6, and therefore fire
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 10), 4500));

		expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-22", 10L, 10000L), 9999L));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		testHarness.close();
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeSessionWindowsWithCountTriggerInMintConditionSnapshot() throws Exception {

		final int sessionSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<String>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new SessionWindowFunction()),
				PurgingTrigger.of(CountTrigger.of(4)),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		// do snapshot and save to file
		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
		OperatorSnapshotUtil.writeStateHandle(
			snapshot,
			"src/test/resources/win-op-migration-test-session-with-stateful-trigger-mint-flink" + flinkGenerateSavepointVersion + "-snapshot");

		testHarness.close();
	}

	/**
	 * This checks that we can restore from a virgin {@code WindowOperator} that has never seen
	 * any elements.
	 */
	@Test
	public void testRestoreSessionWindowsWithCountTriggerInMintCondition() throws Exception {

		final int sessionSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<String>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new SessionWindowFunction()),
				PurgingTrigger.of(CountTrigger.of(4)),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"win-op-migration-test-session-with-stateful-trigger-mint-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 3500));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 2500));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 6000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 6500));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 7000));

		expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-10", 0L, 6500L), 6499));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		// add an element that merges the two "key1" sessions, they should now have count 6, and therefore fire
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 10), 4500));

		expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-22", 10L, 10000L), 9999L));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		testHarness.close();
	}


	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeReducingEventTimeWindowsSnapshot() throws Exception {
		final int windowSize = 3;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer<>(),
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());

		// do snapshot and save to file
		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
		OperatorSnapshotUtil.writeStateHandle(
			snapshot,
			"src/test/resources/win-op-migration-test-reduce-event-time-flink" + flinkGenerateSavepointVersion + "-snapshot");

		testHarness.close();
	}

	@Test
	public void testRestoreReducingEventTimeWindows() throws Exception {
		final int windowSize = 3;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer<>(),
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"win-op-migration-test-reduce-event-time-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
		expectedOutput.add(new Watermark(2999));

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
		expectedOutput.add(new Watermark(5999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());
		testHarness.close();
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeApplyEventTimeWindowsSnapshot() throws Exception {
		final int windowSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new RichSumReducer<TimeWindow>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());

		// do snapshot and save to file
		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
		OperatorSnapshotUtil.writeStateHandle(
			snapshot,
			"src/test/resources/win-op-migration-test-apply-event-time-flink" + flinkGenerateSavepointVersion + "-snapshot");

		testHarness.close();
	}

	@Test
	public void testRestoreApplyEventTimeWindows() throws Exception {
		final int windowSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new RichSumReducer<TimeWindow>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"win-op-migration-test-apply-event-time-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
		expectedOutput.add(new Watermark(2999));

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
		expectedOutput.add(new Watermark(5999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());
		testHarness.close();
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeReducingProcessingTimeWindowsSnapshot() throws Exception {
		final int windowSize = 3;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer<>(),
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingProcessingTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				ProcessingTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(10);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1)));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1)));

		testHarness.setProcessingTime(3010);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1)));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key3", 1)));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 1), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 2999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());

		// do snapshot and save to file
		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
		OperatorSnapshotUtil.writeStateHandle(
			snapshot,
			"src/test/resources/win-op-migration-test-reduce-processing-time-flink" + flinkGenerateSavepointVersion + "-snapshot");

		testHarness.close();

	}

	@Test
	public void testRestoreReducingProcessingTimeWindows() throws Exception {
		final int windowSize = 3;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer<>(),
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingProcessingTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				ProcessingTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"win-op-migration-test-reduce-processing-time-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		testHarness.setProcessingTime(3020);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3)));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3)));

		testHarness.setProcessingTime(6000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), 5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key3", 1), 5999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());
		testHarness.close();
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeApplyProcessingTimeWindowsSnapshot() throws Exception {
		final int windowSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingProcessingTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new RichSumReducer<TimeWindow>()),
				ProcessingTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(10);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1)));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1)));

		testHarness.setProcessingTime(3010);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1)));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key3", 1)));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 1), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 2999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());

		// do snapshot and save to file
		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
		OperatorSnapshotUtil.writeStateHandle(
			snapshot,
			"src/test/resources/win-op-migration-test-apply-processing-time-flink" + flinkGenerateSavepointVersion + "-snapshot");

		testHarness.close();
	}

	@Test
	public void testRestoreApplyProcessingTimeWindows() throws Exception {
		final int windowSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingProcessingTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector<>(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new RichSumReducer<TimeWindow>()),
				ProcessingTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"win-op-migration-test-apply-processing-time-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		testHarness.setProcessingTime(3020);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3)));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3)));

		testHarness.setProcessingTime(6000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), 5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key3", 1), 5999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());
		testHarness.close();
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeWindowsWithKryoSerializedKeysSnapshot() throws Exception {
		final int windowSize = 3;

		TypeInformation<Tuple2<NonPojoType, Integer>> inputType = new TypeHint<Tuple2<NonPojoType, Integer>>() {}.getTypeInfo();

		ReducingStateDescriptor<Tuple2<NonPojoType, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer<>(),
			inputType.createSerializer(new ExecutionConfig()));

		TypeSerializer<NonPojoType> keySerializer = TypeInformation.of(NonPojoType.class).createSerializer(new ExecutionConfig());
		assertTrue(keySerializer instanceof KryoSerializer);

		WindowOperator<NonPojoType, Tuple2<NonPojoType, Integer>, Tuple2<NonPojoType, Integer>, Tuple2<NonPojoType, Integer>, TimeWindow> operator = new WindowOperator<>(
			TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
			new TimeWindow.Serializer(),
			new TupleKeySelector<>(),
			keySerializer,
			stateDesc,
			new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<>()),
			EventTimeTrigger.create(),
			0,
			null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<NonPojoType, Integer>, Tuple2<NonPojoType, Integer>> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), TypeInformation.of(NonPojoType.class));

		testHarness.setup();
		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(new NonPojoType("key2"), 1), 3999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(new NonPojoType("key2"), 1), 3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>(new NonPojoType("key1"), 1), 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(new NonPojoType("key1"), 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(new NonPojoType("key1"), 1), 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>(new NonPojoType("key2"), 1), 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(new NonPojoType("key2"), 1), 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(new NonPojoType("key2"), 1), 1000));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());

		// do snapshot and save to file
		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
		OperatorSnapshotUtil.writeStateHandle(
			snapshot,
			"src/test/resources/win-op-migration-test-kryo-serialized-key-flink" + flinkGenerateSavepointVersion + "-snapshot");

		testHarness.close();
	}

	@Test
	public void testRestoreKryoSerializedKeysWindows() throws Exception {
		final int windowSize = 3;

		TypeInformation<Tuple2<NonPojoType, Integer>> inputType = new TypeHint<Tuple2<NonPojoType, Integer>>() {}.getTypeInfo();

		ReducingStateDescriptor<Tuple2<NonPojoType, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer<>(),
			inputType.createSerializer(new ExecutionConfig()));

		TypeSerializer<NonPojoType> keySerializer = TypeInformation.of(NonPojoType.class).createSerializer(new ExecutionConfig());
		assertTrue(keySerializer instanceof KryoSerializer);

		WindowOperator<NonPojoType, Tuple2<NonPojoType, Integer>, Tuple2<NonPojoType, Integer>, Tuple2<NonPojoType, Integer>, TimeWindow> operator = new WindowOperator<>(
			TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
			new TimeWindow.Serializer(),
			new TupleKeySelector<>(),
			keySerializer,
			stateDesc,
			new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<>()),
			EventTimeTrigger.create(),
			0,
			null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		OneInputStreamOperatorTestHarness<Tuple2<NonPojoType, Integer>, Tuple2<NonPojoType, Integer>> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector<>(), TypeInformation.of(NonPojoType.class));

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"win-op-migration-test-kryo-serialized-key-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>(new NonPojoType("key1"), 3), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>(new NonPojoType("key2"), 3), 2999));
		expectedOutput.add(new Watermark(2999));

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>(new NonPojoType("key2"), 2), 5999));
		expectedOutput.add(new Watermark(5999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator<>());
		testHarness.close();
	}

	private static class TupleKeySelector<K> implements KeySelector<Tuple2<K, Integer>, K> {
		private static final long serialVersionUID = 1L;

		@Override
		public K getKey(Tuple2<K, Integer> value) throws Exception {
			return value.f0;
		}
	}

	@SuppressWarnings("unchecked")
	private static class Tuple2ResultSortComparator<K extends Comparable> implements Comparator<Object> {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Tuple2<K, Integer>> sr0 = (StreamRecord<Tuple2<K, Integer>>) o1;
				StreamRecord<Tuple2<K, Integer>> sr1 = (StreamRecord<Tuple2<K, Integer>>) o2;
				if (sr0.getTimestamp() != sr1.getTimestamp()) {
					return (int) (sr0.getTimestamp() - sr1.getTimestamp());
				}
				int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
				if (comparison != 0) {
					return comparison;
				} else {
					return sr0.getValue().f1 - sr1.getValue().f1;
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static class Tuple3ResultSortComparator implements Comparator<Object> {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Tuple3<String, Long, Long>> sr0 = (StreamRecord<Tuple3<String, Long, Long>>) o1;
				StreamRecord<Tuple3<String, Long, Long>> sr1 = (StreamRecord<Tuple3<String, Long, Long>>) o2;
				if (sr0.getTimestamp() != sr1.getTimestamp()) {
					return (int) (sr0.getTimestamp() - sr1.getTimestamp());
				}
				int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
				if (comparison != 0) {
					return comparison;
				} else {
					comparison = (int) (sr0.getValue().f1 - sr1.getValue().f1);
					if (comparison != 0) {
						return comparison;
					}
					return (int) (sr0.getValue().f1 - sr1.getValue().f1);
				}
			}
		}
	}

	private static class SumReducer<K> implements ReduceFunction<Tuple2<K, Integer>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<K, Integer> reduce(Tuple2<K, Integer> value1,
				Tuple2<K, Integer> value2) throws Exception {
			return new Tuple2<>(value2.f0, value1.f1 + value2.f1);
		}
	}

	private static class RichSumReducer<W extends Window> extends RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, W> {
		private static final long serialVersionUID = 1L;

		private boolean openCalled = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
		}

		@Override
		public void apply(String key,
				W window,
				Iterable<Tuple2<String, Integer>> input,
				Collector<Tuple2<String, Integer>> out) throws Exception {

			if (!openCalled) {
				fail("Open was not called");
			}
			int sum = 0;

			for (Tuple2<String, Integer> t: input) {
				sum += t.f1;
			}
			out.collect(new Tuple2<>(key, sum));

		}

	}

	private static class SessionWindowFunction implements WindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(String key,
				TimeWindow window,
				Iterable<Tuple2<String, Integer>> values,
				Collector<Tuple3<String, Long, Long>> out) throws Exception {
			int sum = 0;
			for (Tuple2<String, Integer> i: values) {
				sum += i.f1;
			}
			String resultString = key + "-" + sum;
			out.collect(new Tuple3<>(resultString, window.getStart(), window.getEnd()));
		}
	}
}
