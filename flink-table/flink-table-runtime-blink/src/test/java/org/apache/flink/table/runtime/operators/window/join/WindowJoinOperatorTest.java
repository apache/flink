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

package org.apache.flink.table.runtime.operators.window.join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.WindowTestUtils;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * {@link WindowJoinOperator} tests.
 *
 * <p>Add test cases for different window: tumbling, sliding, session
 * and join type: INNER, LEFT, RIGHT, FULL OUTER.
 *
 * <p>Both join inputs use the first field as the equal join key. The join output type should be
 * left_type + right_type + window_attributes(window_start, window_end).
 */
public class WindowJoinOperatorTest<K, W extends Window> {

	// To simplify test, join left and right have the same type.
	private final InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(
			new IntType(),
			new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType());

	// Key selector for both inputs.
	private final BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[] { 0 }, inputType.toRowFieldTypes());
	private final TypeInformation<RowData> keyType = keySelector.getProducedType();

	private final JoinInputSideSpec inputSideSpec1 = JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
			InternalTypeInfo.ofFields(new IntType()),
			keySelector);
	private final JoinInputSideSpec inputSideSpec2 = JoinInputSideSpec.withUniqueKey(
			InternalTypeInfo.ofFields(new VarCharType(VarCharType.MAX_LENGTH)),
			new BinaryRowDataKeySelector(new int[] { 1 }, inputType.toRowFieldTypes()));

	private final InternalTypeInfo<RowData> outputType = InternalTypeInfo.ofFields(
			new IntType(),
			new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(),
			new IntType(),
			new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(),
			new TimestampType(3),
			new TimestampType(3));

	private final RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(
			outputType.toRowFieldTypes(),
			new GenericRowRecordSortComparator(0, new IntType()));

	@Test
	public void testEventTimeSlidingWindows() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.sliding(Duration.ofSeconds(3), Duration.ofSeconds(1))
				.eventTime(2, 2)
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		testHarness.open();

		// process elements
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		// add elements out-of-order
		testHarness.processElement1(insertRecord(1, "key1", 3999L));
		testHarness.processElement1(insertRecord(1, "key1", 3000L));

		testHarness.processElement2(insertRecord(1, "key2", 3999L));
		testHarness.processElement2(insertRecord(1, "key2", 3000L));

		testHarness.processElement1(insertRecord(1, "key1", 20L));
		testHarness.processElement1(insertRecord(1, "key1", 0L));
		testHarness.processElement1(insertRecord(1, "key1", 999L));

		testHarness.processElement2(insertRecord(1, "key2", 20L));
		testHarness.processElement2(insertRecord(1, "key2", 0L));
		testHarness.processElement2(insertRecord(1, "key2", 999L));

		testHarness.processElement1(insertRecord(2, "key1", 1998L));
		testHarness.processElement1(insertRecord(3, "key1", 1999L));
		testHarness.processElement1(insertRecord(4, "key1", 1000L));

		testHarness.processElement2(insertRecord(2, "key2", 1998L));
		testHarness.processElement2(insertRecord(3, "key2", 1999L));
		testHarness.processElement2(insertRecord(4, "key2", 1000L));

		testHarness.processWatermark1(new Watermark(999));
		testHarness.processWatermark2(new Watermark(999));
		expectedOutput.add(insertRecord(1, "key1", 999L, 1, "key2", 999L,
				TimestampData.fromEpochMillis(-2000), TimestampData.fromEpochMillis(1000)));
		expectedOutput.add(new Watermark(999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(1999));
		testHarness.processWatermark2(new Watermark(1999));
		expectedOutput.add(insertRecord(1, "key1", 999L, 1, "key2", 999L,
				TimestampData.fromEpochMillis(-1000), TimestampData.fromEpochMillis(2000)));
		expectedOutput.add(insertRecord(2, "key1", 1998L, 2, "key2", 1998L,
				TimestampData.fromEpochMillis(-1000), TimestampData.fromEpochMillis(2000)));
		expectedOutput.add(insertRecord(3, "key1", 1999L, 3, "key2", 1999L,
				TimestampData.fromEpochMillis(-1000), TimestampData.fromEpochMillis(2000)));
		expectedOutput.add(insertRecord(4, "key1", 1000L, 4, "key2", 1000L,
				TimestampData.fromEpochMillis(-1000), TimestampData.fromEpochMillis(2000)));
		expectedOutput.add(new Watermark(1999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(2999));
		testHarness.processWatermark2(new Watermark(2999));
		expectedOutput.add(insertRecord(1, "key1", 999L, 1, "key2", 999L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(2, "key1", 1998L, 2, "key2", 1998L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(3, "key1", 1999L, 3, "key2", 1999L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(4, "key1", 1000L, 4, "key2", 1000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(new Watermark(2999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark1(new Watermark(3999));
		testHarness.processWatermark2(new Watermark(3999));
		expectedOutput.add(insertRecord(1, "key1", 3000L, 1, "key2", 3000L,
				TimestampData.fromEpochMillis(1000), TimestampData.fromEpochMillis(4000)));
		expectedOutput.add(insertRecord(2, "key1", 1998L, 2, "key2", 1998L,
				TimestampData.fromEpochMillis(1000), TimestampData.fromEpochMillis(4000)));
		expectedOutput.add(insertRecord(3, "key1", 1999L, 3, "key2", 1999L,
				TimestampData.fromEpochMillis(1000), TimestampData.fromEpochMillis(4000)));
		expectedOutput.add(insertRecord(4, "key1", 1000L, 4, "key2", 1000L,
				TimestampData.fromEpochMillis(1000), TimestampData.fromEpochMillis(4000)));
		expectedOutput.add(new Watermark(3999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(4999));
		testHarness.processWatermark2(new Watermark(4999));
		expectedOutput.add(insertRecord(1, "key1", 3000L, 1, "key2", 3000L,
				TimestampData.fromEpochMillis(2000), TimestampData.fromEpochMillis(5000)));
		expectedOutput.add(new Watermark(4999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(5999));
		testHarness.processWatermark2(new Watermark(5999));
		expectedOutput.add(insertRecord(1, "key1", 3000L, 1, "key2", 3000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(new Watermark(5999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// those don't have any effect...
		testHarness.processWatermark1(new Watermark(6999));
		testHarness.processWatermark2(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testProcessingTimeSlidingWindows() throws Throwable {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.sliding(Duration.ofSeconds(3), Duration.ofSeconds(1))
				.processingTime()
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// timestamp is ignored in processing time
		testHarness.setProcessingTime(3);
		testHarness.processElement1(insertRecord(1, "key1", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(1, "key2", Long.MAX_VALUE));

		testHarness.setProcessingTime(1000);

		expectedOutput.add(insertRecord(1, "key1", Long.MAX_VALUE, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(-2000), TimestampData.fromEpochMillis(1000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(2, "key1", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(2, "key2", Long.MAX_VALUE));

		testHarness.setProcessingTime(2000);

		expectedOutput.add(insertRecord(1, "key1", Long.MAX_VALUE, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(-1000), TimestampData.fromEpochMillis(2000)));
		expectedOutput.add(insertRecord(2, "key1", Long.MAX_VALUE, 2, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(-1000), TimestampData.fromEpochMillis(2000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(3, "key1", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(3, "key2", Long.MAX_VALUE));

		testHarness.setProcessingTime(3000);

		expectedOutput.add(insertRecord(1, "key1", Long.MAX_VALUE, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(2, "key1", Long.MAX_VALUE, 2, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(3, "key1", Long.MAX_VALUE, 3, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(4, "key1", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(4, "key2", Long.MAX_VALUE));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(insertRecord(2, "key1", Long.MAX_VALUE, 2, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(1000), TimestampData.fromEpochMillis(4000)));
		expectedOutput.add(insertRecord(3, "key1", Long.MAX_VALUE, 3, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(1000), TimestampData.fromEpochMillis(4000)));
		expectedOutput.add(insertRecord(4, "key1", Long.MAX_VALUE, 4, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(1000), TimestampData.fromEpochMillis(4000)));
		expectedOutput.add(insertRecord(3, "key1", Long.MAX_VALUE, 3, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(2000), TimestampData.fromEpochMillis(5000)));
		expectedOutput.add(insertRecord(4, "key1", Long.MAX_VALUE, 4, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(2000), TimestampData.fromEpochMillis(5000)));
		expectedOutput.add(insertRecord(4, "key1", Long.MAX_VALUE, 4, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testEventTimeTumblingWindows() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.tumble(Duration.ofSeconds(3))
				.eventTime(2, 2)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement1(insertRecord(1, "key1", 3999L));
		testHarness.processElement1(insertRecord(1, "key1", 3000L));

		testHarness.processElement2(insertRecord(1, "key2", 3999L));
		testHarness.processElement2(insertRecord(1, "key2", 3000L));

		testHarness.processElement1(insertRecord(2, "key1", 20L));
		testHarness.processElement1(insertRecord(3, "key1", 0L));
		testHarness.processElement1(insertRecord(4, "key1", 999L));

		testHarness.processElement2(insertRecord(2, "key2", 20L));
		testHarness.processElement2(insertRecord(3, "key2", 0L));
		testHarness.processElement2(insertRecord(4, "key2", 999L));

		testHarness.processElement1(insertRecord(5, "key1", 1998L));
		testHarness.processElement1(insertRecord(6, "key1", 1999L));
		testHarness.processElement1(insertRecord(7, "key1", 1000L));

		testHarness.processElement2(insertRecord(5, "key2", 1998L));
		testHarness.processElement2(insertRecord(6, "key2", 1999L));
		testHarness.processElement2(insertRecord(7, "key2", 1000L));

		testHarness.processWatermark1(new Watermark(999));
		testHarness.processWatermark2(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(1999));
		testHarness.processWatermark2(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark1(new Watermark(2999));
		testHarness.processWatermark2(new Watermark(2999));
		expectedOutput.add(insertRecord(2, "key1", 20L, 2, "key2", 20L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(3, "key1", 0L, 3, "key2", 0L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(4, "key1", 999L, 4, "key2", 999L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(5, "key1", 1998L, 5, "key2", 1998L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(6, "key1", 1999L, 6, "key2", 1999L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(7, "key1", 1000L, 7, "key2", 1000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(new Watermark(2999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(3999));
		testHarness.processWatermark2(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(4999));
		testHarness.processWatermark2(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(5999));
		testHarness.processWatermark2(new Watermark(5999));
		expectedOutput.add(insertRecord(1, "key1", 3000L, 1, "key2", 3000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(new Watermark(5999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// those don't have any effect...
		testHarness.processWatermark1(new Watermark(6999));
		testHarness.processWatermark2(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testProcessingTimeTumblingWindows() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.tumble(Duration.ofSeconds(3))
				.processingTime()
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement1(insertRecord(1, "key1", Long.MAX_VALUE));
		testHarness.processElement1(insertRecord(2, "key1", 7000L));
		testHarness.processElement1(insertRecord(3, "key1", 7000L));

		testHarness.processElement2(insertRecord(1, "key2", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(2, "key2", 7000L));
		testHarness.processElement2(insertRecord(3, "key2", 7000L));

		testHarness.processElement1(insertRecord(4, "key1", 7000L));
		testHarness.processElement1(insertRecord(5, "key1", 7000L));

		testHarness.processElement2(insertRecord(4, "key2", 7000L));
		testHarness.processElement2(insertRecord(5, "key2", 7000L));

		testHarness.setProcessingTime(5000);
		expectedOutput.add(insertRecord(1, "key1", Long.MAX_VALUE, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(2, "key1", 7000L, 2, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(3, "key1", 7000L, 3, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(4, "key1", 7000L, 4, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(5, "key1", 7000L, 5, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(6, "key1", 7000L));
		testHarness.processElement1(insertRecord(7, "key1", 7000L));
		testHarness.processElement1(insertRecord(8, "key1", 7000L));

		testHarness.processElement2(insertRecord(6, "key2", 7000L));
		testHarness.processElement2(insertRecord(7, "key2", 7000L));
		testHarness.processElement2(insertRecord(8, "key2", 7000L));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(insertRecord(6, "key1", 7000L, 6, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(7, "key1", 7000L, 7, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(8, "key1", 7000L, 8, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));

		assertThat(operator.getWatermarkLatency().getValue(), is(0L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testEventTimeSessionWindows() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				// Join input has no unique key, so there is no override.
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.session(Duration.ofSeconds(3))
				.eventTime(2, 2)
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement1(insertRecord(1, "key1", 0L));
		testHarness.processElement1(insertRecord(1, "key1", 1000L));
		testHarness.processElement1(insertRecord(1, "key1", 2500L));

		testHarness.processElement1(insertRecord(2, "key1", 10L));
		testHarness.processElement1(insertRecord(2, "key1", 1000L));

		testHarness.processElement2(insertRecord(1, "key2", 0L));
		testHarness.processElement2(insertRecord(1, "key2", 1000L));
		testHarness.processElement2(insertRecord(1, "key2", 2500L));

		testHarness.processElement2(insertRecord(2, "key2", 10L));
		testHarness.processElement2(insertRecord(2, "key2", 1000L));

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshotV2 = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshotV2);
		testHarness.open();

		assertThat(operator.getWatermarkLatency().getValue(), is(0L));

		testHarness.processElement1(insertRecord(2, "key1", 2500L));
		testHarness.processElement1(insertRecord(3, "key1", 5501L));
		testHarness.processElement1(insertRecord(3, "key1", 6000L));
		testHarness.processElement1(insertRecord(3, "key1", 6000L));
		testHarness.processElement1(insertRecord(3, "key1", 6050L));

		testHarness.processElement2(insertRecord(2, "key2", 2500L));
		testHarness.processElement2(insertRecord(3, "key2", 5501L));
		testHarness.processElement2(insertRecord(3, "key2", 6000L));
		testHarness.processElement2(insertRecord(3, "key2", 6000L));
		testHarness.processElement2(insertRecord(3, "key2", 6050L));

		testHarness.processWatermark1(new Watermark(12000));
		testHarness.processWatermark2(new Watermark(12000));

		expectedOutput.add(insertRecord(1, "key1", 2500L, 1, "key2", 2500L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(5500)));
		expectedOutput.add(insertRecord(2, "key1", 2500L, 2, "key2", 2500L,
				TimestampData.fromEpochMillis(10), TimestampData.fromEpochMillis(5500)));
		expectedOutput.add(insertRecord(3, "key1", 6050L, 3, "key2", 6050L,
				TimestampData.fromEpochMillis(5501), TimestampData.fromEpochMillis(9050)));

		expectedOutput.add(new Watermark(12000));

		// add a late data
		testHarness.processElement1(insertRecord(4, "key1", 4000L));
		testHarness.processElement1(insertRecord(5, "key1", 15000L));
		testHarness.processElement1(insertRecord(5, "key1", 15000L));

		testHarness.processElement2(insertRecord(4, "key2", 4000L));
		testHarness.processElement2(insertRecord(5, "key2", 15000L));
		testHarness.processElement2(insertRecord(5, "key2", 15000L));

		testHarness.processWatermark1(new Watermark(17999));
		testHarness.processWatermark2(new Watermark(17999));

		expectedOutput.add(insertRecord(5, "key1", 15000L, 5, "key2", 15000L,
				TimestampData.fromEpochMillis(15000L), TimestampData.fromEpochMillis(18000L)));
		expectedOutput.add(new Watermark(17999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(18000);
		assertThat(operator.getWatermarkLatency().getValue(), is(1L));

		testHarness.close();
	}

	@Test
	public void testProcessingTimeSessionWindows() throws Throwable {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.session(Duration.ofSeconds(3))
				.processingTime()
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(
			outputType.toRowFieldTypes(),
			new GenericRowRecordSortComparator(0, new IntType()));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// timestamp is ignored in processing time
		testHarness.setProcessingTime(3);
		testHarness.processElement1(insertRecord(1, "key1", 1L));
		testHarness.processElement2(insertRecord(1, "key2", 1L));

		testHarness.setProcessingTime(1000);
		testHarness.processElement1(insertRecord(1, "key1", 1002L));
		testHarness.processElement2(insertRecord(1, "key2", 1002L));

		testHarness.setProcessingTime(5000);

		expectedOutput.add(insertRecord(1, "key1", 1002L, 1, "key2", 1002L,
				TimestampData.fromEpochMillis(3L), TimestampData.fromEpochMillis(4000L)));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(1, "key1", 5000L));
		testHarness.processElement1(insertRecord(1, "key1", 5000L));
		testHarness.processElement2(insertRecord(1, "key2", 5000L));
		testHarness.processElement2(insertRecord(1, "key2", 5000L));

		testHarness.setProcessingTime(10000);

		expectedOutput.add(insertRecord(1, "key1", 5000L, 1, "key2", 5000L,
				TimestampData.fromEpochMillis(5000L), TimestampData.fromEpochMillis(8000L)));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	/**
	 * This tests a custom Session window assigner that assigns some elements to "point windows",
	 * windows that have the same timestamp for start and end.
	 *
	 * <p>In this test, elements that have 33 as the record key will be put into a point
	 * window.
	 */
	@Test
	public void testPointSessions() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.assigner(WindowTestUtils.PointSessionWindowAssigner.of(3000, 0, 33))
				.eventTime(2, 2)
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement1(insertRecord(1, "key1", 0L));
		testHarness.processElement1(insertRecord(33, "key1", 1000L));

		testHarness.processElement2(insertRecord(1, "key2", 0L));
		testHarness.processElement2(insertRecord(33, "key2", 1000L));

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processElement1(insertRecord(1, "key1", 10L));
		testHarness.processElement1(insertRecord(2, "key1", 1000L));
		testHarness.processElement1(insertRecord(33, "key1", 2500L));

		testHarness.processElement2(insertRecord(1, "key2", 10L));
		testHarness.processElement2(insertRecord(2, "key2", 1000L));
		testHarness.processElement2(insertRecord(33, "key2", 2500L));

		testHarness.processWatermark1(new Watermark(12000));
		testHarness.processWatermark2(new Watermark(12000));

		expectedOutput.add(insertRecord(1, "key1", 10L, 1, "key2", 10L,
				TimestampData.fromEpochMillis(0L), TimestampData.fromEpochMillis(3010L)));
		expectedOutput.add(insertRecord(2, "key1", 1000L, 2, "key2", 1000L,
				TimestampData.fromEpochMillis(1000L), TimestampData.fromEpochMillis(4000L)));
		expectedOutput.add(insertRecord(33, "key1", 1000L, 33, "key2", 1000L,
				TimestampData.fromEpochMillis(1000L), TimestampData.fromEpochMillis(1000L)));
		expectedOutput.add(insertRecord(33, "key1", 2500L, 33, "key2", 2500L,
				TimestampData.fromEpochMillis(2500L), TimestampData.fromEpochMillis(2500L)));
		expectedOutput.add(new Watermark(12000));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testLateness() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.tumble(Duration.ofSeconds(2))
				.eventTime(2, 2)
				.allowedLateness(Duration.ofMillis(500))
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement1(insertRecord(1, "key1", 500L));
		testHarness.processElement2(insertRecord(1, "key2", 500L));

		testHarness.processWatermark1(new Watermark(1500));
		testHarness.processWatermark2(new Watermark(1500));

		expectedOutput.add(new Watermark(1500));

		testHarness.processElement1(insertRecord(1, "key1", 1300L));
		testHarness.processElement2(insertRecord(1, "key2", 1300L));
		testHarness.processWatermark1(new Watermark(2300));
		testHarness.processWatermark2(new Watermark(2300));

		expectedOutput.add(insertRecord(1, "key1", 1300L, 1, "key2", 1300L,
				TimestampData.fromEpochMillis(0L), TimestampData.fromEpochMillis(2000L)));
		expectedOutput.add(new Watermark(2300));

		// this will not be dropped because window.maxTimestamp() + allowedLateness > currentWatermark
		testHarness.processElement1(insertRecord(1, "key1", 1997L));
		testHarness.processElement2(insertRecord(1, "key2", 1997L));
		testHarness.processWatermark1(new Watermark(6000));
		testHarness.processWatermark2(new Watermark(6000));

		// element1 and element2 would trigger the join 2 times.
		expectedOutput.add(insertRecord(1, "key1", 1997L, 1, "key2", 1300L,
				TimestampData.fromEpochMillis(0L), TimestampData.fromEpochMillis(2000L)));
		expectedOutput.add(insertRecord(1, "key1", 1997L, 1, "key2", 1997L,
				TimestampData.fromEpochMillis(0L), TimestampData.fromEpochMillis(2000L)));
		expectedOutput.add(new Watermark(6000));

		// this will be dropped because window.maxTimestamp() + allowedLateness < currentWatermark
		testHarness.processElement1(insertRecord(1, "key1", 1998L));
		testHarness.processElement2(insertRecord(1, "key2", 1998L));
		testHarness.processWatermark1(new Watermark(7000));
		testHarness.processWatermark2(new Watermark(7000));

		expectedOutput.add(new Watermark(7000));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		assertThat(operator.getNumLateRecordsDropped().getCount(), is(2L));

		testHarness.close();
	}

	/**
	 * 	See {@code WindowJoinOperator.cleanupTime} for how the cleaning time is wrapped-around.
	 */
	@Test
	public void testCleanupTimeOverflow() throws Exception {
		long windowSize = 1000;
		long lateness = 2000;
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.tumble(Duration.ofMillis(windowSize))
				.eventTime(2, 2)
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.allowedLateness(Duration.ofMillis(lateness))
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
				createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		WindowAssigner<TimeWindow> windowAssigner = TumblingWindowAssigner.of(Duration.ofMillis(windowSize));
		long timestamp = Long.MAX_VALUE - 1750;
		Collection<TimeWindow> windows = windowAssigner.assignWindows(GenericRowData.of(1, fromString("key2")), timestamp);
		TimeWindow window = windows.iterator().next();

		// window interval [Long.MAX_VALUE - 1807, Long.MAX_VALUE - 0807)
		// the garbage collection timer would wrap-around
		assertTrue(window.maxTimestamp() + lateness < window.maxTimestamp());
		// and it would prematurely fire with watermark (Long.MAX_VALUE - 1500)
		assertTrue(window.maxTimestamp() + lateness < Long.MAX_VALUE - 1500);

		testHarness.processElement1(insertRecord(1, "key1", timestamp));
		testHarness.processElement2(insertRecord(1, "key2", timestamp));

		// if we don't correctly prevent wrap-around in the garbage collection
		// timers, this watermark will clean our window state for the just-added
		// element/window
		testHarness.processWatermark1(new Watermark(Long.MAX_VALUE - 1500));
		testHarness.processWatermark2(new Watermark(Long.MAX_VALUE - 1500));

		// this watermark is before the end timestamp of our only window
		assertTrue(Long.MAX_VALUE - 1500 < window.maxTimestamp());
		assertTrue(window.maxTimestamp() < Long.MAX_VALUE);

		// push in a watermark that will trigger computation of our window
		testHarness.processWatermark1(new Watermark(window.maxTimestamp()));
		testHarness.processWatermark2(new Watermark(window.maxTimestamp()));

		expected.add(new Watermark(Long.MAX_VALUE - 1500));
		expected.add(insertRecord(1, "key1", timestamp, 1, "key2", timestamp,
				TimestampData.fromEpochMillis(window.getStart()), TimestampData.fromEpochMillis(window.getEnd())));
		expected.add(new Watermark(window.maxTimestamp()));

		assertor.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyInputStateForTumblingWindows() throws Exception {
		final int windowSize = 2;
		final long lateness = 1;

		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.tumble(Duration.ofSeconds(windowSize))
				.eventTime(2, 2)
				.allowedLateness(Duration.ofMillis(lateness))
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		// normal element
		testHarness.processElement1(insertRecord(1, "key1", 1000L));
		testHarness.processElement2(insertRecord(1, "key2", 1000L));
		testHarness.processWatermark1(new Watermark(1599));
		testHarness.processWatermark2(new Watermark(1599));
		testHarness.processWatermark1(new Watermark(1999));
		testHarness.processWatermark2(new Watermark(1999));
		testHarness.processWatermark1(new Watermark(2000));
		testHarness.processWatermark2(new Watermark(2000));
		testHarness.processWatermark1(new Watermark(5000));
		testHarness.processWatermark2(new Watermark(5000));

		expected.add(new Watermark(1599));
		expected.add(insertRecord(1, "key1", 1000L, 1, "key2", 1000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(2000)));
		expected.add(new Watermark(1999)); // here it fires and purges
		expected.add(new Watermark(2000)); // here is the cleanup timer
		expected.add(new Watermark(5000));

		assertor.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput());
		testHarness.close();
	}

	// -------------------------------------------------------------------------
	//  Test other join types: LEFT, RIGHT, FULL OUTER
	//  using the processing time tumbling window. Different type of windows
	//  only differ on how the window are assigned and triggered, here
	//  we want to test the join behavior within one triggered window.
	// -------------------------------------------------------------------------

	@Test
	public void testLeftJoinProcessingTimeTumbling() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.LEFT)
				.tumble(Duration.ofSeconds(3))
				.processingTime()
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement1(insertRecord(1, "key1", Long.MAX_VALUE));
		testHarness.processElement1(insertRecord(2, "key1", 7000L));
		testHarness.processElement1(insertRecord(3, "key1", 7000L));

		testHarness.processElement2(insertRecord(1, "key2", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(2, "key2", 7000L));

		testHarness.processElement1(insertRecord(4, "key1", 7000L));
		testHarness.processElement1(insertRecord(5, "key1", 7000L));

		testHarness.processElement2(insertRecord(4, "key2", 7000L));

		testHarness.setProcessingTime(5000);
		expectedOutput.add(insertRecord(1, "key1", Long.MAX_VALUE, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(2, "key1", 7000L, 2, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(3, "key1", 7000L, null, null, null,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(4, "key1", 7000L, 4, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(5, "key1", 7000L, null, null, null,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(6, "key1", 7000L));
		testHarness.processElement1(insertRecord(7, "key1", 7000L));
		testHarness.processElement1(insertRecord(8, "key1", 7000L));

		testHarness.processElement2(insertRecord(6, "key2", 7000L));
		testHarness.processElement2(insertRecord(8, "key2", 7000L));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(insertRecord(6, "key1", 7000L, 6, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(7, "key1", 7000L, null, null, null,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(8, "key1", 7000L, 8, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));

		assertThat(operator.getWatermarkLatency().getValue(), is(0L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testRightJoinProcessingTimeTumbling() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.RIGHT)
				.tumble(Duration.ofSeconds(3))
				.processingTime()
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(
				outputType.toRowFieldTypes(),
				new GenericRowRecordSortComparator(3, new IntType()));

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement1(insertRecord(1, "key1", Long.MAX_VALUE));
		testHarness.processElement1(insertRecord(2, "key1", 7000L));

		testHarness.processElement2(insertRecord(1, "key2", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(2, "key2", 7000L));
		testHarness.processElement2(insertRecord(3, "key2", 7000L));

		testHarness.processElement1(insertRecord(4, "key1", 7000L));

		testHarness.processElement2(insertRecord(4, "key2", 7000L));
		testHarness.processElement2(insertRecord(5, "key2", 7000L));

		testHarness.setProcessingTime(5000);
		expectedOutput.add(insertRecord(1, "key1", Long.MAX_VALUE, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(2, "key1", 7000L, 2, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(null, null, null, 3, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(4, "key1", 7000L, 4, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(null, null, null, 5, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(6, "key1", 7000L));
		testHarness.processElement1(insertRecord(8, "key1", 7000L));

		testHarness.processElement2(insertRecord(6, "key2", 7000L));
		testHarness.processElement2(insertRecord(7, "key2", 7000L));
		testHarness.processElement2(insertRecord(8, "key2", 7000L));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(insertRecord(6, "key1", 7000L, 6, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(null, null, null, 7, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(8, "key1", 7000L, 8, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));

		assertThat(operator.getWatermarkLatency().getValue(), is(0L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testFullOuterJoinProcessingTimeTumbling() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.FULL)
				.tumble(Duration.ofSeconds(3))
				.processingTime()
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		final Comparator<GenericRowData> comparator = (row1, row2) -> {
			RowKind kind1 = row1.getRowKind();
			RowKind kind2 = row2.getRowKind();
			if (kind1 != kind2) {
				return kind1.toByteValue() - kind2.toByteValue();
			} else {
				Object key1 = row1.isNullAt(0)
						? RowData.get(row1, 3, new IntType())
						: RowData.get(row1, 0, new IntType());
				Object key2 = row2.isNullAt(0)
						? RowData.get(row2, 3, new IntType())
						: RowData.get(row2, 0, new IntType());
				if (key1 instanceof Comparable && key2 instanceof Comparable) {
					return ((Comparable) key1).compareTo(key2);
				} else {
					throw new UnsupportedOperationException();
				}
			}
		};

		RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(
				outputType.toRowFieldTypes(),
				comparator);

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement1(insertRecord(1, "key1", Long.MAX_VALUE));
		testHarness.processElement1(insertRecord(2, "key1", 7000L));
		testHarness.processElement1(insertRecord(3, "key1", 7000L));

		testHarness.processElement2(insertRecord(1, "key2", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(2, "key2", 7000L));

		testHarness.processElement1(insertRecord(4, "key1", 7000L));
		testHarness.processElement1(insertRecord(5, "key1", 7000L));

		testHarness.processElement2(insertRecord(4, "key2", 7000L));

		testHarness.setProcessingTime(5000);
		expectedOutput.add(insertRecord(1, "key1", Long.MAX_VALUE, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(2, "key1", 7000L, 2, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(3, "key1", 7000L, null, null, null,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(4, "key1", 7000L, 4, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(5, "key1", 7000L, null, null, null,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(6, "key1", 7000L));
		testHarness.processElement1(insertRecord(8, "key1", 7000L));

		testHarness.processElement2(insertRecord(6, "key2", 7000L));
		testHarness.processElement2(insertRecord(7, "key2", 7000L));
		testHarness.processElement2(insertRecord(8, "key2", 7000L));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(insertRecord(6, "key1", 7000L, 6, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(null, null, null, 7, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(8, "key1", 7000L, 8, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));

		assertThat(operator.getWatermarkLatency().getValue(), is(0L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	/**
	 * Test processing time tumbling window without any unique keys for both inputs.
	 * Without unique keys, same value join key records of one input side can appear
	 * multiple times.
	 */
	@Test
	public void testProcessingTimeTumblingWithoutUniqueKeys() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(JoinInputSideSpec.withoutUniqueKey(), JoinInputSideSpec.withoutUniqueKey())
				.joinType(FlinkJoinType.INNER)
				.tumble(Duration.ofSeconds(3))
				.processingTime()
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement1(insertRecord(1, "key1", Long.MAX_VALUE));
		testHarness.processElement1(insertRecord(1, "key1", 7000L));
		testHarness.processElement1(insertRecord(3, "key1", 7000L));

		testHarness.processElement2(insertRecord(1, "key2", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(2, "key2", 7000L));

		testHarness.setProcessingTime(5000);
		expectedOutput.add(insertRecord(1, "key1", 7000L, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		expectedOutput.add(insertRecord(1, "key1", Long.MAX_VALUE, 1, "key2", Long.MAX_VALUE,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(6, "key1", 7000L));
		testHarness.processElement1(insertRecord(7, "key1", 7000L));
		testHarness.processElement1(insertRecord(7, "key1", 7000L));

		testHarness.processElement2(insertRecord(6, "key2", 7000L));
		testHarness.processElement2(insertRecord(7, "key2", 7000L));
		testHarness.processElement2(insertRecord(8, "key2", 7000L));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(insertRecord(6, "key1", 7000L, 6, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(7, "key1", 7000L, 7, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(7, "key1", 7000L, 7, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));

		assertThat(operator.getWatermarkLatency().getValue(), is(0L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	/**
	 * Test processing time tumbling window with UPDATE and DELETE messages in the inputs.
	 * The UPDATE_BEFORE message expect to be ignored, the DELETE message expect to retract the
	 * message within the window.
	 */
	@Test
	public void testProcessingTimeTumblingWithUpdatingMessages() throws Exception {
		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(JoinInputSideSpec.withoutUniqueKey(), JoinInputSideSpec.withoutUniqueKey())
				.joinType(FlinkJoinType.INNER)
				.tumble(Duration.ofSeconds(3))
				.processingTime()
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement1(insertRecord(1, "key1", Long.MAX_VALUE));
		// update_before message should be ignored
		testHarness.processElement1(updateBeforeRecord(1, "key1", 7000L));
		// delete message retract the first message (by the same record)
		testHarness.processElement1(deleteRecord(1, "key1", Long.MAX_VALUE));
		testHarness.processElement1(insertRecord(2, "key1", 7000L));

		testHarness.processElement2(insertRecord(1, "key2", Long.MAX_VALUE));
		testHarness.processElement2(insertRecord(2, "key2", 7000L));

		testHarness.setProcessingTime(5000);
		expectedOutput.add(insertRecord(2, "key1", 7000L, 2, "key2", 7000L,
				TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(3000)));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement1(insertRecord(6, "key1", 7000L));
		testHarness.processElement1(insertRecord(7, "key1", 7000L));
		testHarness.processElement1(insertRecord(7, "key1", 7000L));

		testHarness.processElement2(insertRecord(6, "key2", 7000L));
		// update_after message behaviors same with insert message
		testHarness.processElement2(updateAfterRecord(7, "key2", 7000L));
		testHarness.processElement2(insertRecord(8, "key2", 7000L));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(insertRecord(6, "key1", 7000L, 6, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(7, "key1", 7000L, 7, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));
		expectedOutput.add(insertRecord(7, "key1", 7000L, 7, "key2", 7000L,
				TimestampData.fromEpochMillis(3000), TimestampData.fromEpochMillis(6000)));

		assertThat(operator.getWatermarkLatency().getValue(), is(0L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testEventTimeWithInputWindowAttributes() throws Exception {
		final InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(
				new IntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new BigIntType(),
				new BigIntType());

		// Key selector for both inputs.
		final BinaryRowDataKeySelector keySelector =
				new BinaryRowDataKeySelector(new int[] { 0 }, inputType.toRowFieldTypes());

		final JoinInputSideSpec inputSideSpec1 = JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
				InternalTypeInfo.ofFields(new IntType()),
				keySelector);
		final JoinInputSideSpec inputSideSpec2 = JoinInputSideSpec.withUniqueKey(
				InternalTypeInfo.ofFields(new VarCharType(VarCharType.MAX_LENGTH)),
				new BinaryRowDataKeySelector(new int[] { 1 }, inputType.toRowFieldTypes()));

		final InternalTypeInfo<RowData> outputType = InternalTypeInfo.ofFields(
				new IntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new BigIntType(),
				new BigIntType(),
				new IntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new BigIntType(),
				new BigIntType());

		final RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(
				outputType.toRowFieldTypes(),
				new GenericRowRecordSortComparator(0, new IntType()));

		WindowJoinOperatorBase<K, W> operator = WindowJoinOperatorSimple
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.windowAttributeRefs(new int[] {2, 3}, new int[] {2, 3})
				.eventTime()
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		testHarness.open();

		// process elements
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		// add elements out-of-order
		testHarness.processElement1(insertRecord(1, "key1", -1000L, 2000L));
		testHarness.processElement1(insertRecord(1, "key1", -1000L, 2000L));

		testHarness.processElement2(insertRecord(1, "key2", -1000L, 2000L));
		testHarness.processElement2(insertRecord(1, "key2", -1000L, 2000L));

		testHarness.processElement1(insertRecord(1, "key1", 1000L, 4000L));
		testHarness.processElement1(insertRecord(1, "key1", 1000L, 4000L));
		testHarness.processElement1(insertRecord(1, "key1", 1000L, 4000L));

		testHarness.processElement2(insertRecord(1, "key2", 1000L, 4000L));
		testHarness.processElement2(insertRecord(1, "key2", 1000L, 4000L));
		testHarness.processElement2(insertRecord(1, "key2", 1000L, 4000L));

		testHarness.processElement1(insertRecord(2, "key1", 3000L, 6000L));
		testHarness.processElement1(insertRecord(3, "key1", 3000L, 6000L));
		testHarness.processElement1(insertRecord(4, "key1", 3000L, 6000L));

		testHarness.processElement2(insertRecord(2, "key2", 3000L, 6000L));
		testHarness.processElement2(insertRecord(3, "key2", 3000L, 6000L));
		testHarness.processElement2(insertRecord(4, "key2", 3000L, 6000L));

		testHarness.processWatermark1(new Watermark(999));
		testHarness.processWatermark2(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(1999));
		testHarness.processWatermark2(new Watermark(1999));
		expectedOutput.add(insertRecord(1, "key1", -1000L, 2000L, 1, "key2", -1000L, 2000L));
		expectedOutput.add(new Watermark(1999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(3999));
		testHarness.processWatermark2(new Watermark(3999));
		expectedOutput.add(insertRecord(1, "key1", 1000L, 4000L, 1, "key2", 1000L, 4000L));
		expectedOutput.add(new Watermark(3999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark1(new Watermark(4999));
		testHarness.processWatermark2(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark1(new Watermark(5999));
		testHarness.processWatermark2(new Watermark(5999));
		expectedOutput.add(insertRecord(2, "key1", 3000L, 6000L, 2, "key2", 3000L, 6000L));
		expectedOutput.add(insertRecord(3, "key1", 3000L, 6000L, 3, "key2", 3000L, 6000L));
		expectedOutput.add(insertRecord(4, "key1", 3000L, 6000L, 4, "key2", 3000L, 6000L));
		expectedOutput.add(new Watermark(5999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// those don't have any effect...
		testHarness.processWatermark1(new Watermark(6999));
		testHarness.processWatermark2(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testProcessingTimeWithInputWindowAttributes() throws Throwable {
		final InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(
				new IntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new BigIntType(),
				new BigIntType());

		// Key selector for both inputs.
		final BinaryRowDataKeySelector keySelector =
				new BinaryRowDataKeySelector(new int[] { 0 }, inputType.toRowFieldTypes());

		final JoinInputSideSpec inputSideSpec1 = JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
				InternalTypeInfo.ofFields(new IntType()),
				keySelector);
		final JoinInputSideSpec inputSideSpec2 = JoinInputSideSpec.withUniqueKey(
				InternalTypeInfo.ofFields(new VarCharType(VarCharType.MAX_LENGTH)),
				new BinaryRowDataKeySelector(new int[] { 1 }, inputType.toRowFieldTypes()));

		final InternalTypeInfo<RowData> outputType = InternalTypeInfo.ofFields(
				new IntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new BigIntType(),
				new BigIntType(),
				new IntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new BigIntType(),
				new BigIntType());

		final RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(
				outputType.toRowFieldTypes(),
				new GenericRowRecordSortComparator(0, new IntType()));

		WindowJoinOperatorBase<K, W> operator = WindowJoinOperatorSimple
				.builder()
				.inputType(inputType, inputType)
				.joinInputSpec(inputSideSpec1, inputSideSpec2)
				.joinType(FlinkJoinType.INNER)
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.filterNullKeys(true)
				.windowAttributeRefs(new int[] {2, 3}, new int[] {2, 3})
				.processingTime()
				.build();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = createTestHarness(operator);

		testHarness.open();

		// process elements
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		// add elements out-of-order
		testHarness.processElement1(insertRecord(1, "key1", -1000L, 2000L));
		testHarness.processElement1(insertRecord(1, "key1", -1000L, 2000L));

		testHarness.processElement2(insertRecord(1, "key2", -1000L, 2000L));
		testHarness.processElement2(insertRecord(1, "key2", -1000L, 2000L));

		testHarness.processElement1(insertRecord(1, "key1", 1000L, 4000L));
		testHarness.processElement1(insertRecord(1, "key1", 1000L, 4000L));
		testHarness.processElement1(insertRecord(1, "key1", 1000L, 4000L));

		testHarness.processElement2(insertRecord(1, "key2", 1000L, 4000L));
		testHarness.processElement2(insertRecord(1, "key2", 1000L, 4000L));
		testHarness.processElement2(insertRecord(1, "key2", 1000L, 4000L));

		testHarness.processElement1(insertRecord(2, "key1", 3000L, 6000L));
		testHarness.processElement1(insertRecord(3, "key1", 3000L, 6000L));
		testHarness.processElement1(insertRecord(4, "key1", 3000L, 6000L));

		testHarness.processElement2(insertRecord(2, "key2", 3000L, 6000L));
		testHarness.processElement2(insertRecord(3, "key2", 3000L, 6000L));
		testHarness.processElement2(insertRecord(4, "key2", 3000L, 6000L));

		testHarness.processWatermark1(new Watermark(999));
		testHarness.processWatermark2(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(2000L);

		testHarness.processWatermark1(new Watermark(1999));
		testHarness.processWatermark2(new Watermark(1999));
		expectedOutput.add(insertRecord(1, "key1", -1000L, 2000L, 1, "key2", -1000L, 2000L));
		expectedOutput.add(new Watermark(1999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(4000L);

		testHarness.processWatermark1(new Watermark(3999));
		testHarness.processWatermark2(new Watermark(3999));
		expectedOutput.add(insertRecord(1, "key1", 1000L, 4000L, 1, "key2", 1000L, 4000L));
		expectedOutput.add(new Watermark(3999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(5000L);

		testHarness.processWatermark1(new Watermark(4999));
		testHarness.processWatermark2(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(6000L);

		testHarness.processWatermark1(new Watermark(5999));
		testHarness.processWatermark2(new Watermark(5999));
		expectedOutput.add(insertRecord(2, "key1", 3000L, 6000L, 2, "key2", 3000L, 6000L));
		expectedOutput.add(insertRecord(3, "key1", 3000L, 6000L, 3, "key2", 3000L, 6000L));
		expectedOutput.add(insertRecord(4, "key1", 3000L, 6000L, 4, "key2", 3000L, 6000L));
		expectedOutput.add(new Watermark(5999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// those don't have any effect...
		testHarness.processWatermark1(new Watermark(6999));
		testHarness.processWatermark2(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	// -------------------------------------------------------------------------
	//  Utilities
	// -------------------------------------------------------------------------

	private TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> createTestHarness(
			WindowJoinOperatorBase<K, W> operator)
			throws Exception {
		return new KeyedTwoInputStreamOperatorTestHarness<>(operator,
				keySelector, keySelector, keyType);
	}

}
