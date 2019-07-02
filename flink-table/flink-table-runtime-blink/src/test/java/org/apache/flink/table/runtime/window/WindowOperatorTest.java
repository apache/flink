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

package org.apache.flink.table.runtime.window;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.window.CountWindow;
import org.apache.flink.table.api.window.TimeWindow;
import org.apache.flink.table.api.window.Window;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.dataview.StateDataViewStore;
import org.apache.flink.table.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.runtime.util.BinaryRowKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.window.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.window.triggers.ElementTriggers;
import org.apache.flink.table.runtime.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.window.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.retractRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link WindowOperator}.
 */
public class WindowOperatorTest {

	// For counting if close() is called the correct number of times on the SumReducer
	private static AtomicInteger closeCalled = new AtomicInteger(0);

	private LogicalType[] inputFieldTypes = new LogicalType[]{
			new VarCharType(VarCharType.MAX_LENGTH),
			new IntType(),
			new BigIntType()};

	private BaseRowTypeInfo outputType = new BaseRowTypeInfo(
			new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType());

	private LogicalType[] aggResultTypes = new LogicalType[] { new BigIntType(), new BigIntType() };
	private LogicalType[] accTypes = new LogicalType[] { new BigIntType(), new BigIntType() };
	private LogicalType[] windowTypes = new LogicalType[] { new BigIntType(), new BigIntType(), new BigIntType() };
	private GenericRowEqualiser equaliser = new GenericRowEqualiser(accTypes, windowTypes);
	private BinaryRowKeySelector keySelector = new BinaryRowKeySelector(new int[] { 0 }, inputFieldTypes);
	private TypeInformation<BaseRow> keyType = keySelector.getProducedType();
	private BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(
			outputType.getFieldTypes(),
			new GenericRowRecordSortComparator(0, new VarCharType(VarCharType.MAX_LENGTH)));

	@Test
	public void testEventTimeSlidingWindows() throws Exception {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.sliding(Duration.ofSeconds(3), Duration.ofSeconds(1), 0)
				.withEventTime(2)
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		testHarness.open();

		// process elements
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		// add elements out-of-order
		testHarness.processElement(record("key2", 1, 3999L));
		testHarness.processElement(record("key2", 1, 3000L));

		testHarness.processElement(record("key1", 1, 20L));
		testHarness.processElement(record("key1", 1, 0L));
		testHarness.processElement(record("key1", 1, 999L));

		testHarness.processElement(record("key2", 1, 1998L));
		testHarness.processElement(record("key2", 1, 1999L));
		testHarness.processElement(record("key2", 1, 1000L));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(record("key1", 3L, 3L, -2000L, 1000L, 999L));
		expectedOutput.add(new Watermark(999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(record("key1", 3L, 3L, -1000L, 2000L, 1999L));
		expectedOutput.add(record("key2", 3L, 3L, -1000L, 2000L, 1999L));
		expectedOutput.add(new Watermark(1999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(record("key1", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(record("key2", 3L, 3L, 0L, 3000L, 2999L));
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

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(record("key2", 5L, 5L, 1000L, 4000L, 3999L));
		expectedOutput.add(new Watermark(3999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(record("key2", 2L, 2L, 2000L, 5000L, 4999L));
		expectedOutput.add(new Watermark(4999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(record("key2", 2L, 2L, 3000L, 6000L, 5999L));
		expectedOutput.add(new Watermark(5999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		// we close once in the rest...
		assertEquals("Close was not called.", 2, closeCalled.get());
	}

	@Test
	public void testProcessingTimeSlidingWindows() throws Throwable {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.sliding(Duration.ofSeconds(3), Duration.ofSeconds(1), 0)
				.withProcessingTime()
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// timestamp is ignored in processing time
		testHarness.setProcessingTime(3);
		testHarness.processElement(record("key2", 1, Long.MAX_VALUE));

		testHarness.setProcessingTime(1000);

		expectedOutput.add(record("key2", 1L, 1L, -2000L, 1000L, 999L));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key2", 1, Long.MAX_VALUE));
		testHarness.processElement(record("key2", 1, Long.MAX_VALUE));

		testHarness.setProcessingTime(2000);

		expectedOutput.add(record("key2", 3L, 3L, -1000L, 2000L, 1999L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key1", 1, Long.MAX_VALUE));
		testHarness.processElement(record("key1", 1, Long.MAX_VALUE));

		testHarness.setProcessingTime(3000);

		expectedOutput.add(record("key2", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(record("key1", 2L, 2L, 0L, 3000L, 2999L));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key1", 1, Long.MAX_VALUE));
		testHarness.processElement(record("key1", 1, Long.MAX_VALUE));
		testHarness.processElement(record("key1", 1, Long.MAX_VALUE));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(record("key2", 2L, 2L, 1000L, 4000L, 3999L));
		expectedOutput.add(record("key1", 5L, 5L, 1000L, 4000L, 3999L));
		expectedOutput.add(record("key1", 5L, 5L, 2000L, 5000L, 4999L));
		expectedOutput.add(record("key1", 3L, 3L, 3000L, 6000L, 5999L));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEventTimeTumblingWindows() throws Exception {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.tumble(Duration.ofSeconds(3), 0)
				.withEventTime(2)
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(record("key2", 1, 3999L));
		testHarness.processElement(record("key2", 1, 3000L));

		testHarness.processElement(record("key1", 1, 20L));
		testHarness.processElement(record("key1", 1, 0L));
		testHarness.processElement(record("key1", 1, 999L));

		testHarness.processElement(record("key2", 1, 1998L));
		testHarness.processElement(record("key2", 1, 1999L));
		testHarness.processElement(record("key2", 1, 1000L));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(1999));
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

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(record("key1", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(record("key2", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(new Watermark(2999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(record("key2", 2L, 2L, 3000L, 6000L, 5999L));
		expectedOutput.add(new Watermark(5999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		// we close once in the rest...
		assertEquals("Close was not called.", 2, closeCalled.get());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEventTimeTumblingWindowsWithEarlyFiring() throws Exception {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.tumble(Duration.ofSeconds(3), 0)
				.withEventTime(2)
				.triggering(
						EventTimeTriggers
								.afterEndOfWindow()
								.withEarlyFirings(ProcessingTimeTriggers.every(Duration.ofSeconds(1))))
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.withSendRetraction()
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();
		testHarness.setProcessingTime(0L);

		// add elements out-of-order
		testHarness.processElement(record("key2", 1, 3999L));
		testHarness.processElement(record("key2", 1, 3000L));

		testHarness.setProcessingTime(1L);
		testHarness.processElement(record("key1", 1, 20L));
		testHarness.processElement(record("key1", 1, 0L));
		testHarness.processElement(record("key1", 1, 999L));

		testHarness.processElement(record("key2", 1, 1998L));
		testHarness.processElement(record("key2", 1, 1999L));
		testHarness.processElement(record("key2", 1, 1000L));

		testHarness.setProcessingTime(1000);
		expectedOutput.add(record("key2", 2L, 2L, 3000L, 6000L, 5999L));
		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(1001);
		expectedOutput.add(record("key1", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(record("key2", 3L, 3L, 0L, 3000L, 2999L));

		testHarness.processWatermark(new Watermark(1999));
		testHarness.setProcessingTime(2001);
		expectedOutput.add(new Watermark(1999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		// new a testHarness
		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(3001);
		testHarness.processWatermark(new Watermark(2999));
		// on time fire key1 & key2 [0 ~ 3000) window, but because of early firing, on time result is ignored
		expectedOutput.add(new Watermark(2999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key2", 1, 4999L));
		testHarness.processWatermark(new Watermark(3999));
		testHarness.setProcessingTime(4001);
		expectedOutput.add(new Watermark(3999));
		expectedOutput.add(retractRecord("key2", 2L, 2L, 3000L, 6000L, 5999L));
		expectedOutput.add(record("key2", 3L, 3L, 3000L, 6000L, 5999L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// late arrival
		testHarness.processElement(record("key2", 1, 2001L));
		testHarness.processElement(record("key1", 1, 2030L));
		// drop late elements
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(5100);
		testHarness.processElement(record("key2", 1, 5122L));
		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(retractRecord("key2", 3L, 3L, 3000L, 6000L, 5999L));
		expectedOutput.add(record("key2", 4L, 4L, 3000L, 6000L, 5999L));
		expectedOutput.add(new Watermark(5999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(6001);
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// late arrival, drop
		testHarness.processElement(record("key2", 1, 2877L));
		testHarness.processElement(record("key1", 1, 2899L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		// we close once in the rest...
		assertEquals("Close was not called.", 2, closeCalled.get());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEventTimeTumblingWindowsWithEarlyAndLateFirings() throws Exception {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.tumble(Duration.ofSeconds(3), 0)
				.withEventTime(2)
				.triggering(
						EventTimeTriggers
								.afterEndOfWindow()
								.withEarlyFirings(ProcessingTimeTriggers.every(Duration.ofSeconds(1)))
								.withLateFirings(ElementTriggers.every()))
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.withAllowedLateness(Duration.ofSeconds(3))
				.withSendRetraction()
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();
		testHarness.setProcessingTime(0L);

		// add elements out-of-order
		testHarness.processElement(record("key2", 1, 3999L));
		testHarness.processElement(record("key2", 1, 3000L));

		testHarness.setProcessingTime(1L);
		testHarness.processElement(record("key1", 1, 20L));
		testHarness.processElement(record("key1", 1, 0L));
		testHarness.processElement(record("key1", 1, 999L));

		testHarness.processElement(record("key2", 1, 1998L));
		testHarness.processElement(record("key2", 1, 1999L));
		testHarness.processElement(record("key2", 1, 1000L));

		testHarness.setProcessingTime(1000);
		expectedOutput.add(record("key2", 2L, 2L, 3000L, 6000L, 5999L));
		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(1001);
		expectedOutput.add(record("key1", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(record("key2", 3L, 3L, 0L, 3000L, 2999L));

		testHarness.processWatermark(new Watermark(1999));
		testHarness.setProcessingTime(2001);
		expectedOutput.add(new Watermark(1999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		// new a testHarness
		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(3001);
		testHarness.processWatermark(new Watermark(2999));
		// on time fire key1 & key2 [0 ~ 3000) window, but because of early firing, on time result is ignored
		expectedOutput.add(new Watermark(2999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key2", 1, 4999L));
		testHarness.processWatermark(new Watermark(3999));
		testHarness.setProcessingTime(4001);
		expectedOutput.add(new Watermark(3999));
		expectedOutput.add(retractRecord("key2", 2L, 2L, 3000L, 6000L, 5999L));
		expectedOutput.add(record("key2", 3L, 3L, 3000L, 6000L, 5999L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// late arrival
		testHarness.processElement(record("key2", 1, 2001L));
		expectedOutput.add(retractRecord("key2", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(record("key2", 4L, 4L, 0L, 3000L, 2999L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// late arrival
		testHarness.processElement(record("key1", 1, 2030L));
		expectedOutput.add(retractRecord("key1", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(record("key1", 4L, 4L, 0L, 3000L, 2999L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(5100);
		testHarness.processElement(record("key2", 1, 5122L));
		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(retractRecord("key2", 3L, 3L, 3000L, 6000L, 5999L));
		expectedOutput.add(record("key2", 4L, 4L, 3000L, 6000L, 5999L));
		expectedOutput.add(new Watermark(5999));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(6001);
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// late arrival, but too late, drop
		testHarness.processElement(record("key2", 1, 2877L));
		testHarness.processElement(record("key1", 1, 2899L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		// we close once in the rest...
		assertEquals("Close was not called.", 2, closeCalled.get());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testProcessingTimeTumblingWindows() throws Exception {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.tumble(Duration.ofSeconds(3), 0)
				.withProcessingTime()
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(record("key2", 1, Long.MAX_VALUE));
		testHarness.processElement(record("key2", 1, 7000L));
		testHarness.processElement(record("key2", 1, 7000L));

		testHarness.processElement(record("key1", 1, 7000L));
		testHarness.processElement(record("key1", 1, 7000L));

		testHarness.setProcessingTime(5000);

		expectedOutput.add(record("key2", 3L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(record("key1", 2L, 2L, 0L, 3000L, 2999L));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key1", 1, 7000L));
		testHarness.processElement(record("key1", 1, 7000L));
		testHarness.processElement(record("key1", 1, 7000L));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(record("key1", 3L, 3L, 3000L, 6000L, 5999L));

		assertEquals(0L, operator.getWatermarkLatency().getValue());
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEventTimeSessionWindows() throws Exception {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.session(Duration.ofSeconds(3))
				.withEventTime(2)
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(record("key2", 1, 0L));
		testHarness.processElement(record("key2", 2, 1000L));
		testHarness.processElement(record("key2", 3, 2500L));

		testHarness.processElement(record("key1", 1, 10L));
		testHarness.processElement(record("key1", 2, 1000L));

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshotV2 = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshotV2);
		testHarness.open();

		assertEquals(0L, operator.getWatermarkLatency().getValue());

		testHarness.processElement(record("key1", 3, 2500L));

		testHarness.processElement(record("key2", 4, 5501L));
		testHarness.processElement(record("key2", 5, 6000L));
		testHarness.processElement(record("key2", 5, 6000L));
		testHarness.processElement(record("key2", 6, 6050L));

		testHarness.processWatermark(new Watermark(12000));

		expectedOutput.add(record("key1", 6L, 3L, 10L, 5500L, 5499L));
		expectedOutput.add(record("key2", 6L, 3L, 0L, 5500L, 5499L));

		expectedOutput.add(record("key2", 20L, 4L, 5501L, 9050L, 9049L));
		expectedOutput.add(new Watermark(12000));

		// add a late data
		testHarness.processElement(record("key1", 3, 4000L));
		testHarness.processElement(record("key2", 10, 15000L));
		testHarness.processElement(record("key2", 20, 15000L));

		testHarness.processWatermark(new Watermark(17999));

		expectedOutput.add(record("key2", 30L, 2L, 15000L, 18000L, 17999L));
		expectedOutput.add(new Watermark(17999));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(18000);
		assertEquals(1L, operator.getWatermarkLatency().getValue());

		testHarness.close();

		// we close once in the rest...
		assertEquals("Close was not called.", 2, closeCalled.get());
		assertEquals(1, operator.getNumLateRecordsDropped().getCount());
	}

	@Test
	public void testProcessingTimeSessionWindows() throws Throwable {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.session(Duration.ofSeconds(3))
				.withProcessingTime()
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(
				outputType.getFieldTypes(), new GenericRowRecordSortComparator(0, new VarCharType(VarCharType.MAX_LENGTH)));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// timestamp is ignored in processing time
		testHarness.setProcessingTime(3);
		testHarness.processElement(record("key2", 1, 1L));

		testHarness.setProcessingTime(1000);
		testHarness.processElement(record("key2", 1, 1002L));

		testHarness.setProcessingTime(5000);

		expectedOutput.add(record("key2", 2L, 2L, 3L, 4000L, 3999L));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key2", 1, 5000L));
		testHarness.processElement(record("key2", 1, 5000L));
		testHarness.processElement(record("key1", 1, 5000L));
		testHarness.processElement(record("key1", 1, 5000L));
		testHarness.processElement(record("key1", 1, 5000L));

		testHarness.setProcessingTime(10000);

		expectedOutput.add(record("key2", 2L, 2L, 5000L, 8000L, 7999L));
		expectedOutput.add(record("key1", 3L, 3L, 5000L, 8000L, 7999L));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	/**
	 * This tests a custom Session window assigner that assigns some elements to "point windows",
	 * windows that have the same timestamp for start and end.
	 *
	 * <p>In this test, elements that have 33 as the second tuple field will be put into a point
	 * window.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testPointSessions() throws Exception {
		closeCalled.set(0);

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.assigner(new PointSessionWindowAssigner(3000))
				.withEventTime(2)
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(record("key2", 1, 0L));
		testHarness.processElement(record("key2", 33, 1000L));

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processElement(record("key2", 33, 2500L));

		testHarness.processElement(record("key1", 1, 10L));
		testHarness.processElement(record("key1", 2, 1000L));
		testHarness.processElement(record("key1", 33, 2500L));

		testHarness.processWatermark(new Watermark(12000));

		expectedOutput.add(record("key1", 36L, 3L, 10L, 4000L, 3999L));
		expectedOutput.add(record("key2", 67L, 3L, 0L, 3000L, 2999L));
		expectedOutput.add(new Watermark(12000));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		// we close once in the rest...
		assertEquals("Close was not called.", 2, closeCalled.get());
	}

	@Test
	public void testLateness() throws Exception {
		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.tumble(Duration.ofSeconds(2), 0)
				.withEventTime(2)
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.withAllowedLateness(Duration.ofMillis(500))
				.withSendRetraction()
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(record("key2", 1, 500L));
		testHarness.processWatermark(new Watermark(1500));

		expectedOutput.add(new Watermark(1500));

		testHarness.processElement(record("key2", 1, 1300L));
		testHarness.processWatermark(new Watermark(2300));

		GenericRow key2Result = GenericRow.of(fromString("key2"), 2L, 2L, 0L, 2000L, 1999L);
		expectedOutput.add(new StreamRecord<>(key2Result));
		expectedOutput.add(new Watermark(2300));

		// this will not be dropped because window.maxTimestamp() + allowedLateness > currentWatermark
		testHarness.processElement(record("key2", 1, 1997L));
		testHarness.processWatermark(new Watermark(6000));

		// this is 1 and not 3 because the trigger fires and purges
		BaseRow key2Retract = BaseRowUtil.setRetract(GenericRow.copyReference(key2Result));
		expectedOutput.add(new StreamRecord<>(key2Retract));
		expectedOutput.add(record("key2", 3L, 3L, 0L, 2000L, 1999L));
		expectedOutput.add(new Watermark(6000));

		// this will be dropped because window.maxTimestamp() + allowedLateness < currentWatermark
		testHarness.processElement(record("key2", 1, 1998L));
		testHarness.processWatermark(new Watermark(7000));

		expectedOutput.add(new Watermark(7000));

		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		assertEquals(1, operator.getNumLateRecordsDropped().getCount());

		testHarness.close();
	}

	@Test
	public void testCleanupTimeOverflow() throws Exception {
		long windowSize = 1000;
		long lateness = 2000;
		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.tumble(Duration.ofMillis(windowSize), 0)
				.withEventTime(2)
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.withAllowedLateness(Duration.ofMillis(lateness))
				.withSendRetraction()
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<BaseRow, BaseRow, BaseRow>(
						operator, keySelector, keyType);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		WindowAssigner<TimeWindow> windowAssigner = TumblingWindowAssigner.of(Duration.ofMillis(windowSize));
		long timestamp = Long.MAX_VALUE - 1750;
		Collection<TimeWindow> windows = windowAssigner.assignWindows(GenericRow.of(fromString("key2"), 1), timestamp);
		TimeWindow window = windows.iterator().next();

		testHarness.processElement(record("key2", 1, timestamp));

		// the garbage collection timer would wrap-around
		assertTrue(window.maxTimestamp() + lateness < window.maxTimestamp());

		// and it would prematurely fire with watermark (Long.MAX_VALUE - 1500)
		assertTrue(window.maxTimestamp() + lateness < Long.MAX_VALUE - 1500);

		// if we don't correctly prevent wrap-around in the garbage collection
		// timers this watermark will clean our window state for the just-added
		// element/window
		testHarness.processWatermark(new Watermark(Long.MAX_VALUE - 1500));

		// this watermark is before the end timestamp of our only window
		assertTrue(Long.MAX_VALUE - 1500 < window.maxTimestamp());
		assertTrue(window.maxTimestamp() < Long.MAX_VALUE);

		// push in a watermark that will trigger computation of our window
		testHarness.processWatermark(new Watermark(window.maxTimestamp()));

		expected.add(new Watermark(Long.MAX_VALUE - 1500));
		expected.add(record("key2", 1L, 1L, window.getStart(), window.getEnd(), window.maxTimestamp()));
		expected.add(new Watermark(window.maxTimestamp()));

		assertor.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyReduceStateForTumblingWindows() throws Exception {
		final int windowSize = 2;
		final long lateness = 1;

		WindowOperator operator = WindowOperatorBuilder
				.builder()
				.withInputFields(inputFieldTypes)
				.tumble(Duration.ofSeconds(windowSize), 0)
				.withEventTime(2)
				.aggregate(new SumAndCountAggTimeWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.withAllowedLateness(Duration.ofMillis(lateness))
				.withSendRetraction()
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		// normal element
		testHarness.processElement(record("key2", 1, 1000L));
		testHarness.processWatermark(new Watermark(1599));
		testHarness.processWatermark(new Watermark(1999));
		testHarness.processWatermark(new Watermark(2000));
		testHarness.processWatermark(new Watermark(5000));

		expected.add(new Watermark(1599));
		expected.add(record("key2", 1L, 1L, 0L, 2000L, 1999L));
		expected.add(new Watermark(1999)); // here it fires and purges
		expected.add(new Watermark(2000)); // here is the cleanup timer
		expected.add(new Watermark(5000));

		assertor.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTumblingCountWindow() throws Exception {
		closeCalled.set(0);
		final int windowSize = 3;
		LogicalType[] windowTypes = new LogicalType[] { new BigIntType() };

		WindowOperator operator = WindowOperatorBuilder.builder()
				.withInputFields(inputFieldTypes)
				.countWindow(windowSize)
				.aggregate(new SumAndCountAggCountWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(record("key2", 1, 0L));
		testHarness.processElement(record("key2", 2, 1000L));
		testHarness.processElement(record("key2", 3, 2500L));
		testHarness.processElement(record("key1", 1, 10L));
		testHarness.processElement(record("key1", 2, 1000L));

		testHarness.processWatermark(new Watermark(12000));
		testHarness.setProcessingTime(12000L);
		expectedOutput.add(record("key2", 6L, 3L, 0L));
		expectedOutput.add(new Watermark(12000));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshotV2 = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshotV2);
		testHarness.open();

		testHarness.processElement(record("key1", 2, 2500L));
		expectedOutput.add(record("key1", 5L, 3L, 0L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key2", 4, 5501L));
		testHarness.processElement(record("key2", 5, 6000L));
		testHarness.processElement(record("key2", 5, 6000L));
		testHarness.processElement(record("key2", 6, 6050L));

		expectedOutput.add(record("key2", 14L, 3L, 1L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key1", 3, 4000L));
		testHarness.processElement(record("key2", 10, 15000L));
		testHarness.processElement(record("key2", 20, 15000L));
		expectedOutput.add(record("key2", 36L, 3L, 2L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key1", 2, 2500L));
		testHarness.processElement(record("key1", 2, 2500L));
		expectedOutput.add(record("key1", 7L, 3L, 1L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		// we close once in the rest...
		assertEquals("Close was not called.", 2, closeCalled.get());
	}

	@Test
	public void testSlidingCountWindow() throws Exception {
		closeCalled.set(0);
		final int windowSize = 5;
		final int windowSlide = 3;
		LogicalType[] windowTypes = new LogicalType[] { new BigIntType() };

		WindowOperator operator = WindowOperatorBuilder.builder()
				.withInputFields(inputFieldTypes)
				.countWindow(windowSize, windowSlide)
				.aggregate(new SumAndCountAggCountWindow(), equaliser, accTypes, aggResultTypes, windowTypes)
				.build();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(record("key2", 1, 0L));
		testHarness.processElement(record("key2", 2, 1000L));
		testHarness.processElement(record("key2", 3, 2500L));
		testHarness.processElement(record("key2", 4, 2500L));
		testHarness.processElement(record("key2", 5, 2500L));
		testHarness.processElement(record("key1", 1, 10L));
		testHarness.processElement(record("key1", 2, 1000L));

		testHarness.processWatermark(new Watermark(12000));
		testHarness.setProcessingTime(12000L);
		expectedOutput.add(record("key2", 15L, 5L, 0L));
		expectedOutput.add(new Watermark(12000));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshotV2 = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshotV2);
		testHarness.open();

		testHarness.processElement(record("key1", 3, 2500L));
		testHarness.processElement(record("key1", 4, 2500L));
		testHarness.processElement(record("key1", 5, 2500L));
		expectedOutput.add(record("key1", 15L, 5L, 0L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key2", 6, 6000L));
		testHarness.processElement(record("key2", 7, 6000L));
		testHarness.processElement(record("key2", 8, 6050L));
		testHarness.processElement(record("key2", 9, 6050L));
		expectedOutput.add(record("key2", 30L, 5L, 1L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(record("key1", 6, 4000L));
		testHarness.processElement(record("key1", 7, 4000L));
		testHarness.processElement(record("key1", 8, 4000L));
		testHarness.processElement(record("key2", 10, 15000L));
		testHarness.processElement(record("key2", 11, 15000L));
		expectedOutput.add(record("key1", 30L, 5L, 1L));
		expectedOutput.add(record("key2", 45L, 5L, 2L));
		assertor.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		// we close once in the rest...
		assertEquals("Close was not called.", 2, closeCalled.get());
	}

	// --------------------------------------------------------------------------------

	private static class PointSessionWindowAssigner extends SessionWindowAssigner {
		private static final long serialVersionUID = 1L;

		private final long sessionTimeout;

		private PointSessionWindowAssigner(long sessionTimeout) {
			super(sessionTimeout, true);
			this.sessionTimeout = sessionTimeout;
		}

		private PointSessionWindowAssigner(long sessionTimeout, boolean isEventTime) {
			super(sessionTimeout, isEventTime);
			this.sessionTimeout = sessionTimeout;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Collection<TimeWindow> assignWindows(BaseRow element, long timestamp) {
			int second = element.getInt(1);
			if (second == 33) {
				return Collections.singletonList(new TimeWindow(timestamp, timestamp));
			}
			return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
		}

		@Override
		public SessionWindowAssigner withEventTime() {
			return new PointSessionWindowAssigner(sessionTimeout, true);
		}

		@Override
		public SessionWindowAssigner withProcessingTime() {
			return new PointSessionWindowAssigner(sessionTimeout, false);
		}
	}

	// sum, count, window_start, window_end
	private static class SumAndCountAggTimeWindow extends SumAndCountAgg<TimeWindow> {

		private static final long serialVersionUID = 2062031590687738047L;

		@Override
		public BaseRow getValue(TimeWindow namespace) throws Exception {
			if (!openCalled) {
				fail("Open was not called");
			}
			GenericRow row = new GenericRow(5);
			if (!sumIsNull) {
				row.setField(0, sum);
			}
			if (!countIsNull) {
				row.setField(1, count);
			}
			row.setField(2, namespace.getStart());
			row.setField(3, namespace.getEnd());
			row.setField(4, namespace.maxTimestamp());
			return row;
		}
	}

	// sum, count, window_id
	private static class SumAndCountAggCountWindow extends SumAndCountAgg<CountWindow> {

		private static final long serialVersionUID = -2634639678371135643L;

		@Override
		public BaseRow getValue(CountWindow namespace) throws Exception {
			if (!openCalled) {
				fail("Open was not called");
			}
			GenericRow row = new GenericRow(3);
			if (!sumIsNull) {
				row.setField(0, sum);
			}
			if (!countIsNull) {
				row.setField(1, count);
			}
			row.setField(2, namespace.getId());
			return row;
		}
	}

	private static class SumAndCountAgg<W extends Window> implements NamespaceAggsHandleFunction<W> {

		private static final long serialVersionUID = 2822222597580664436L;

		protected boolean openCalled = false;

		long sum;
		boolean sumIsNull;
		long count;
		boolean countIsNull;

		@Override
		public void open(StateDataViewStore store) throws Exception {
			openCalled = true;
		}

		@Override
		public void setAccumulators(W namespace, BaseRow acc) throws Exception {
			if (!openCalled) {
				fail("Open was not called");
			}
			sumIsNull = acc.isNullAt(0);
			if (!sumIsNull) {
				sum = acc.getLong(0);
			}

			countIsNull = acc.isNullAt(1);
			if (!countIsNull) {
				count = acc.getLong(1);
			}
		}

		@Override
		public void accumulate(BaseRow inputRow) throws Exception {
			if (!openCalled) {
				fail("Open was not called");
			}
			boolean inputIsNull = inputRow.isNullAt(1);
			if (!inputIsNull) {
				sum += inputRow.getInt(1);
				count += 1;
			}
		}

		@Override
		public void retract(BaseRow inputRow) throws Exception {
			if (!openCalled) {
				fail("Open was not called");
			}
			boolean inputIsNull = inputRow.isNullAt(1);
			if (!inputIsNull) {
				sum -= inputRow.getInt(1);
				count -= 1;
			}
		}

		@Override
		public void merge(W w, BaseRow otherAcc) throws Exception {
			if (!openCalled) {
				fail("Open was not called");
			}
			boolean sumIsNull2 = otherAcc.isNullAt(0);
			if (!sumIsNull2) {
				sum += otherAcc.getLong(0);
			}
			boolean countIsNull2 = otherAcc.isNullAt(1);
			if (!countIsNull2) {
				count += otherAcc.getLong(1);
			}
		}

		@Override
		public BaseRow createAccumulators() {
			if (!openCalled) {
				fail("Open was not called");
			}
			GenericRow acc = new GenericRow(2);
			acc.setField(0, 0L);
			acc.setField(1, 0L);
			return acc;
		}

		@Override
		public BaseRow getAccumulators() throws Exception {
			if (!openCalled) {
				fail("Open was not called");
			}
			GenericRow row = new GenericRow(2);
			if (!sumIsNull) {
				row.setField(0, sum);
			}
			if (!countIsNull) {
				row.setField(1, count);
			}
			return row;
		}

		@Override
		public BaseRow getValue(W namespace) throws Exception {
			if (!openCalled) {
				fail("Open was not called");
			}
			GenericRow row = new GenericRow(2);
			if (!sumIsNull) {
				row.setField(0, sum);
			}
			if (!countIsNull) {
				row.setField(1, count);
			}
			return row;
		}

		@Override
		public void cleanup(W window) {

		}

		@Override
		public void close() {
			closeCalled.incrementAndGet();
		}
	}

	private static class GenericRowEqualiser implements RecordEqualiser {

		private final LogicalType[] fieldTypes;

		GenericRowEqualiser(LogicalType[] aggResultTypes, LogicalType[] windowTypes) {
			int size = aggResultTypes.length + windowTypes.length;
			this.fieldTypes = new LogicalType[size];
			for (int i = 0; i < size; i++) {
				if (i < aggResultTypes.length) {
					fieldTypes[i] = aggResultTypes[i];
				} else {
					fieldTypes[i] = windowTypes[i - aggResultTypes.length];
				}
			}
		}

		@Override
		public boolean equals(BaseRow row1, BaseRow row2) {
			GenericRow left = BaseRowUtil.toGenericRow(row1, fieldTypes);
			GenericRow right = BaseRowUtil.toGenericRow(row2, fieldTypes);
			return left.equals(right);
		}

		@Override
		public boolean equalsWithoutHeader(BaseRow row1, BaseRow row2) {
			GenericRow left = BaseRowUtil.toGenericRow(row1, fieldTypes);
			GenericRow right = BaseRowUtil.toGenericRow(row2, fieldTypes);
			return left.equalsWithoutHeader(right);
		}
	}

	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createTestHarness(
			WindowOperator operator)
			throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<BaseRow, BaseRow, BaseRow>(
				operator, keySelector, keyType);
	}

}
