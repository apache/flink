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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.AbstractArrowPythonAggregateFunctionOperator;
import org.apache.flink.table.runtime.utils.PassThroughPythonAggregateFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link BatchArrowPythonOverWindowAggregateFunctionOperator}. These test that:
 *
 * <ul>
 * 		<li>FinishBundle is called when bundled element count reach to max bundle size</li>
 * 		<li>FinishBundle is called when bundled time reach to max bundle time</li>
 * </ul>
 */
public class BatchArrowPythonOverWindowAggregateFunctionOperatorTest
	extends AbstractBatchArrowPythonAggregateFunctionOperatorTest {

	@Test
	public void testOverWindowAggregateFunction() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(
			new Configuration());

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 0L), initialTime + 2));
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));

		testHarness.close();

		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L, 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 0L, 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 2L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L, 3L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testFinishBundleTriggeredByCount() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 3);
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 0L), initialTime + 2));
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
		assertOutputEquals("FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L, 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 0L, 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 2L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L, 3L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testFinishBundleTriggeredByTime() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		conf.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, 1000L);
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 0L), initialTime + 2));
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));
		assertOutputEquals("FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(1000L);
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L, 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 0L, 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 2L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();

		expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L, 3L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testUserDefinedFunctionsProto() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(
			new Configuration());
		testHarness.open();
		BatchArrowPythonOverWindowAggregateFunctionOperator operator =
			(BatchArrowPythonOverWindowAggregateFunctionOperator) testHarness.getOneInputOperator();
		FlinkFnApi.UserDefinedFunctions functionsProto = operator.getUserDefinedFunctionsProto();
		List<FlinkFnApi.OverWindow> windows = functionsProto.getWindowsList();
		assertEquals(2, windows.size());

		// first window is a range sliding window.
		FlinkFnApi.OverWindow firstWindow = windows.get(0);
		assertEquals(firstWindow.getWindowType(), FlinkFnApi.OverWindow.WindowType.RANGE_SLIDING);

		// second window is a row unbounded preceding window.
		FlinkFnApi.OverWindow secondWindow = windows.get(1);
		assertEquals(secondWindow.getWindowType(), FlinkFnApi.OverWindow.WindowType.ROW_UNBOUNDED_PRECEDING);
		assertEquals(secondWindow.getUpperBoundary(), 2L);
	}

	@Override
	public LogicalType[] getOutputLogicalType() {
		return new LogicalType[]{
			DataTypes.STRING().getLogicalType(),
			DataTypes.STRING().getLogicalType(),
			DataTypes.BIGINT().getLogicalType(),
			DataTypes.BIGINT().getLogicalType(),
			DataTypes.BIGINT().getLogicalType()
		};
	}

	@Override
	public RowType getInputType() {
		return new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new VarCharType()),
			new RowType.RowField("f3", new BigIntType()),
			new RowType.RowField("rowTime", new BigIntType())));
	}

	@Override
	public RowType getOutputType() {
		return new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new VarCharType()),
			new RowType.RowField("f3", new BigIntType()),
			new RowType.RowField("rowTime", new BigIntType()),
			new RowType.RowField("agg", new BigIntType())));
	}

	@Override
	public AbstractArrowPythonAggregateFunctionOperator getTestOperator(
		Configuration config,
		PythonFunctionInfo[] pandasAggregateFunctions,
		RowType inputType,
		RowType outputType,
		int[] groupingSet,
		int[] udafInputOffsets) {
		return new PassThroughBatchArrowPythonOverWindowAggregateFunctionOperator(
			config,
			pandasAggregateFunctions,
			inputType,
			outputType,
			new long[]{0L, Long.MIN_VALUE},
			new long[]{0L, 2L},
			new boolean[]{true, false},
			new int[]{0},
			groupingSet,
			groupingSet,
			udafInputOffsets,
			3,
			true);
	}

	private static class PassThroughBatchArrowPythonOverWindowAggregateFunctionOperator
		extends BatchArrowPythonOverWindowAggregateFunctionOperator {

		PassThroughBatchArrowPythonOverWindowAggregateFunctionOperator(
			Configuration config,
			PythonFunctionInfo[] pandasAggFunctions,
			RowType inputType,
			RowType outputType,
			long[] lowerBoundary,
			long[] upperBoundary,
			boolean[] isRangeWindow,
			int[] aggWindowIndex,
			int[] groupKey,
			int[] groupingSet,
			int[] udafInputOffsets,
			int inputTimeFieldIndex,
			boolean asc) {
			super(config, pandasAggFunctions, inputType, outputType, lowerBoundary, upperBoundary,
				isRangeWindow, aggWindowIndex, groupKey, groupingSet, udafInputOffsets, inputTimeFieldIndex, asc);
		}

		@Override
		public PythonFunctionRunner createPythonFunctionRunner() {
			return new PassThroughPythonAggregateFunctionRunner(
				getRuntimeContext().getTaskName(),
				PythonTestUtils.createTestEnvironmentManager(),
				userDefinedFunctionInputType,
				userDefinedFunctionOutputType,
				getFunctionUrn(),
				getUserDefinedFunctionsProto(),
				getInputOutputCoderUrn(),
				new HashMap<>(),
				PythonTestUtils.createMockFlinkMetricContainer(),
				true);
		}
	}
}
