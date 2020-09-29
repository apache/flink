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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
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
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test for {@link StreamArrowPythonProcTimeBoundedRangeOperator}.
 */
public class StreamArrowPythonProcTimeBoundedRangeOperatorTest
	extends AbstractStreamArrowPythonAggregateFunctionOperatorTest {

	@Test
	public void testOverWindowAggregateFunction() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(
			new Configuration());

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(100);
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L)));
		testHarness.setProcessingTime(150);
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L)));
		testHarness.setProcessingTime(200);
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L)));
		testHarness.setProcessingTime(300);
		testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L)));
		testHarness.setProcessingTime(600);

		testHarness.close();

		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 1L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 3L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Override
	public LogicalType[] getOutputLogicalType() {
		return new LogicalType[]{
			DataTypes.STRING().getLogicalType(),
			DataTypes.STRING().getLogicalType(),
			DataTypes.BIGINT().getLogicalType(),
			DataTypes.BIGINT().getLogicalType(),
		};
	}

	@Override
	public RowType getInputType() {
		return new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new VarCharType()),
			new RowType.RowField("f3", new BigIntType())));
	}

	@Override
	public RowType getOutputType() {
		return new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new VarCharType()),
			new RowType.RowField("f3", new BigIntType()),
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
		return new PassThroughStreamArrowPythonProcTimeBoundedRangeOperator(
			config,
			pandasAggregateFunctions,
			inputType,
			outputType,
			-1,
			100L,
			groupingSet,
			udafInputOffsets);
	}

	private static class PassThroughStreamArrowPythonProcTimeBoundedRangeOperator
		extends StreamArrowPythonProcTimeBoundedRangeOperator {

		PassThroughStreamArrowPythonProcTimeBoundedRangeOperator(
			Configuration config,
			PythonFunctionInfo[] pandasAggFunctions,
			RowType inputType,
			RowType outputType,
			int inputTimeFieldIndex,
			long lowerBoundary,
			int[] groupingSet,
			int[] udafInputOffsets) {
			super(config, pandasAggFunctions, inputType, outputType, inputTimeFieldIndex,
				lowerBoundary, groupingSet, udafInputOffsets);
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
				false
			);
		}
	}
}
