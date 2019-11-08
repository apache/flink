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

package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.functions.python.AbstractPythonScalarFunctionRunnerTest;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Base class for Python scalar function operator test. These test that:
 *
 * <ul>
 *     <li>Retraction flag is correctly forwarded to the downstream</li>
 *     <li>FinishBundle is called when checkpoint is encountered</li>
 *     <li>Watermarks are buffered and only sent to downstream when finishedBundle is triggered</li>
 * </ul>
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 * @param <UDFIN> Type of the UDF input type.
 * @param <UDFOUT> Type of the UDF input type.
 */
public abstract class PythonScalarFunctionOperatorTestBase<IN, OUT, UDFIN, UDFOUT> {

	@Test
	public void testRetractionFieldKept() throws Exception {
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness();

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		testHarness.processElement(new StreamRecord<>(newRow(false, "c3", "c4", 1L), initialTime + 2));
		testHarness.processElement(new StreamRecord<>(newRow(false, "c5", "c6", 2L), initialTime + 3));
		testHarness.close();

		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(false, "c3", "c4", 1L)));
		expectedOutput.add(new StreamRecord<>(newRow(false, "c5", "c6", 2L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testFinishBundleTriggeredOnCheckpoint() throws Exception {
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness();

		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		testHarness.getExecutionConfig().setGlobalJobParameters(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));

		// checkpoint trigger finishBundle
		testHarness.prepareSnapshotPreBarrier(0L);

		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testFinishBundleTriggeredByCount() throws Exception {
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness();
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 2);
		testHarness.getExecutionConfig().setGlobalJobParameters(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		assertOutputEquals("FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 1L), initialTime + 2));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 1L)));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testFinishBundleTriggeredByTime() throws Exception {
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness();
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		conf.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, 1000L);
		testHarness.getExecutionConfig().setGlobalJobParameters(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		assertOutputEquals("FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

		testHarness.setProcessingTime(1000L);
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testFinishBundleTriggeredByClose() throws Exception {
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness();
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		testHarness.getExecutionConfig().setGlobalJobParameters(conf);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		assertOutputEquals("FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

		testHarness.close();
		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testWatermarkProcessedOnFinishBundle() throws Exception {
		OneInputStreamOperatorTestHarness<IN, OUT> testHarness = getTestHarness();
		Configuration conf = new Configuration();
		conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
		testHarness.getExecutionConfig().setGlobalJobParameters(conf);
		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
		testHarness.processWatermark(initialTime + 2);
		assertOutputEquals("Watermark has been processed", expectedOutput, testHarness.getOutput());

		// checkpoint trigger finishBundle
		testHarness.prepareSnapshotPreBarrier(0L);

		expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L)));
		expectedOutput.add(new Watermark(initialTime + 2));

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	private OneInputStreamOperatorTestHarness<IN, OUT> getTestHarness() throws Exception {
		RowType dataType = new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new VarCharType()),
			new RowType.RowField("f3", new BigIntType())));
		AbstractPythonScalarFunctionOperator<IN, OUT, UDFIN, UDFOUT> operator = getTestOperator(
			new PythonFunctionInfo[] {
				new PythonFunctionInfo(
					AbstractPythonScalarFunctionRunnerTest.DummyPythonFunction.INSTANCE,
					new Integer[]{0})
			},
			dataType,
			dataType,
			new int[]{2},
			new int[]{0, 1}
		);

		return new OneInputStreamOperatorTestHarness<>(operator);
	}

	public abstract AbstractPythonScalarFunctionOperator<IN, OUT, UDFIN, UDFOUT> getTestOperator(
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields);

	public abstract IN newRow(boolean accumulateMsg, Object... fields);

	public abstract void assertOutputEquals(String message, Collection<Object> expected, Collection<Object> actual);
}
