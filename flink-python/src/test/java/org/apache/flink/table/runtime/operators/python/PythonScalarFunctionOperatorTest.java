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

import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.functions.python.AbstractPythonScalarFunctionRunnerTest;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for {@link PythonScalarFunctionOperator}. These test that:
 *
 * <ul>
 *     <li>Retraction flag is correctly forwarded to the downstream</li>
 *     <li>FinishBundle is called when checkpoint is encountered</li>
 *     <li>Watermarks are buffered and only sent to downstream when finishedBundle is triggered</li>
 * </ul>
 */
public class PythonScalarFunctionOperatorTest {

	@Test
	public void testRetractionFieldKept() throws Exception {
		OneInputStreamOperatorTestHarness<CRow, CRow> testHarness = getTestHarness();

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new CRow(Row.of("c1", "c2", 0L), true), initialTime + 1));
		testHarness.processElement(new StreamRecord<>(new CRow(Row.of("c3", "c4", 1L), false), initialTime + 2));
		testHarness.processElement(new StreamRecord<>(new CRow(Row.of("c5", "c6", 2L), false), initialTime + 3));
		testHarness.close();

		expectedOutput.add(new StreamRecord<>(new CRow(Row.of("c1", "c2", 0L), true)));
		expectedOutput.add(new StreamRecord<>(new CRow(Row.of("c3", "c4", 1L), false)));
		expectedOutput.add(new StreamRecord<>(new CRow(Row.of("c5", "c6", 2L), false)));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testFinishedBundleTriggeredOnCheckpoint() throws Exception {
		OneInputStreamOperatorTestHarness<CRow, CRow> testHarness = getTestHarness();
		testHarness.getEnvironment().getTaskConfiguration().setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new CRow(Row.of("c1", "c2", 0L), true), initialTime + 1));

		// checkpoint trigger finishBundle
		testHarness.prepareSnapshotPreBarrier(0L);

		expectedOutput.add(new StreamRecord<>(new CRow(Row.of("c1", "c2", 0L), true)));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testWatermarkProcessedOnFinishedBundle() throws Exception {
		OneInputStreamOperatorTestHarness<CRow, CRow> testHarness = getTestHarness();
		testHarness.getEnvironment().getTaskConfiguration().setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new CRow(Row.of("c1", "c2", 0L), true), initialTime + 1));
		testHarness.processWatermark(initialTime + 2);
		TestHarnessUtil.assertOutputEquals("Watermark has been processed", expectedOutput, testHarness.getOutput());

		// checkpoint trigger finishBundle
		testHarness.prepareSnapshotPreBarrier(0L);

		expectedOutput.add(new StreamRecord<>(new CRow(Row.of("c1", "c2", 0L), true)));
		expectedOutput.add(new Watermark(initialTime + 2));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	private OneInputStreamOperatorTestHarness<CRow, CRow> getTestHarness() throws Exception {
		RowType inputType = new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new VarCharType()),
			new RowType.RowField("f3", new BigIntType())));
		RowType outputType = new RowType(Arrays.asList(
			new RowType.RowField("f1", new VarCharType()),
			new RowType.RowField("f2", new VarCharType()),
			new RowType.RowField("f3", new BigIntType())));
		PassThroughPythonScalarFunctionOperator operator = new PassThroughPythonScalarFunctionOperator(
			new PythonFunctionInfo[] {
				new PythonFunctionInfo(
					AbstractPythonScalarFunctionRunnerTest.DummyPythonFunction.INSTANCE,
					new Integer[]{0})
			},
			inputType,
			outputType,
			new int[]{2},
			2
		);

		return new OneInputStreamOperatorTestHarness<>(operator);
	}

	private static class PassThroughPythonFunctionRunner implements PythonFunctionRunner<Row> {

		private boolean bundleStarted;
		private final List<Row> bufferedElements;
		private final FnDataReceiver<Row> resultReceiver;

		PassThroughPythonFunctionRunner(FnDataReceiver<Row> resultReceiver) {
			this.resultReceiver = Preconditions.checkNotNull(resultReceiver);
			bundleStarted = false;
			bufferedElements = new ArrayList<>();
		}

		@Override
		public void open() {}

		@Override
		public void close() {}

		@Override
		public void startBundle() {
			Preconditions.checkState(!bundleStarted);
			bundleStarted = true;
		}

		@Override
		public void finishBundle() throws Exception {
			Preconditions.checkState(bundleStarted);
			bundleStarted = false;

			for (Row element : bufferedElements) {
				resultReceiver.accept(element);
			}
			bufferedElements.clear();
		}

		@Override
		public void processElement(Row element) {
			bufferedElements.add(element);
		}
	}

	private static class PassThroughPythonScalarFunctionOperator extends PythonScalarFunctionOperator {

		PassThroughPythonScalarFunctionOperator(
			PythonFunctionInfo[] scalarFunctions,
			RowType inputType,
			RowType outputType,
			int[] udfInputOffsets,
			int forwardedFieldCnt) {
			super(scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFieldCnt);
		}

		@Override
		public PythonFunctionRunner<Row> createPythonFunctionRunner(
			FnDataReceiver<Row> resultReceiver) {
			return new PassThroughPythonFunctionRunner(resultReceiver);
		}
	}
}
