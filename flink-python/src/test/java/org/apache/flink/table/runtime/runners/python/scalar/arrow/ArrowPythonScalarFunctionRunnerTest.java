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

package org.apache.flink.table.runtime.runners.python.scalar.arrow;

import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.runtime.arrow.writers.ArrowFieldWriter;
import org.apache.flink.table.runtime.arrow.writers.RowBigIntWriter;
import org.apache.flink.table.runtime.runners.python.scalar.AbstractPythonScalarFunctionRunnerTest;
import org.apache.flink.table.runtime.utils.PassThroughArrowPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.runtime.utils.PythonTestUtils.createTestEnvironmentManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ArrowPythonScalarFunctionRunner}.
 */
public class ArrowPythonScalarFunctionRunnerTest extends AbstractPythonScalarFunctionRunnerTest<Row> {

	@Test
	public void testArrowWriterConstructedProperlyForSingleUDF() throws Exception {
		final AbstractArrowPythonScalarFunctionRunner<Row> runner = (AbstractArrowPythonScalarFunctionRunner<Row>) createSingleUDFRunner();
		runner.open();

		ArrowFieldWriter<Row>[] fieldWriters = runner.arrowWriter.getFieldWriters();
		assertEquals(1, fieldWriters.length);
		assertTrue(fieldWriters[0] instanceof RowBigIntWriter);
	}

	@Test
	public void testArrowWriterConstructedProperlyForMultipleUDFs() throws Exception {
		final AbstractArrowPythonScalarFunctionRunner<Row> runner = (AbstractArrowPythonScalarFunctionRunner<Row>) createMultipleUDFRunner();
		runner.open();

		ArrowFieldWriter<Row>[] fieldWriters = runner.arrowWriter.getFieldWriters();
		assertEquals(3, fieldWriters.length);
		assertTrue(fieldWriters[0] instanceof RowBigIntWriter);
		assertTrue(fieldWriters[1] instanceof RowBigIntWriter);
		assertTrue(fieldWriters[2] instanceof RowBigIntWriter);
	}

	@Test
	public void testArrowWriterConstructedProperlyForChainedUDFs() throws Exception {
		final AbstractArrowPythonScalarFunctionRunner<Row> runner = (AbstractArrowPythonScalarFunctionRunner<Row>) createChainedUDFRunner();
		runner.open();

		ArrowFieldWriter<Row>[] fieldWriters = runner.arrowWriter.getFieldWriters();
		assertEquals(5, fieldWriters.length);
		assertTrue(fieldWriters[0] instanceof RowBigIntWriter);
		assertTrue(fieldWriters[1] instanceof RowBigIntWriter);
		assertTrue(fieldWriters[2] instanceof RowBigIntWriter);
		assertTrue(fieldWriters[3] instanceof RowBigIntWriter);
		assertTrue(fieldWriters[4] instanceof RowBigIntWriter);
	}

	@Test
	public void testArrowPythonScalarFunctionRunner() throws Exception {
		JobBundleFactory jobBundleFactorySpy = spy(JobBundleFactory.class);
		FnDataReceiver<byte[]> resultReceiverSpy = spy(FnDataReceiver.class);
		PythonFunctionInfo[] pythonFunctionInfos = new PythonFunctionInfo[] {
			new PythonFunctionInfo(
				DummyPythonFunction.INSTANCE,
				new Integer[]{0})
		};
		RowType rowType = new RowType(Collections.singletonList(new RowType.RowField("f1", new BigIntType())));
		final AbstractArrowPythonScalarFunctionRunner<Row> runner = createPassThroughArrowPythonScalarFunctionRunner(
			resultReceiverSpy,
			pythonFunctionInfos,
			rowType,
			rowType,
			2,
			jobBundleFactorySpy);

		StageBundleFactory stageBundleFactorySpy = spy(StageBundleFactory.class);
		when(jobBundleFactorySpy.forStage(any())).thenReturn(stageBundleFactorySpy);
		RemoteBundle remoteBundleSpy = spy(RemoteBundle.class);
		when(stageBundleFactorySpy.getBundle(any(), any(), any())).thenReturn(remoteBundleSpy);
		Map<String, FnDataReceiver> inputReceivers = new HashMap<>();
		FnDataReceiver<WindowedValue<?>> windowedValueReceiverSpy = spy(FnDataReceiver.class);
		inputReceivers.put("input", windowedValueReceiverSpy);
		when(remoteBundleSpy.getInputReceivers()).thenReturn(inputReceivers);

		runner.open();
		verify(jobBundleFactorySpy, times(1)).forStage(any());

		// verify stageBundleFactory.getBundle is called during startBundle
		verify(stageBundleFactorySpy, times(0)).getBundle(any(), any(), any());
		runner.startBundle();
		verify(stageBundleFactorySpy, times(1)).getBundle(any(), any(), any());

		runner.processElement(Row.of(1L));

		// verify remoteBundle.close() is called during finishBundle
		verify(remoteBundleSpy, times(0)).close();
		runner.finishBundle();
		verify(remoteBundleSpy, times(1)).close();

		// verify the input element is processed and the result is hand over to the result receiver.
		verify(resultReceiverSpy, times(1)).accept(any());
	}

	@Override
	public AbstractArrowPythonScalarFunctionRunner<Row> createPythonScalarFunctionRunner(
		PythonFunctionInfo[] pythonFunctionInfos,
		RowType inputType,
		RowType outputType) {
		final FnDataReceiver<byte[]> dummyReceiver = input -> {
			// ignore the execution results
		};

		return createPassThroughArrowPythonScalarFunctionRunner(
			dummyReceiver, pythonFunctionInfos, inputType, outputType, 1, spy(JobBundleFactory.class));
	}

	private AbstractArrowPythonScalarFunctionRunner<Row> createPassThroughArrowPythonScalarFunctionRunner(
		FnDataReceiver<byte[]> receiver,
		PythonFunctionInfo[] pythonFunctionInfos,
		RowType inputType,
		RowType outputType,
		int maxArrowBatchSize,
		JobBundleFactory jobBundleFactory) {

		final PythonEnvironmentManager environmentManager = createTestEnvironmentManager();

		return new PassThroughArrowPythonScalarFunctionRunner<Row>(
			"testPythonRunner",
			receiver,
			pythonFunctionInfos,
			environmentManager,
			inputType,
			outputType,
			maxArrowBatchSize,
			Collections.emptyMap(),
			jobBundleFactory,
			PythonTestUtils.createMockFlinkMetricContainer()) {
			@Override
			public ArrowWriter<Row> createArrowWriter() {
				return ArrowUtils.createRowArrowWriter(root, getInputType());
			}
		};
	}
}
