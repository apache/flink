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

package org.apache.flink.table.runtime.runners.python.scalar;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.ProcessPythonEnvironmentManager;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.utils.PassThroughPythonScalarFunctionRunner;
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

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PythonScalarFunctionRunner}. These test that:
 *
 * <ul>
 *     <li>The input data type and output data type are properly constructed</li>
 *     <li>The UDF proto is properly constructed</li>
 * </ul>
 */
public class PythonScalarFunctionRunnerTest extends AbstractPythonScalarFunctionRunnerTest<Row> {

	@Test
	public void testInputOutputDataTypeConstructedProperlyForSingleUDF() throws Exception {
		final PythonScalarFunctionRunner runner = (PythonScalarFunctionRunner) createSingleUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertEquals(1, ((RowSerializer) inputTypeSerializer).getArity());
	}

	@Test
	public void testInputOutputDataTypeConstructedProperlyForMultipleUDFs() throws Exception {
		final PythonScalarFunctionRunner runner = (PythonScalarFunctionRunner) createMultipleUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertEquals(3, ((RowSerializer) inputTypeSerializer).getArity());
	}

	@Test
	public void testInputOutputDataTypeConstructedProperlyForChainedUDFs() throws Exception {
		final PythonScalarFunctionRunner runner = (PythonScalarFunctionRunner) createChainedUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertEquals(5, ((RowSerializer) inputTypeSerializer).getArity());
	}

	@Test
	public void testUDFProtoConstructedProperlyForSingleUDF() throws Exception {
		final AbstractPythonScalarFunctionRunner<Row> runner = createSingleUDFRunner();

		FlinkFnApi.UserDefinedFunctions udfs = runner.getUserDefinedFunctionsProto();
		assertEquals(1, udfs.getUdfsCount());

		FlinkFnApi.UserDefinedFunction udf = udfs.getUdfs(0);
		assertEquals(1, udf.getInputsCount());
		assertEquals(0, udf.getInputs(0).getInputOffset());
	}

	@Test
	public void testUDFProtoConstructedProperlyForMultipleUDFs() throws Exception {
		final AbstractPythonScalarFunctionRunner<Row> runner = createMultipleUDFRunner();

		FlinkFnApi.UserDefinedFunctions udfs = runner.getUserDefinedFunctionsProto();
		assertEquals(2, udfs.getUdfsCount());

		FlinkFnApi.UserDefinedFunction udf = udfs.getUdfs(0);
		assertEquals(2, udf.getInputsCount());
		assertEquals(0, udf.getInputs(0).getInputOffset());
		assertEquals(1, udf.getInputs(1).getInputOffset());

		udf = udfs.getUdfs(1);
		assertEquals(2, udf.getInputsCount());
		assertEquals(0, udf.getInputs(0).getInputOffset());
		assertEquals(2, udf.getInputs(1).getInputOffset());
	}

	@Test
	public void testUDFProtoConstructedProperlyForChainedUDFs() throws Exception {
		final PythonScalarFunctionRunner runner = (PythonScalarFunctionRunner) createChainedUDFRunner();

		FlinkFnApi.UserDefinedFunctions udfs = runner.getUserDefinedFunctionsProto();
		assertEquals(3, udfs.getUdfsCount());

		FlinkFnApi.UserDefinedFunction udf = udfs.getUdfs(0);
		assertEquals(2, udf.getInputsCount());
		assertEquals(0, udf.getInputs(0).getInputOffset());
		assertEquals(1, udf.getInputs(1).getInputOffset());

		udf = udfs.getUdfs(1);
		assertEquals(2, udf.getInputsCount());
		assertEquals(0, udf.getInputs(0).getInputOffset());
		FlinkFnApi.UserDefinedFunction chainedUdf = udf.getInputs(1).getUdf();
		assertEquals(2, chainedUdf.getInputsCount());
		assertEquals(1, chainedUdf.getInputs(0).getInputOffset());
		assertEquals(2, chainedUdf.getInputs(1).getInputOffset());

		udf = udfs.getUdfs(2);
		chainedUdf = udf.getInputs(0).getUdf();
		assertEquals(2, chainedUdf.getInputsCount());
		assertEquals(1, chainedUdf.getInputs(0).getInputOffset());
		assertEquals(3, chainedUdf.getInputs(1).getInputOffset());
		chainedUdf = udf.getInputs(1).getUdf();
		assertEquals(2, chainedUdf.getInputsCount());
		assertEquals(3, chainedUdf.getInputs(0).getInputOffset());
		assertEquals(4, chainedUdf.getInputs(1).getInputOffset());
	}

	@Test
	public void testPythonScalarFunctionRunner() throws Exception {
		JobBundleFactory jobBundleFactorySpy = spy(JobBundleFactory.class);
		FnDataReceiver<byte[]> resultReceiverSpy = spy(FnDataReceiver.class);
		final AbstractGeneralPythonScalarFunctionRunner<Row> runner =
			createUDFRunner(jobBundleFactorySpy, resultReceiverSpy);

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
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		runner.getInputTypeSerializer().serialize(Row.of(1L), new DataOutputViewStreamWrapper(baos));

		// verify remoteBundle.close() is called during finishBundle
		verify(remoteBundleSpy, times(0)).close();
		runner.finishBundle();
		verify(remoteBundleSpy, times(1)).close();

		// verify the input element is processed and the result is hand over to the result receiver.
		verify(resultReceiverSpy, times(1)).accept(argThat(
			value -> Objects.deepEquals(value, baos.toByteArray())));
	}

	@Override
	public AbstractGeneralPythonScalarFunctionRunner<Row> createPythonScalarFunctionRunner(
		final PythonFunctionInfo[] pythonFunctionInfos,
		RowType inputType,
		RowType outputType) {
		final FnDataReceiver<byte[]> dummyReceiver = input -> {
			// ignore the execution results
		};

		final PythonEnvironmentManager environmentManager =
			new ProcessPythonEnvironmentManager(
				new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), null),
				new String[] {System.getProperty("java.io.tmpdir")},
				new HashMap<>());

		return new PythonScalarFunctionRunner(
			"testPythonRunner",
			dummyReceiver,
			pythonFunctionInfos,
			environmentManager,
			inputType,
			outputType,
			Collections.emptyMap(),
			PythonTestUtils.createMockFlinkMetricContainer());
	}

	private AbstractGeneralPythonScalarFunctionRunner<Row> createUDFRunner(
		JobBundleFactory jobBundleFactory, FnDataReceiver<byte[]> receiver) {
		PythonFunctionInfo[] pythonFunctionInfos = new PythonFunctionInfo[] {
			new PythonFunctionInfo(
				DummyPythonFunction.INSTANCE,
				new Integer[]{0})
		};

		RowType rowType = new RowType(Collections.singletonList(new RowType.RowField("f1", new BigIntType())));

		final PythonEnvironmentManager environmentManager =
			new ProcessPythonEnvironmentManager(
				new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), null),
				new String[] {System.getProperty("java.io.tmpdir")},
				new HashMap<>());

		return new PassThroughPythonScalarFunctionRunner<Row>(
			"testPythonRunner",
			receiver,
			pythonFunctionInfos,
			environmentManager,
			rowType,
			rowType,
			Collections.emptyMap(),
			jobBundleFactory,
			PythonTestUtils.createMockFlinkMetricContainer()) {
			@Override
			public TypeSerializer<Row> getInputTypeSerializer() {
				return (RowSerializer) PythonTypeUtils.toFlinkTypeSerializer(getInputType());
			}
		};
	}
}
