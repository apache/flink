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

package org.apache.flink.table.functions.python;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.ProcessPythonEnvironmentManager;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.runtime.runners.python.AbstractPythonTableFunctionRunner;
import org.apache.flink.table.runtime.runners.python.PythonTableFunctionRunner;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PythonTableFunctionRunner}. These test that:
 *
 * <ul>T
 *     <li>The input data type and output data type are properly constructed</li>
 *     <li>The UDTF proto is properly constructed</li>
 * </ul>
 */
public class PythonTableFunctionRunnerTest extends AbstractPythonTableFunctionRunnerTest<Row> {

	@Test
	public void testInputOutputDataTypeConstructedProperlyForSingleUDTF() throws Exception {
		final AbstractPythonTableFunctionRunner<Row> runner = createUDTFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertTrue(inputTypeSerializer instanceof RowSerializer);

		assertEquals(1, ((RowSerializer) inputTypeSerializer).getArity());
	}

	@Test
	public void testUDFnProtoConstructedProperlyForSingleUTDF() throws Exception {
		final AbstractPythonTableFunctionRunner<Row> runner = createUDTFRunner();

		FlinkFnApi.UserDefinedFunctions udtfs = runner.getUserDefinedFunctionsProto();
		assertEquals(1, udtfs.getUdfsCount());

		FlinkFnApi.UserDefinedFunction udtf = udtfs.getUdfs(0);
		assertEquals(1, udtf.getInputsCount());
		assertEquals(0, udtf.getInputs(0).getInputOffset());
	}

	@Override
	public AbstractPythonTableFunctionRunner<Row> createPythonTableFunctionRunner(
		PythonFunctionInfo pythonFunctionInfo,
		RowType inputType,
		RowType outputType) throws Exception {
		final FnDataReceiver<byte[]> dummyReceiver = input -> {
			// ignore the execution results
		};

		final PythonEnvironmentManager environmentManager =
			new ProcessPythonEnvironmentManager(
				new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), null),
				new String[]{System.getProperty("java.io.tmpdir")},
				new HashMap<>());

		return new PythonTableFunctionRunner(
			"testPythonRunner",
			dummyReceiver,
			pythonFunctionInfo,
			environmentManager,
			inputType,
			outputType);
	}

	private AbstractPythonTableFunctionRunner<Row> createUDTFRunner(
		JobBundleFactory jobBundleFactory, FnDataReceiver<byte[]> receiver) throws IOException {
		PythonFunctionInfo pythonFunctionInfo = new PythonFunctionInfo(
			AbstractPythonScalarFunctionRunnerTest.DummyPythonFunction.INSTANCE,
			new Integer[]{0});

		RowType rowType = new RowType(Collections.singletonList(new RowType.RowField("f1", new BigIntType())));

		final PythonEnvironmentManager environmentManager =
			new ProcessPythonEnvironmentManager(
				new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), null),
				new String[]{System.getProperty("java.io.tmpdir")},
				new HashMap<>());

		return new PythonTableFunctionRunnerTestHarness(
			"testPythonRunner",
			receiver,
			pythonFunctionInfo,
			environmentManager,
			rowType,
			rowType,
			jobBundleFactory);
	}

	private static class PythonTableFunctionRunnerTestHarness extends PythonTableFunctionRunner {

		private final JobBundleFactory jobBundleFactory;

		PythonTableFunctionRunnerTestHarness(
			String taskName,
			FnDataReceiver<byte[]> resultReceiver,
			PythonFunctionInfo tableFunction,
			PythonEnvironmentManager environmentManager,
			RowType inputType,
			RowType outputType,
			JobBundleFactory jobBundleFactory) {
			super(taskName, resultReceiver, tableFunction, environmentManager, inputType, outputType);
			this.jobBundleFactory = jobBundleFactory;
		}

		@Override
		public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) throws Exception {
			return jobBundleFactory;
		}
	}
}
