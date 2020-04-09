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
import org.apache.flink.python.env.ProcessPythonEnvironmentManager;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link BaseRowPythonScalarFunctionRunner}. These test that
 * the input data type and output data type are properly constructed.
 */
public class BaseRowPythonScalarFunctionRunnerTest extends AbstractPythonScalarFunctionRunnerTest<BaseRow> {

	@Test
	public void testInputOutputDataTypeConstructedProperlyForSingleUDF() throws Exception {
		final BaseRowPythonScalarFunctionRunner runner = (BaseRowPythonScalarFunctionRunner) createSingleUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertEquals(1, ((BaseRowSerializer) inputTypeSerializer).getArity());
	}

	@Test
	public void testInputOutputDataTypeConstructedProperlyForMultipleUDFs() throws Exception {
		final BaseRowPythonScalarFunctionRunner runner = (BaseRowPythonScalarFunctionRunner) createMultipleUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertEquals(3, ((BaseRowSerializer) inputTypeSerializer).getArity());
	}

	@Test
	public void testInputOutputDataTypeConstructedProperlyForChainedUDFs() throws Exception {
		final BaseRowPythonScalarFunctionRunner runner = (BaseRowPythonScalarFunctionRunner) createChainedUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertEquals(5, ((BaseRowSerializer) inputTypeSerializer).getArity());
	}

	@Override
	public AbstractGeneralPythonScalarFunctionRunner<BaseRow> createPythonScalarFunctionRunner(
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

		return new BaseRowPythonScalarFunctionRunner(
			"testPythonRunner",
			dummyReceiver,
			pythonFunctionInfos,
			environmentManager,
			inputType,
			outputType,
			Collections.emptyMap(),
			PythonTestUtils.createMockFlinkMetricContainer());
	}
}
