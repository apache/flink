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
import org.apache.flink.python.env.ProcessPythonEnvironmentManager;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.runners.python.AbstractPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.runners.python.BaseRowPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BaseRowPythonScalarFunctionRunner}. These test that
 * the input data type and output data type are properly constructed.
 */
public class BaseRowPythonScalarFunctionRunnerTest extends AbstractPythonScalarFunctionRunnerTest<BaseRow, BaseRow> {

	@Test
	public void testInputOutputDataTypeConstructedProperlyForSingleUDF() throws Exception {
		final AbstractPythonScalarFunctionRunner<BaseRow, BaseRow> runner = createSingleUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertTrue(inputTypeSerializer instanceof BaseRowSerializer);

		assertEquals(1, ((BaseRowSerializer) inputTypeSerializer).getArity());

		// check output TypeSerializer
		TypeSerializer outputTypeSerializer = runner.getOutputTypeSerializer();
		assertTrue(outputTypeSerializer instanceof BaseRowSerializer);
		assertEquals(1, ((BaseRowSerializer) outputTypeSerializer).getArity());
	}

	@Test
	public void testInputOutputDataTypeConstructedProperlyForMultipleUDFs() throws Exception {
		final AbstractPythonScalarFunctionRunner<BaseRow, BaseRow> runner = createMultipleUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertTrue(inputTypeSerializer instanceof BaseRowSerializer);

		assertEquals(3, ((BaseRowSerializer) inputTypeSerializer).getArity());

		// check output TypeSerializer
		TypeSerializer outputTypeSerializer = runner.getOutputTypeSerializer();
		assertTrue(outputTypeSerializer instanceof BaseRowSerializer);
		assertEquals(2, ((BaseRowSerializer) outputTypeSerializer).getArity());
	}

	@Test
	public void testInputOutputDataTypeConstructedProperlyForChainedUDFs() throws Exception {
		final AbstractPythonScalarFunctionRunner<BaseRow, BaseRow> runner = createChainedUDFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertTrue(inputTypeSerializer instanceof BaseRowSerializer);

		assertEquals(5, ((BaseRowSerializer) inputTypeSerializer).getArity());

		// check output TypeSerializer
		TypeSerializer outputTypeSerializer = runner.getOutputTypeSerializer();
		assertTrue(outputTypeSerializer instanceof BaseRowSerializer);
		assertEquals(3, ((BaseRowSerializer) outputTypeSerializer).getArity());
	}

	@Override
	public AbstractPythonScalarFunctionRunner<BaseRow, BaseRow> createPythonScalarFunctionRunner(
		final PythonFunctionInfo[] pythonFunctionInfos,
		RowType inputType,
		RowType outputType) throws IOException {
		final FnDataReceiver<BaseRow> dummyReceiver = input -> {
			// ignore the execution results
		};

		final PythonEnvironmentManager environmentManager =
			new ProcessPythonEnvironmentManager(
				new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), null),
				new String[] {System.getProperty("java.io.tmpdir")},
				null,
				new HashMap<>());

		return new BaseRowPythonScalarFunctionRunner(
			"testPythonRunner",
			dummyReceiver,
			pythonFunctionInfos,
			environmentManager,
			inputType,
			outputType);
	}
}
