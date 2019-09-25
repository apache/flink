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

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.runners.python.AbstractPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.runners.python.BaseRowPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.typeutils.coders.BaseRowCoder;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BaseRowPythonScalarFunctionRunner}. These test that
 * the input data type and output data type are properly constructed.
 */
public class BaseRowPythonScalarFunctionRunnerTest extends AbstractPythonScalarFunctionRunnerTest<BaseRow, BaseRow> {

	@Test
	public void testInputOutputDataTypeConstructedProperlyForSingleUDF() {
		final AbstractPythonScalarFunctionRunner<BaseRow, BaseRow> runner = createSingleUDFRunner();

		// check input coder
		Coder<BaseRow> inputCoder = runner.getInputCoder();
		assertTrue(inputCoder instanceof BaseRowCoder);

		Coder<?>[] inputFieldCoders = ((BaseRowCoder) inputCoder).getFieldCoders();
		assertEquals(1, inputFieldCoders.length);
		assertTrue(inputFieldCoders[0] instanceof VarLongCoder);

		// check output coder
		Coder<BaseRow> outputCoder = runner.getOutputCoder();
		assertTrue(outputCoder instanceof BaseRowCoder);
		Coder<?>[] outputFieldCoders = ((BaseRowCoder) outputCoder).getFieldCoders();
		assertEquals(1, outputFieldCoders.length);
		assertTrue(outputFieldCoders[0] instanceof VarLongCoder);
	}

	@Test
	public void testInputOutputDataTypeConstructedProperlyForMultipleUDFs() {
		final AbstractPythonScalarFunctionRunner<BaseRow, BaseRow> runner = createMultipleUDFRunner();

		// check input coder
		Coder<BaseRow> inputCoder = runner.getInputCoder();
		assertTrue(inputCoder instanceof BaseRowCoder);

		Coder<?>[] inputFieldCoders = ((BaseRowCoder) inputCoder).getFieldCoders();
		assertEquals(3, inputFieldCoders.length);
		assertTrue(inputFieldCoders[0] instanceof VarLongCoder);
		assertTrue(inputFieldCoders[1] instanceof VarLongCoder);
		assertTrue(inputFieldCoders[2] instanceof VarLongCoder);

		// check output coder
		Coder<BaseRow> outputCoder = runner.getOutputCoder();
		assertTrue(outputCoder instanceof BaseRowCoder);
		Coder<?>[] outputFieldCoders = ((BaseRowCoder) outputCoder).getFieldCoders();
		assertEquals(2, outputFieldCoders.length);
		assertTrue(outputFieldCoders[0] instanceof VarLongCoder);
		assertTrue(outputFieldCoders[1] instanceof VarLongCoder);
	}

	@Test
	public void testInputOutputDataTypeConstructedProperlyForChainedUDFs() {
		final AbstractPythonScalarFunctionRunner<BaseRow, BaseRow> runner = createChainedUDFRunner();

		// check input coder
		Coder<BaseRow> inputCoder = runner.getInputCoder();
		assertTrue(inputCoder instanceof BaseRowCoder);

		Coder<?>[] inputFieldCoders = ((BaseRowCoder) inputCoder).getFieldCoders();
		assertEquals(5, inputFieldCoders.length);
		assertTrue(inputFieldCoders[0] instanceof VarLongCoder);
		assertTrue(inputFieldCoders[1] instanceof VarLongCoder);
		assertTrue(inputFieldCoders[2] instanceof VarLongCoder);
		assertTrue(inputFieldCoders[3] instanceof VarLongCoder);
		assertTrue(inputFieldCoders[4] instanceof VarLongCoder);

		// check output coder
		Coder<BaseRow> outputCoder = runner.getOutputCoder();
		assertTrue(outputCoder instanceof BaseRowCoder);
		Coder<?>[] outputFieldCoders = ((BaseRowCoder) outputCoder).getFieldCoders();
		assertEquals(3, outputFieldCoders.length);
		assertTrue(outputFieldCoders[0] instanceof VarLongCoder);
		assertTrue(outputFieldCoders[1] instanceof VarLongCoder);
		assertTrue(outputFieldCoders[2] instanceof VarLongCoder);
	}

	@Override
	public AbstractPythonScalarFunctionRunner<BaseRow, BaseRow> createPythonScalarFunctionRunner(
		final PythonFunctionInfo[] pythonFunctionInfos,
		RowType inputType,
		RowType outputType) {
		final FnDataReceiver<BaseRow> dummyReceiver = input -> {
			// ignore the execution results
		};

		final PythonEnv pythonEnv = new PythonEnv(PythonEnv.ExecType.PROCESS);

		return new BaseRowPythonScalarFunctionRunner(
			"testPythonRunner",
			dummyReceiver,
			pythonFunctionInfos,
			pythonEnv,
			inputType,
			outputType,
			new String[] {System.getProperty("java.io.tmpdir")});
	}
}
