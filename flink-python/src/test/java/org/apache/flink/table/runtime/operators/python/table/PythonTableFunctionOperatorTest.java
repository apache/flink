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

package org.apache.flink.table.runtime.operators.python.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.utils.PassThroughPythonTableFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.calcite.rel.core.JoinRelType;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Queue;

/**
 * Tests for {@link PythonTableFunctionOperator}.
 */
public class PythonTableFunctionOperatorTest extends PythonTableFunctionOperatorTestBase<CRow, CRow, Row> {
	@Override
	public AbstractPythonTableFunctionOperator<CRow, CRow, Row> getTestOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		JoinRelType joinRelType) {
		return new PassThroughPythonTableFunctionOperator(
			config, tableFunction, inputType, outputType, udfInputOffsets, joinRelType);
	}

	@Override
	public CRow newRow(boolean accumulateMsg, Object... fields) {
		return new CRow(Row.of(fields), accumulateMsg);
	}

	@Override
	public void assertOutputEquals(String message, Collection<Object> expected, Collection<Object> actual) {
		TestHarnessUtil.assertOutputEquals(message, (Queue<Object>) expected, (Queue<Object>) actual);
	}

	private static class PassThroughPythonTableFunctionOperator extends PythonTableFunctionOperator {

		PassThroughPythonTableFunctionOperator(
			Configuration config,
			PythonFunctionInfo tableFunction,
			RowType inputType,
			RowType outputType,
			int[] udfInputOffsets,
			JoinRelType joinRelType) {
			super(config, tableFunction, inputType, outputType, udfInputOffsets, joinRelType);
		}

		@Override
		public PythonFunctionRunner createPythonFunctionRunner() throws IOException {
			return new PassThroughPythonTableFunctionRunner(
				getRuntimeContext().getTaskName(),
				PythonTestUtils.createTestEnvironmentManager(),
				userDefinedFunctionInputType,
				userDefinedFunctionOutputType,
				getFunctionUrn(),
				getUserDefinedFunctionsProto(),
				getInputOutputCoderUrn(),
				new HashMap<>(),
				PythonTestUtils.createMockFlinkMetricContainer());
		}
	}
}
