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
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.Collection;
import java.util.Queue;

/**
 * Tests for {@link PythonScalarFunctionOperator}.
 */
public class PythonScalarFunctionOperatorTest extends PythonScalarFunctionOperatorTestBase<CRow, CRow, Row, Row> {

	@Override
	public AbstractPythonScalarFunctionOperator<CRow, CRow, Row, Row> getTestOperator(
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		return new PassThroughPythonScalarFunctionOperator(
			scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
	}

	@Override
	public CRow newRow(boolean accumulateMsg, Object... fields) {
		return new CRow(Row.of(fields), accumulateMsg);
	}

	@Override
	public void assertOutputEquals(String message, Collection<Object> expected, Collection<Object> actual) {
		TestHarnessUtil.assertOutputEquals(message, (Queue<Object>) expected, (Queue<Object>) actual);
	}

	@Override
	public StreamTableEnvironment createTableEnvironment(StreamExecutionEnvironment env) {
		return StreamTableEnvironment.create(env);
	}

	private static class PassThroughPythonScalarFunctionOperator extends PythonScalarFunctionOperator {

		PassThroughPythonScalarFunctionOperator(
			PythonFunctionInfo[] scalarFunctions,
			RowType inputType,
			RowType outputType,
			int[] udfInputOffsets,
			int[] forwardedFields) {
			super(scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
		}

		@Override
		public PythonFunctionRunner<Row> createPythonFunctionRunner(
				FnDataReceiver<Row> resultReceiver,
				PythonEnvironmentManager pythonEnvironmentManager) {
			return new PassThroughPythonFunctionRunner<Row>(resultReceiver) {
				@Override
				public Row copy(Row element) {
					return Row.copy(element);
				}
			};
		}
	}
}
