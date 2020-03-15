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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.Collection;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.baserow;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.binaryrow;

/**
 * Tests for {@link BaseRowPythonScalarFunctionOperator}.
 */
public class BaseRowPythonScalarFunctionOperatorTest
		extends PythonScalarFunctionOperatorTestBase<BaseRow, BaseRow, BaseRow, BaseRow> {

	private final BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(new TypeInformation[]{
		Types.STRING,
		Types.STRING,
		Types.LONG
	});

	@Override
	public AbstractPythonScalarFunctionOperator<BaseRow, BaseRow, BaseRow, BaseRow> getTestOperator(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		return new PassThroughPythonScalarFunctionOperator(
			config,
			scalarFunctions,
			inputType,
			outputType,
			udfInputOffsets,
			forwardedFields
		);
	}

	@Override
	public BaseRow newRow(boolean accumulateMsg, Object... fields) {
		if (accumulateMsg) {
			return baserow(fields);
		} else {
			return BaseRowUtil.setRetract(baserow(fields));
		}
	}

	@Override
	public void assertOutputEquals(String message, Collection<Object> expected, Collection<Object> actual) {
		assertor.assertOutputEquals(message, expected, actual);
	}

	@Override
	public StreamTableEnvironment createTableEnvironment(StreamExecutionEnvironment env) {
		return StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build());
	}

	private static class PassThroughPythonScalarFunctionOperator extends BaseRowPythonScalarFunctionOperator {

		PassThroughPythonScalarFunctionOperator(
			Configuration config,
			PythonFunctionInfo[] scalarFunctions,
			RowType inputType,
			RowType outputType,
			int[] udfInputOffsets,
			int[] forwardedFields) {
			super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
		}

		@Override
		public PythonFunctionRunner<BaseRow> createPythonFunctionRunner(
				FnDataReceiver<BaseRow> resultReceiver,
				PythonEnvironmentManager pythonEnvironmentManager) {
			return new PassThroughPythonFunctionRunner<BaseRow>(resultReceiver) {
				@Override
				public BaseRow copy(BaseRow element) {
					BaseRow row = binaryrow(element.getLong(0));
					row.setHeader(element.getHeader());
					return row;
				}
			};
		}
	}
}
