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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.runtime.utils.PassThroughPythonTableFunctionRunner;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.Collection;
import java.util.Map;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.baserow;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.binaryrow;

/**
 * Tests for {@link BaseRowPythonTableFunctionOperator}.
 */
public class BaseRowPythonTableFunctionOperatorTest
	extends PythonTableFunctionOperatorTestBase<BaseRow, BaseRow, BaseRow> {

	private final BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(new TypeInformation[]{
		Types.STRING,
		Types.STRING,
		Types.LONG,
		Types.LONG
	});

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
	public AbstractPythonTableFunctionOperator<BaseRow, BaseRow, BaseRow> getTestOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		JoinRelType joinRelType) {
		return new BaseRowPassThroughPythonTableFunctionOperator(
			config, tableFunction, inputType, outputType, udfInputOffsets, joinRelType);
	}

	private static class BaseRowPassThroughPythonTableFunctionOperator extends BaseRowPythonTableFunctionOperator {

		BaseRowPassThroughPythonTableFunctionOperator(
			Configuration config,
			PythonFunctionInfo tableFunction,
			RowType inputType,
			RowType outputType,
			int[] udfInputOffsets,
			JoinRelType joinRelType) {
			super(config, tableFunction, inputType, outputType, udfInputOffsets, joinRelType);
		}

		@Override
		public PythonFunctionRunner<BaseRow> createPythonFunctionRunner(
			FnDataReceiver<byte[]> resultReceiver,
			PythonEnvironmentManager pythonEnvironmentManager,
			Map<String, String> jobOptions) {
			return new PassThroughPythonTableFunctionRunner<BaseRow>(resultReceiver) {
				@Override
				public BaseRow copy(BaseRow element) {
					BaseRow row = binaryrow(element.getLong(0));
					row.setHeader(element.getHeader());
					return row;
				}

				@Override
				@SuppressWarnings("unchecked")
				public TypeSerializer<BaseRow> getInputTypeSerializer() {
					return PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionInputType);
				}
			};
		}
	}
}
