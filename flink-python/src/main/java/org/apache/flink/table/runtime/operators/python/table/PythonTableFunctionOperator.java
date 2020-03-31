/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.table.PythonTableFunctionRunner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.calcite.rel.core.JoinRelType;

import java.io.IOException;
import java.util.Map;

/**
 * The Python {@link TableFunction} operator for the legacy planner.
 */
@Internal
public class PythonTableFunctionOperator extends AbstractPythonTableFunctionOperator<CRow, CRow, Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordCRowWrappingCollector cRowWrapper;

	/**
	 * The type serializer for the forwarded fields.
	 */
	private transient TypeSerializer<CRow> forwardedInputSerializer;

	/**
	 * The TypeSerializer for udtf execution results.
	 */
	private transient TypeSerializer<Row> udtfOutputTypeSerializer;

	public PythonTableFunctionOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udtfInputOffsets,
		JoinRelType joinType) {
		super(config, tableFunction, inputType, outputType, udtfInputOffsets, joinType);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open() throws Exception {
		super.open();
		this.cRowWrapper = new StreamRecordCRowWrappingCollector(output);
		CRowTypeInfo forwardedInputTypeInfo = new CRowTypeInfo(
			new RowTypeInfo(TypeConversions.fromDataTypeToLegacyInfo(
				TypeConversions.fromLogicalToDataType(inputType))));
		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getExecutionConfig());
		udtfOutputTypeSerializer = PythonTypeUtils.toFlinkTypeSerializer(userDefinedFunctionOutputType);
	}

	@Override
	public void emitResults() throws IOException {
		CRow input = null;
		byte[] rawUdtfResult;
		boolean lastIsFinishResult = true;
		while ((rawUdtfResult = userDefinedFunctionResultQueue.poll()) != null) {
			if (input == null) {
				input = forwardedInputQueue.poll();
			}
			boolean isFinishResult = isFinishResult(rawUdtfResult);
			if (isFinishResult && (!lastIsFinishResult || joinType == JoinRelType.INNER)) {
				input = forwardedInputQueue.poll();
			} else if (input != null) {
				if (!isFinishResult) {
					bais.setBuffer(rawUdtfResult, 0, rawUdtfResult.length);
					Row udtfResult = udtfOutputTypeSerializer.deserialize(baisWrapper);
					cRowWrapper.setChange(input.change());
					cRowWrapper.collect(Row.join(input.row(), udtfResult));
				} else {
					Row udtfResult = new Row(userDefinedFunctionOutputType.getFieldCount());
					for (int i = 0; i < udtfResult.getArity(); i++) {
						udtfResult.setField(0, null);
					}
					cRowWrapper.collect(Row.join(input.row(), udtfResult));
					input = forwardedInputQueue.poll();
				}
			}
			lastIsFinishResult = isFinishResult;
		}
	}

	@Override
	public void bufferInput(CRow input) {
		if (getExecutionConfig().isObjectReuseEnabled()) {
			input = forwardedInputSerializer.copy(input);
		}
		forwardedInputQueue.add(input);
	}

	@Override
	public Row getFunctionInput(CRow element) {
		return Row.project(element.row(), userDefinedFunctionInputOffsets);
	}

	@Override
	public PythonFunctionRunner<Row> createPythonFunctionRunner(
		FnDataReceiver<byte[]> resultReceiver,
		PythonEnvironmentManager pythonEnvironmentManager,
		Map<String, String> jobOptions) {
		return new PythonTableFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			tableFunction,
			pythonEnvironmentManager,
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType,
			jobOptions,
			getFlinkMetricContainer());
	}
}
