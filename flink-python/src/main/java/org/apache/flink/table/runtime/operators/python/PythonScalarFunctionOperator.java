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

package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.PythonScalarFunctionRunner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;
import java.util.Arrays;

/**
 * The Python {@link ScalarFunction} operator for the legacy planner.
 */
@Internal
public class PythonScalarFunctionOperator extends AbstractPythonScalarFunctionOperator<CRow, CRow, Row> {

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
	 * The TypeSerializer for udf execution results.
	 */
	private transient TypeSerializer<Row> udfOutputTypeSerializer;

	public PythonScalarFunctionOperator(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open() throws Exception {
		super.open();
		this.cRowWrapper = new StreamRecordCRowWrappingCollector(output);

		CRowTypeInfo forwardedInputTypeInfo = new CRowTypeInfo(new RowTypeInfo(
			Arrays.stream(forwardedFields)
				.mapToObj(i -> inputType.getFields().get(i))
				.map(RowType.RowField::getType)
				.map(TypeConversions::fromLogicalToDataType)
				.map(TypeConversions::fromDataTypeToLegacyInfo)
				.toArray(TypeInformation[]::new)));
		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getExecutionConfig());
		udfOutputTypeSerializer = PythonTypeUtils.toFlinkTypeSerializer(userDefinedFunctionOutputType);
	}

	@Override
	public void bufferInput(CRow input) {
		CRow forwardedFieldsRow = new CRow(Row.project(input.row(), forwardedFields), input.change());
		if (getExecutionConfig().isObjectReuseEnabled()) {
			forwardedFieldsRow = forwardedInputSerializer.copy(forwardedFieldsRow);
		}
		forwardedInputQueue.add(forwardedFieldsRow);
	}

	@Override
	public Row getFunctionInput(CRow element) {
		return Row.project(element.row(), userDefinedFunctionInputOffsets);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResults() throws IOException {
		byte[] rawUdfResult;
		while ((rawUdfResult = userDefinedFunctionResultQueue.poll()) != null) {
			CRow input = forwardedInputQueue.poll();
			cRowWrapper.setChange(input.change());
			bais.setBuffer(rawUdfResult, 0, rawUdfResult.length);
			Row udfResult = udfOutputTypeSerializer.deserialize(baisWrapper);
			cRowWrapper.collect(Row.join(input.row(), udfResult));
		}
	}

	@Override
	public PythonFunctionRunner<Row> createPythonFunctionRunner(
			FnDataReceiver<byte[]> resultReceiver,
			PythonEnvironmentManager pythonEnvironmentManager) {
		return new PythonScalarFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			scalarFunctions,
			pythonEnvironmentManager,
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType);
	}
}
