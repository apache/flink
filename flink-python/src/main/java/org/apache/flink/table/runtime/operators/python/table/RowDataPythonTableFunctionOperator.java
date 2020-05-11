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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.runners.python.table.RowDataPythonTableFunctionRunner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.calcite.rel.core.JoinRelType;

import java.io.IOException;
import java.util.Map;

/**
 * The Python {@link TableFunction} operator for the blink planner.
 */
@Internal
public class RowDataPythonTableFunctionOperator
	extends AbstractPythonTableFunctionOperator<RowData, RowData, RowData> {


	private static final long serialVersionUID = 1L;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordRowDataWrappingCollector rowDataWrapper;

	/**
	 * The JoinedRowData reused holding the execution result.
	 */
	private transient JoinedRowData reuseJoinedRow;

	/**
	 * The Projection which projects the udtf input fields from the input row.
	 */
	private transient Projection<RowData, BinaryRowData> udtfInputProjection;

	/**
	 * The TypeSerializer for udtf execution results.
	 */
	private transient TypeSerializer<RowData> udtfOutputTypeSerializer;

	/**
	 * The type serializer for the forwarded fields.
	 */
	private transient RowDataSerializer forwardedInputSerializer;

	public RowDataPythonTableFunctionOperator(
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
		rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
		reuseJoinedRow = new JoinedRowData();

		udtfInputProjection = createUdtfInputProjection();
		forwardedInputSerializer = new RowDataSerializer(this.getExecutionConfig(), inputType);
		udtfOutputTypeSerializer = PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionOutputType);
	}

	@Override
	public void bufferInput(RowData input) {
		// always copy the input RowData
		RowData forwardedFields = forwardedInputSerializer.copy(input);
		forwardedFields.setRowKind(input.getRowKind());
		forwardedInputQueue.add(forwardedFields);
	}

	@Override
	public RowData getFunctionInput(RowData element) {
		return udtfInputProjection.apply(element);
	}

	@Override
	public PythonFunctionRunner<RowData> createPythonFunctionRunner(
		FnDataReceiver<byte[]> resultReceiver,
		PythonEnvironmentManager pythonEnvironmentManager,
		Map<String, String> jobOptions) {
		return new RowDataPythonTableFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			tableFunction,
			pythonEnvironmentManager,
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType,
			jobOptions,
			getFlinkMetricContainer());
	}

	private Projection<RowData, BinaryRowData> createUdtfInputProjection() {
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"UdtfInputProjection",
			inputType,
			userDefinedFunctionInputType,
			userDefinedFunctionInputOffsets);
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}

	@Override
	public void emitResults() throws IOException {
		RowData input = null;
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
					reuseJoinedRow.setRowKind(input.getRowKind());
					bais.setBuffer(rawUdtfResult, 0, rawUdtfResult.length);
					RowData udtfResult = udtfOutputTypeSerializer.deserialize(baisWrapper);
					rowDataWrapper.collect(reuseJoinedRow.replace(input, udtfResult));
				} else {
					GenericRowData udtfResult = new GenericRowData(userDefinedFunctionOutputType.getFieldCount());
					for (int i = 0; i < udtfResult.getArity(); i++) {
						udtfResult.setField(i, null);
					}
					rowDataWrapper.collect(reuseJoinedRow.replace(input, udtfResult));
					input = forwardedInputQueue.poll();
				}
			}
			lastIsFinishResult = isFinishResult;
		}
	}
}
