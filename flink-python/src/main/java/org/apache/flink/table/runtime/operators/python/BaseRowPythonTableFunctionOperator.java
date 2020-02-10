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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.runners.python.BaseRowPythonTableFunctionRunner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;

/**
 * The Python {@link TableFunction} operator for the blink planner.
 */
@Internal
public class BaseRowPythonTableFunctionOperator
	extends AbstractPythonTableFunctionOperator<BaseRow, BaseRow, BaseRow> {


	private static final long serialVersionUID = 1L;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordBaseRowWrappingCollector baseRowWrapper;

	/**
	 * The JoinedRow reused holding the execution result.
	 */
	private transient JoinedRow reuseJoinedRow;

	/**
	 * The Projection which projects the udtf input fields from the input row.
	 */
	private transient Projection<BaseRow, BinaryRow> udtfInputProjection;

	/**
	 * The TypeSerializer for udtf execution results.
	 */
	private transient TypeSerializer<BaseRow> udtfOutputTypeSerializer;

	public BaseRowPythonTableFunctionOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udtfInputOffsets) {
		super(config, tableFunction, inputType, outputType, udtfInputOffsets);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open() throws Exception {
		super.open();
		baseRowWrapper = new StreamRecordBaseRowWrappingCollector(output);
		reuseJoinedRow = new JoinedRow();

		udtfInputProjection = createUdtfInputProjection();
		udtfOutputTypeSerializer = PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionOutputType);
	}

	@Override
	public void bufferInput(BaseRow input) {
		forwardedInputQueue.add(input);
	}

	@Override
	public BaseRow getUdfInput(BaseRow element) {
		return udtfInputProjection.apply(element);
	}

	@Override
	public PythonFunctionRunner<BaseRow> createPythonFunctionRunner(
		FnDataReceiver<byte[]> resultReceiver,
		PythonEnvironmentManager pythonEnvironmentManager) {
		return new BaseRowPythonTableFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			tableFunction,
			pythonEnvironmentManager,
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType);
	}

	private Projection<BaseRow, BinaryRow> createUdtfInputProjection() {
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
		BaseRow input = null;
		byte[] rawUdtfResult;
		while ((rawUdtfResult = userDefinedFunctionResultQueue.poll()) != null) {
			if (input == null) {
				input = forwardedInputQueue.poll();
			}
			boolean isFinishResult = isFinishResult(rawUdtfResult);
			if (isFinishResult) {
				input = forwardedInputQueue.poll();
			}
			if (input != null && !isFinishResult) {
				reuseJoinedRow.setHeader(input.getHeader());
				bais.setBuffer(rawUdtfResult, 0, rawUdtfResult.length);
				BaseRow udtfResult = udtfOutputTypeSerializer.deserialize(baisWrapper);
				baseRowWrapper.collect(reuseJoinedRow.replace(input, udtfResult));
			}
		}
	}
}
