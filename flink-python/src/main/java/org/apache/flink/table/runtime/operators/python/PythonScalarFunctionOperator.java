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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonScalarFunctionRunner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * The {@link PythonScalarFunctionOperator} is responsible for executing Python {@link ScalarFunction}s.
 * It executes the Python {@link ScalarFunction}s in separate Python execution environment.
 *
 * <p>The inputs are assumed as the following format:
 * {{{
 *   +------------------+--------------+
 *   | forwarded fields | extra fields |
 *   +------------------+--------------+
 * }}}.
 *
 * <p>The Python UDFs may take input columns directly from the input row or the execution result of Java UDFs:
 * 1) The input columns from the input row can be referred from the 'forwarded fields';
 * 2) The Java UDFs will be computed and the execution results can be referred from the 'extra fields'.
 *
 * <p>The outputs will be as the following format:
 * {{{
 *   +------------------+-------------------------+
 *   | forwarded fields | scalar function results |
 *   +------------------+-------------------------+
 * }}}.
 */
@Internal
public class PythonScalarFunctionOperator extends AbstractPythonScalarFunctionOperator<CRow, CRow> {

	private static final long serialVersionUID = 1L;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordCRowWrappingCollector cRowWrapper;

	/**
	 * The queue holding the input elements for which the execution results have not been received.
	 */
	private transient LinkedBlockingQueue<CRow> forwardedInputQueue;

	/**
	 * The queue holding the user-defined function execution results. The execution results are in
	 * the same order as the input elements.
	 */
	private transient LinkedBlockingQueue<Row> udfResultQueue;

	/**
	 * The type serializer for the forwarded fields.
	 */
	private transient TypeSerializer<CRow> forwardedInputSerializer;

	public PythonScalarFunctionOperator(
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int forwardedFieldCnt) {
		super(scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFieldCnt);
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.cRowWrapper = new StreamRecordCRowWrappingCollector(output);
		this.forwardedInputQueue = new LinkedBlockingQueue<>();
		this.udfResultQueue = new LinkedBlockingQueue<>();

		CRowTypeInfo forwardedInputTypeInfo = new CRowTypeInfo(new RowTypeInfo(
			getInputType().getFields().stream()
				.limit(getForwardedFieldCnt())
				.map(RowType.RowField::getType)
				.map(TypeConversions::fromLogicalToDataType)
				.map(TypeConversions::fromDataTypeToLegacyInfo)
				.toArray(TypeInformation[]::new)));
		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getExecutionConfig());
	}

	@Override
	public void bufferInput(CRow input) {
		CRow forwardedFields = new CRow(getForwardedRow(input.row()), input.change());
		if (getExecutionConfig().isObjectReuseEnabled()) {
			forwardedFields = forwardedInputSerializer.copy(forwardedFields);
		}
		forwardedInputQueue.add(forwardedFields);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResults() {
		Row udfResult;
		while ((udfResult = udfResultQueue.poll()) != null) {
			CRow input = forwardedInputQueue.poll();
			cRowWrapper.setChange(input.change());
			cRowWrapper.collect(Row.join(input.row(), udfResult));
		}
	}

	@Override
	public PythonFunctionRunner<CRow> createPythonFunctionRunner() {
		final FnDataReceiver<Row> udfResultReceiver = input -> {
			// handover to queue, do not block the result receiver thread
			udfResultQueue.put(input);
		};

		return new PythonScalarFunctionRunnerWrapper(createPythonFunctionRunner(udfResultReceiver));
	}

	@VisibleForTesting
	PythonFunctionRunner<Row> createPythonFunctionRunner(
		FnDataReceiver<Row> resultReceiver) {
		return new PythonScalarFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			getScalarFunctions(),
			getScalarFunctions()[0].getPythonFunction().getPythonEnv(),
			getUdfInputType(),
			getUdfOutputType(),
			getContainingTask().getEnvironment().getTaskManagerInfo().getTmpDirectories()[0]);
	}

	private Row getForwardedRow(Row input) {
		int[] inputs = new int[getForwardedFieldCnt()];
		for (int i = 0; i < inputs.length; i++) {
			inputs[i] = i;
		}
		return Row.project(input, inputs);
	}

	private class PythonScalarFunctionRunnerWrapper implements PythonFunctionRunner<CRow> {

		private final PythonFunctionRunner<Row> pythonFunctionRunner;

		PythonScalarFunctionRunnerWrapper(PythonFunctionRunner<Row> pythonFunctionRunner) {
			this.pythonFunctionRunner = pythonFunctionRunner;
		}

		@Override
		public void open() throws Exception {
			pythonFunctionRunner.open();
		}

		@Override
		public void close() throws Exception {
			pythonFunctionRunner.close();
		}

		@Override
		public void startBundle() throws Exception {
			pythonFunctionRunner.startBundle();
		}

		@Override
		public void finishBundle() throws Exception {
			pythonFunctionRunner.finishBundle();
		}

		@Override
		public void processElement(CRow element) throws Exception {
			pythonFunctionRunner.processElement(Row.project(element.row(), getUdfInputOffsets()));
		}
	}
}
