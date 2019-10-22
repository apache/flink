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
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Base class for all stream operators to execute Python {@link ScalarFunction}s. It executes the Python
 * {@link ScalarFunction}s in separate Python execution environment.
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
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 * @param <UDFIN> Type of the UDF input type.
 * @param <UDFOUT> Type of the UDF input type.
 */
@Internal
public abstract class AbstractPythonScalarFunctionOperator<IN, OUT, UDFIN, UDFOUT>
		extends AbstractPythonFunctionOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	/**
	 * The Python {@link ScalarFunction}s to be executed.
	 */
	protected final PythonFunctionInfo[] scalarFunctions;

	/**
	 * The input DataType.
	 */
	protected final DataType inputType;

	/**
	 * The output DataType.
	 */
	protected final DataType outputType;

	/**
	 * The offsets of udf inputs.
	 */
	protected final int[] udfInputOffsets;

	/**
	 * The offset of the fields which should be forwarded.
	 */
	protected final int[] forwardedFields;

	/**
	 * The udf input DataType.
	 */
	protected transient DataType udfInputType;

	/**
	 * The udf output DataType.
	 */
	protected transient DataType udfOutputType;

	/**
	 * The queue holding the input elements for which the execution results have not been received.
	 */
	protected transient LinkedBlockingQueue<IN> forwardedInputQueue;

	/**
	 * The queue holding the user-defined function execution results. The execution results are in
	 * the same order as the input elements.
	 */
	protected transient LinkedBlockingQueue<UDFOUT> udfResultQueue;

	AbstractPythonScalarFunctionOperator(
		PythonFunctionInfo[] scalarFunctions,
		DataType inputType,
		DataType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.udfInputOffsets = Preconditions.checkNotNull(udfInputOffsets);
		this.forwardedFields = Preconditions.checkNotNull(forwardedFields);
	}

	@Override
	public void open() throws Exception {
		forwardedInputQueue = new LinkedBlockingQueue<>();
		udfResultQueue = new LinkedBlockingQueue<>();
		DataTypes.Field[] inputFields = new DataTypes.Field[udfInputOffsets.length];
		for (int i = 0; i < udfInputOffsets.length; i++) {
			int udfInputOffset = udfInputOffsets[i];
			String fieldName = ((RowType) inputType.getLogicalType()).getFieldNames().get(udfInputOffset);
			inputFields[i] = DataTypes.FIELD(
				fieldName, ((FieldsDataType) inputType).getFieldDataTypes().get(fieldName));
		}
		udfInputType = DataTypes.ROW(inputFields);

		DataTypes.Field[] outputFields =
			new DataTypes.Field[((FieldsDataType) outputType).getFieldDataTypes().size() - forwardedFields.length];
		for (int i = 0; i < outputFields.length; i++) {
			String fieldName = ((RowType) outputType.getLogicalType()).getFieldNames().get(forwardedFields.length + i);
			outputFields[i] = DataTypes.FIELD(
				fieldName, ((FieldsDataType) outputType).getFieldDataTypes().get(fieldName));
		}
		udfOutputType = DataTypes.ROW(outputFields);
		super.open();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		bufferInput(element.getValue());
		super.processElement(element);
		emitResults();
	}

	@Override
	public PythonFunctionRunner<IN> createPythonFunctionRunner() {
		final FnDataReceiver<UDFOUT> udfResultReceiver = input -> {
			// handover to queue, do not block the result receiver thread
			udfResultQueue.put(input);
		};

		return new ProjectUdfInputPythonScalarFunctionRunner(createPythonFunctionRunner(udfResultReceiver));
	}

	/**
	 * Buffers the specified input, it will be used to construct
	 * the operator result together with the udf execution result.
	 */
	public abstract void bufferInput(IN input);

	public abstract UDFIN getUdfInput(IN element);

	public abstract PythonFunctionRunner<UDFIN> createPythonFunctionRunner(FnDataReceiver<UDFOUT> resultReceiver);

	private class ProjectUdfInputPythonScalarFunctionRunner implements PythonFunctionRunner<IN> {

		private final PythonFunctionRunner<UDFIN> pythonFunctionRunner;

		ProjectUdfInputPythonScalarFunctionRunner(PythonFunctionRunner<UDFIN> pythonFunctionRunner) {
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
		public void processElement(IN element) throws Exception {
			pythonFunctionRunner.processElement(getUdfInput(element));
		}
	}
}
