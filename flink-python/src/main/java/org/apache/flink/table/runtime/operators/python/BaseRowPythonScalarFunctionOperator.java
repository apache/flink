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
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.BaseRowPythonScalarFunctionRunner;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * The {@link BaseRowPythonScalarFunctionOperator} is responsible for executing Python {@link ScalarFunction}s.
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
public class BaseRowPythonScalarFunctionOperator extends AbstractPythonScalarFunctionOperator<BaseRow, BaseRow> {

	private static final long serialVersionUID = 1L;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordBaseRowWrappingCollector baseRowWrapper;

	/**
	 * The queue holding the input elements for which the execution results have not been received.
	 */
	private transient LinkedBlockingQueue<BaseRow> forwardedInputQueue;

	/**
	 * The queue holding the user-defined function execution results. The execution results are in
	 * the same order as the input elements.
	 */
	private transient LinkedBlockingQueue<BaseRow> udfResultQueue;

	/**
	 * The JoinedRow reused holding the execution result.
	 */
	private transient JoinedRow reuseJoinedRow;

	/**
	 * The Projection which projects the forwarded fields from the input row.
	 */
	private transient Projection<BaseRow, BinaryRow> forwardedFieldProjection;

	/**
	 * The Projection which projects the udf input fields from the input row.
	 */
	private transient Projection<BaseRow, BinaryRow> udfInputProjection;

	public BaseRowPythonScalarFunctionOperator(
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
		baseRowWrapper = new StreamRecordBaseRowWrappingCollector(output);
		forwardedInputQueue = new LinkedBlockingQueue<>();
		udfResultQueue = new LinkedBlockingQueue<>();
		reuseJoinedRow = new JoinedRow();

		udfInputProjection = createUdfInputProjection();
		forwardedFieldProjection = createForwardedFieldProjection();
	}

	@Override
	public void bufferInput(BaseRow input) {
		// always copy the projection result as the generated Projection reuses the projection result
		BaseRow forwardedFields = forwardedFieldProjection.apply(input).copy();
		forwardedFields.setHeader(input.getHeader());
		forwardedInputQueue.add(forwardedFields);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResults() {
		BaseRow udfResult;
		while ((udfResult = udfResultQueue.poll()) != null) {
			BaseRow input = forwardedInputQueue.poll();
			reuseJoinedRow.setHeader(input.getHeader());
			baseRowWrapper.collect(reuseJoinedRow.replace(input, udfResult));
		}
	}

	@Override
	public PythonFunctionRunner<BaseRow> createPythonFunctionRunner() {
		final FnDataReceiver<BaseRow> udfResultReceiver = input -> {
			// handover to queue, do not block the result receiver thread
			udfResultQueue.put(input);
		};

		return new PythonScalarFunctionRunnerWrapper(createPythonFunctionRunner(udfResultReceiver));
	}

	@VisibleForTesting
	PythonFunctionRunner<BaseRow> createPythonFunctionRunner(
		FnDataReceiver<BaseRow> resultReceiver) {
		return new BaseRowPythonScalarFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			getScalarFunctions(),
			getScalarFunctions()[0].getPythonFunction().getPythonEnv(),
			getUdfInputType(),
			getUdfOutputType(),
			getContainingTask().getEnvironment().getTaskManagerInfo().getTmpDirectories()[0]);
	}

	private Projection<BaseRow, BinaryRow> createUdfInputProjection() {
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"UdfInputProjection",
			getInputType(),
			getUdfInputType(),
			getUdfInputOffsets());
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}

	private Projection<BaseRow, BinaryRow> createForwardedFieldProjection() {
		final int[] fields = new int[getForwardedFieldCnt()];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = i;
		}

		final RowType forwardedFieldType = new RowType(getInputType().getFields().subList(0, getForwardedFieldCnt()));
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"ForwardedFieldProjection",
			getInputType(),
			forwardedFieldType,
			fields);
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}

	private class PythonScalarFunctionRunnerWrapper implements PythonFunctionRunner<BaseRow> {

		private final PythonFunctionRunner<BaseRow> pythonFunctionRunner;

		PythonScalarFunctionRunnerWrapper(PythonFunctionRunner<BaseRow> pythonFunctionRunner) {
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
		public void processElement(BaseRow element) throws Exception {
			// always copy the projection result as the generated Projection reuses the projection result
			pythonFunctionRunner.processElement(udfInputProjection.apply(element).copy());
		}
	}
}
