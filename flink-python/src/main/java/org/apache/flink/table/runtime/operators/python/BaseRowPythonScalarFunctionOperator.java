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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.runners.python.BaseRowPythonScalarFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The Python {@link ScalarFunction} operator for the blink planner.
 */
@Internal
public class BaseRowPythonScalarFunctionOperator
		extends AbstractPythonScalarFunctionOperator<BaseRow, BaseRow, BaseRow, BaseRow> {

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
	 * The Projection which projects the forwarded fields from the input row.
	 */
	private transient Projection<BaseRow, BinaryRow> forwardedFieldProjection;

	/**
	 * The Projection which projects the udf input fields from the input row.
	 */
	private transient Projection<BaseRow, BinaryRow> udfInputProjection;

	public BaseRowPythonScalarFunctionOperator(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
	}

	@Override
	public void open() throws Exception {
		super.open();
		baseRowWrapper = new StreamRecordBaseRowWrappingCollector(output);
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
	public BaseRow getUdfInput(BaseRow element) {
		return udfInputProjection.apply(element);
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
	public PythonFunctionRunner<BaseRow> createPythonFunctionRunner(
			FnDataReceiver<BaseRow> resultReceiver,
			PythonEnvironmentManager pythonEnvironmentManager) {
		return new BaseRowPythonScalarFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			scalarFunctions,
			pythonEnvironmentManager,
			udfInputType,
			udfOutputType);
	}

	private Projection<BaseRow, BinaryRow> createUdfInputProjection() {
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"UdfInputProjection",
			inputType,
			udfInputType,
			udfInputOffsets);
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}

	private Projection<BaseRow, BinaryRow> createForwardedFieldProjection() {
		final RowType forwardedFieldType = new RowType(
			Arrays.stream(forwardedFields)
				.mapToObj(i -> inputType.getFields().get(i))
				.collect(Collectors.toList()));
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"ForwardedFieldProjection",
			inputType,
			forwardedFieldType,
			forwardedFields);
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}

	/**
	 * The collector is used to convert a {@link BaseRow} to a {@link StreamRecord}.
	 */
	private static class StreamRecordBaseRowWrappingCollector implements Collector<BaseRow> {

		private final Collector<StreamRecord<BaseRow>> out;

		/**
		 * For Table API & SQL jobs, the timestamp field is not used.
		 */
		private final StreamRecord<BaseRow> reuseStreamRecord = new StreamRecord<>(null);

		StreamRecordBaseRowWrappingCollector(Collector<StreamRecord<BaseRow>> out) {
			this.out = out;
		}

		@Override
		public void collect(BaseRow record) {
			out.collect(reuseStreamRecord.replace(record));
		}

		@Override
		public void close() {
			out.close();
		}
	}
}
