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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.operators.python.AbstractOneInputPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.beam.BeamTablePythonStatelessFunctionRunner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for all stream operators to execute Python Stateless Functions.
 *
 * @param <IN>    Type of the input elements.
 * @param <OUT>   Type of the output elements.
 * @param <UDFIN> Type of the UDF input type.
 */
@Internal
public abstract class AbstractStatelessFunctionOperator<IN, OUT, UDFIN>
	extends AbstractOneInputPythonFunctionOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	/**
	 * The input logical type.
	 */
	protected final RowType inputType;

	/**
	 * The output logical type.
	 */
	protected final RowType outputType;

	/**
	 * The offsets of user-defined function inputs.
	 */
	protected final int[] userDefinedFunctionInputOffsets;

	/**
	 * The options used to configure the Python worker process.
	 */
	private final Map<String, String> jobOptions;

	/**
	 * The user-defined function input logical type.
	 */
	protected transient RowType userDefinedFunctionInputType;

	/**
	 * The user-defined function output logical type.
	 */
	protected transient RowType userDefinedFunctionOutputType;

	/**
	 * The queue holding the input elements for which the execution results have not been received.
	 */
	protected transient LinkedList<IN> forwardedInputQueue;

	/**
	 * Reusable InputStream used to holding the execution results to be deserialized.
	 */
	protected transient ByteArrayInputStreamWithPos bais;

	/**
	 * InputStream Wrapper.
	 */
	protected transient DataInputViewStreamWrapper baisWrapper;

	/**
	 * Reusable OutputStream used to holding the serialized input elements.
	 */
	protected transient ByteArrayOutputStreamWithPos baos;

	/**
	 * OutputStream Wrapper.
	 */
	protected transient DataOutputViewStreamWrapper baosWrapper;

	public AbstractStatelessFunctionOperator(
		Configuration config,
		RowType inputType,
		RowType outputType,
		int[] userDefinedFunctionInputOffsets) {
		super(config);
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.userDefinedFunctionInputOffsets = Preconditions.checkNotNull(userDefinedFunctionInputOffsets);
		this.jobOptions = buildJobOptions(config);
	}

	@Override
	public void open() throws Exception {
		forwardedInputQueue = new LinkedList<>();
		userDefinedFunctionInputType = new RowType(
			Arrays.stream(userDefinedFunctionInputOffsets)
				.mapToObj(i -> inputType.getFields().get(i))
				.collect(Collectors.toList()));
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);
		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);
		super.open();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		bufferInput(value);
		processElementInternal(value);
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws IOException {
		return new BeamTablePythonStatelessFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType,
			getFunctionUrn(),
			getUserDefinedFunctionsProto(),
			getInputOutputCoderUrn(),
			jobOptions,
			getFlinkMetricContainer());
	}

	protected FlinkFnApi.UserDefinedFunction getUserDefinedFunctionProto(PythonFunctionInfo pythonFunctionInfo) {
		FlinkFnApi.UserDefinedFunction.Builder builder = FlinkFnApi.UserDefinedFunction.newBuilder();
		builder.setPayload(ByteString.copyFrom(pythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
		for (Object input : pythonFunctionInfo.getInputs()) {
			FlinkFnApi.UserDefinedFunction.Input.Builder inputProto =
				FlinkFnApi.UserDefinedFunction.Input.newBuilder();
			if (input instanceof PythonFunctionInfo) {
				inputProto.setUdf(getUserDefinedFunctionProto((PythonFunctionInfo) input));
			} else if (input instanceof Integer) {
				inputProto.setInputOffset((Integer) input);
			} else {
				inputProto.setInputConstant(ByteString.copyFrom((byte[]) input));
			}
			builder.addInputs(inputProto);
		}
		return builder.build();
	}

	/**
	 * Buffers the specified input, it will be used to construct
	 * the operator result together with the user-defined function execution result.
	 */
	public abstract void bufferInput(IN input);

	public abstract UDFIN getFunctionInput(IN element);

	/**
	 * Gets the proto representation of the Python user-defined functions to be executed.
	 */
	public abstract FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto();

	public abstract String getInputOutputCoderUrn();

	public abstract String getFunctionUrn();

	public abstract void processElementInternal(IN value) throws Exception;

	private Map<String, String> buildJobOptions(Configuration config) {
		Map<String, String> jobOptions = new HashMap<>();
		if (config.containsKey("table.exec.timezone")) {
			jobOptions.put("table.exec.timezone", config.getString("table.exec.timezone", null));
		}
		return jobOptions;
	}

	/**
	 * The collector is used to convert a {@link Row} to a {@link CRow}.
	 */
	public static class StreamRecordCRowWrappingCollector implements Collector<Row> {

		private final Collector<StreamRecord<CRow>> out;
		private final CRow reuseCRow = new CRow();

		/**
		 * For Table API & SQL jobs, the timestamp field is not used.
		 */
		private final StreamRecord<CRow> reuseStreamRecord = new StreamRecord<>(reuseCRow);

		public StreamRecordCRowWrappingCollector(Collector<StreamRecord<CRow>> out) {
			this.out = out;
		}

		public void setChange(boolean change) {
			this.reuseCRow.change_$eq(change);
		}

		@Override
		public void collect(Row record) {
			reuseCRow.row_$eq(record);
			out.collect(reuseStreamRecord);
		}

		@Override
		public void close() {
			out.close();
		}
	}

	/**
	 * The collector is used to convert a {@link RowData} to a {@link StreamRecord}.
	 */
	public static class StreamRecordRowDataWrappingCollector implements Collector<RowData> {

		private final Collector<StreamRecord<RowData>> out;

		/**
		 * For Table API & SQL jobs, the timestamp field is not used.
		 */
		private final StreamRecord<RowData> reuseStreamRecord = new StreamRecord<>(null);

		public StreamRecordRowDataWrappingCollector(Collector<StreamRecord<RowData>> out) {
			this.out = out;
		}

		@Override
		public void collect(RowData record) {
			out.collect(reuseStreamRecord.replace(record));
		}

		@Override
		public void close() {
			out.close();
		}
	}
}
