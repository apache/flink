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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
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
	extends AbstractPythonFunctionOperator<IN, OUT> {

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
	protected transient LinkedBlockingQueue<IN> forwardedInputQueue;

	/**
	 * The queue holding the user-defined function execution results. The execution results
	 * are in the same order as the input elements.
	 */
	protected transient LinkedBlockingQueue<byte[]> userDefinedFunctionResultQueue;

	/**
	 * Reusable InputStream used to holding the execution results to be deserialized.
	 */
	protected transient ByteArrayInputStreamWithPos bais;

	/**
	 * InputStream Wrapper.
	 */
	protected transient DataInputViewStreamWrapper baisWrapper;

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
		forwardedInputQueue = new LinkedBlockingQueue<>();
		userDefinedFunctionResultQueue = new LinkedBlockingQueue<>();
		userDefinedFunctionInputType = new RowType(
			Arrays.stream(userDefinedFunctionInputOffsets)
				.mapToObj(i -> inputType.getFields().get(i))
				.collect(Collectors.toList()));
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);
		super.open();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		bufferInput(element.getValue());
		super.processElement(element);
		emitResults();
	}

	@Override
	public PythonFunctionRunner<IN> createPythonFunctionRunner() throws IOException {
		final FnDataReceiver<byte[]> userDefinedFunctionResultReceiver = input -> {
			// handover to queue, do not block the result receiver thread
			userDefinedFunctionResultQueue.put(input);
		};

		return new ProjectUdfInputPythonScalarFunctionRunner(
			createPythonFunctionRunner(
				userDefinedFunctionResultReceiver,
				createPythonEnvironmentManager(),
				jobOptions));
	}

	/**
	 * Buffers the specified input, it will be used to construct
	 * the operator result together with the user-defined function execution result.
	 */
	public abstract void bufferInput(IN input);

	public abstract UDFIN getFunctionInput(IN element);

	public abstract PythonFunctionRunner<UDFIN> createPythonFunctionRunner(
		FnDataReceiver<byte[]> resultReceiver,
		PythonEnvironmentManager pythonEnvironmentManager,
		Map<String, String> jobOptions);

	private Map<String, String> buildJobOptions(Configuration config) {
		Map<String, String> jobOptions = new HashMap<>();
		if (config.containsKey("table.exec.timezone")) {
			jobOptions.put("table.exec.timezone", config.getString("table.exec.timezone", null));
		}
		return jobOptions;
	}

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
			pythonFunctionRunner.processElement(getFunctionInput(element));
		}
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
	 * The collector is used to convert a {@link BaseRow} to a {@link StreamRecord}.
	 */
	public static class StreamRecordBaseRowWrappingCollector implements Collector<BaseRow> {

		private final Collector<StreamRecord<BaseRow>> out;

		/**
		 * For Table API & SQL jobs, the timestamp field is not used.
		 */
		private final StreamRecord<BaseRow> reuseStreamRecord = new StreamRecord<>(null);

		public StreamRecordBaseRowWrappingCollector(Collector<StreamRecord<BaseRow>> out) {
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
