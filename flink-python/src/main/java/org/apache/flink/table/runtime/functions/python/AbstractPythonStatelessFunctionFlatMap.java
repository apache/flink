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

package org.apache.flink.table.runtime.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.runtime.runners.python.beam.BeamTableStatelessPythonFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Base Python stateless {@link RichFlatMapFunction} used to invoke Python stateless functions for the
 * old planner.
 */
@Internal
public abstract class AbstractPythonStatelessFunctionFlatMap
	extends RichFlatMapFunction<Row, Row> implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractPythonStatelessFunctionFlatMap.class);

	/**
	 * The python config.
	 */
	private final PythonConfig config;

	/**
	 * The offsets of user-defined function inputs.
	 */
	private final int[] userDefinedFunctionInputOffsets;

	/**
	 * The input logical type.
	 */
	protected final RowType inputType;

	/**
	 * The output logical type.
	 */
	protected final RowType outputType;

	/**
	 * The options used to configure the Python worker process.
	 */
	protected final Map<String, String> jobOptions;

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
	protected transient LinkedBlockingQueue<Row> forwardedInputQueue;

	/**
	 * Max number of elements to include in a bundle.
	 */
	private transient int maxBundleSize;

	/**
	 * Number of processed elements in the current bundle.
	 */
	private transient int elementCount;

	/**
	 * OutputStream Wrapper.
	 */
	transient DataOutputViewStreamWrapper baosWrapper;

	/**
	 * The collector used to collect records.
	 */
	protected transient Collector<Row> resultCollector;

	/**
	 * The {@link PythonFunctionRunner} which is responsible for Python user-defined function execution.
	 */
	protected transient PythonFunctionRunner pythonFunctionRunner;

	/**
	 * Reusable InputStream used to holding the execution results to be deserialized.
	 */
	protected transient ByteArrayInputStreamWithPos bais;

	/**
	 * InputStream Wrapper.
	 */
	protected transient DataInputViewStreamWrapper baisWrapper;

	/**
	 * The type serializer for the forwarded fields.
	 */
	protected transient TypeSerializer<Row> forwardedInputSerializer;

	/**
	 * Reusable OutputStream used to holding the serialized input elements.
	 */
	protected transient ByteArrayOutputStreamWithPos baos;

	public AbstractPythonStatelessFunctionFlatMap(
		Configuration config,
		RowType inputType,
		RowType outputType,
		int[] userDefinedFunctionInputOffsets) {
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.userDefinedFunctionInputOffsets = Preconditions.checkNotNull(userDefinedFunctionInputOffsets);
		this.config = new PythonConfig(Preconditions.checkNotNull(config));
		this.jobOptions = buildJobOptions(config);
	}

	protected PythonConfig getPythonConfig() {
		return config;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		this.elementCount = 0;
		this.maxBundleSize = config.getMaxBundleSize();
		if (this.maxBundleSize <= 0) {
			this.maxBundleSize = PythonOptions.MAX_BUNDLE_SIZE.defaultValue();
			LOG.error("Invalid value for the maximum bundle size. Using default value of " +
				this.maxBundleSize + '.');
		} else {
			LOG.info("The maximum bundle size is configured to {}.", this.maxBundleSize);
		}

		if (config.getMaxBundleTimeMills() != PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue()) {
			LOG.info("Maximum bundle time takes no effect in old planner under batch mode. " +
				"Config maximum bundle size instead! " +
				"Under batch mode, bundle size should be enough to control both throughput and latency.");
		}
		forwardedInputQueue = new LinkedBlockingQueue<>();
		userDefinedFunctionInputType = new RowType(
			Arrays.stream(userDefinedFunctionInputOffsets)
				.mapToObj(i -> inputType.getFields().get(i))
				.collect(Collectors.toList()));

		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);

		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);

		userDefinedFunctionOutputType = new RowType(outputType.getFields().subList(getForwardedFieldsCount(), outputType.getFieldCount()));

		this.pythonFunctionRunner = createPythonFunctionRunner();
		this.pythonFunctionRunner.open(config);
	}

	@Override
	public void flatMap(Row value, Collector<Row> out) throws Exception {
		this.resultCollector = out;
		bufferInput(value);
		processElementInternal(value);
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeInformation<Row> getProducedType() {
		return (TypeInformation<Row>) LegacyTypeInfoDataTypeConverter
			.toLegacyTypeInfo(LogicalTypeDataTypeConverter.toDataType(outputType));
	}

	@Override
	public void close() throws Exception {
		try {
			invokeFinishBundle();

			if (pythonFunctionRunner != null) {
				pythonFunctionRunner.close();
				pythonFunctionRunner = null;
			}
		} finally {
			super.close();
		}
	}

	/**
	 * Returns the {@link PythonEnv} used to create PythonEnvironmentManager..
	 */
	public abstract PythonEnv getPythonEnv();

	public abstract void bufferInput(Row input);

	public abstract void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception;

	public abstract int getForwardedFieldsCount();

	/**
	 * Gets the proto representation of the Python user-defined functions to be executed.
	 */
	public abstract FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto();

	public abstract String getInputOutputCoderUrn();

	public abstract String getFunctionUrn();

	public abstract void processElementInternal(Row value) throws Exception;

	/**
	 * Checks whether to invoke finishBundle by elements count. Called in flatMap.
	 */
	protected void checkInvokeFinishBundleByCount() throws Exception {
		elementCount++;
		if (elementCount >= maxBundleSize) {
			invokeFinishBundle();
		}
	}

	protected PythonEnvironmentManager createPythonEnvironmentManager() throws IOException {
		PythonDependencyInfo dependencyInfo = PythonDependencyInfo.create(
			config, getRuntimeContext().getDistributedCache());
		PythonEnv pythonEnv = getPythonEnv();
		if (pythonEnv.getExecType() == PythonEnv.ExecType.PROCESS) {
			return new ProcessPythonEnvironmentManager(
				dependencyInfo,
				ConfigurationUtils.splitPaths(System.getProperty("java.io.tmpdir")),
				System.getenv());
		} else {
			throw new UnsupportedOperationException(String.format(
				"Execution type '%s' is not supported.", pythonEnv.getExecType()));
		}
	}

	protected FlinkMetricContainer getFlinkMetricContainer() {
		return this.config.isMetricEnabled() ?
			new FlinkMetricContainer(getRuntimeContext().getMetricGroup()) : null;
	}

	protected Row getFunctionInput(Row element) {
		return Row.project(element, userDefinedFunctionInputOffsets);
	}

	private void emitResults() throws Exception {
		Tuple2<byte[], Integer> resultTuple;
		while ((resultTuple = pythonFunctionRunner.pollResult()) != null) {
			emitResult(resultTuple);
		}
	}

	protected void invokeFinishBundle() throws Exception {
		if (elementCount > 0) {
			pythonFunctionRunner.flush();
			elementCount = 0;
			emitResults();
		}
	}

	private PythonFunctionRunner createPythonFunctionRunner() throws IOException {
		return new BeamTableStatelessPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType,
			getFunctionUrn(),
			getUserDefinedFunctionsProto(),
			getInputOutputCoderUrn(),
			jobOptions,
			getFlinkMetricContainer(),
			null,
			0.0);
	}

	private Map<String, String> buildJobOptions(Configuration config) {
		Map<String, String> jobOptions = new HashMap<>();
		if (config.containsKey("table.exec.timezone")) {
			jobOptions.put("table.exec.timezone", config.getString("table.exec.timezone", null));
		}
		return jobOptions;
	}
}
