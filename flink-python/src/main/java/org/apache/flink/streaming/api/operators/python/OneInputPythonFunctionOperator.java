/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.getUserDefinedDataStreamFunctionProto;

/**
 * {@link OneInputPythonFunctionOperator} is responsible for launching beam runner which will start
 * a python harness to execute user defined python function.
 *
 * <p>The operator will buffer the timestamp of input elements in a queue, and set into the produced
 * output element.
 */
@Internal
public abstract class OneInputPythonFunctionOperator<IN, OUT, UDFIN, UDFOUT> extends AbstractOneInputPythonFunctionOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	protected static final String DATA_STREAM_STATELESS_FUNCTION_URN =
		"flink:transform:datastream_stateless_function:v1";

	/**
	 * The options used to configure the Python worker process.
	 */
	protected final Map<String, String> jobOptions;

	/**
	 * The TypeInformation of runner input data.
	 */
	private final TypeInformation<UDFIN> runnerInputTypeInfo;

	/**
	 * The TypeInformation of runner output data.
	 */
	final TypeInformation<UDFOUT> runnerOutputTypeInfo;

	/**
	 * The serialized python function to be executed.
	 */
	private final DataStreamPythonFunctionInfo pythonFunctionInfo;

	/**
	 * The TypeSerializer of python worker input data.
	 */
	transient TypeSerializer<UDFIN> runnerInputTypeSerializer;

	/**
	 * The TypeSerializer of python worker output data.
	 */
	transient TypeSerializer<UDFOUT> runnerOutputTypeSerializer;

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

	protected transient TimestampedCollector<OUT> collector;

	transient LinkedList<Long> bufferedTimestamp;

	public OneInputPythonFunctionOperator(
		Configuration config,
		TypeInformation<UDFIN> runnerInputTypeInfo,
		TypeInformation<UDFOUT> runnerOutputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config);
		this.jobOptions = config.toMap();
		this.runnerInputTypeInfo = runnerInputTypeInfo;
		this.runnerOutputTypeInfo = runnerOutputTypeInfo;
		this.pythonFunctionInfo = pythonFunctionInfo;
	}

	@Override
	public void open() throws Exception {
		super.open();
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);
		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);

		this.bufferedTimestamp = new LinkedList<>();
		this.runnerInputTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerInputTypeInfo);
		this.runnerOutputTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerOutputTypeInfo);

		this.collector = new TimestampedCollector<>(output);
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
		return new BeamDataStreamPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			runnerInputTypeInfo,
			runnerOutputTypeInfo,
			getFunctionUrn(),
			getUserDefinedDataStreamFunctionProto(pythonFunctionInfo, getRuntimeContext(), getInternalParameters()),
			getCoderUrn(),
			jobOptions,
			getFlinkMetricContainer(),
			null,
			null,
			getContainingTask().getEnvironment().getMemoryManager(),
			getOperatorConfig().getManagedMemoryFractionOperatorUseCaseOfSlot(
				ManagedMemoryUseCase.PYTHON,
				getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration(),
				getContainingTask().getEnvironment().getUserCodeClassLoader().asClassLoader())
		);
	}

	@Override
	public PythonEnv getPythonEnv() {
		return pythonFunctionInfo.getPythonFunction().getPythonEnv();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		bufferedTimestamp.offer(element.getTimestamp());
		runnerInputTypeSerializer.serialize((UDFIN) element.getValue(), baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	public String getFunctionUrn() {
		return DATA_STREAM_STATELESS_FUNCTION_URN;
	}

	public Map<String, String> getInternalParameters() {
		return Collections.EMPTY_MAP;
	}

	public abstract String getCoderUrn();
}
