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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.getUserDefinedDataStreamFunctionProto;

/**
 * {@link StatelessOneInputPythonFunctionOperator} is responsible for launching beam runner which will start a python
 * harness to execute user defined python function.
 */
@Internal
public class StatelessOneInputPythonFunctionOperator<IN, OUT>
	extends AbstractOneInputPythonFunctionOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	protected static final String DATA_STREAM_STATELESS_PYTHON_FUNCTION_URN =
		"flink:transform:datastream_stateless_function:v1";
	protected static final String DATA_STREAM_MAP_FUNCTION_CODER_URN = "flink:coder:map:v1";
	protected static final String DATA_STREAM_FLAT_MAP_FUNCTION_CODER_URN = "flink:coder:flat_map:v1";


	protected final DataStreamPythonFunctionInfo pythonFunctionInfo;

	protected final TypeInformation<IN> inputTypeInfo;

	protected final TypeInformation<OUT> outputTypeInfo;

	protected final Map<String, String> jobOptions;

	protected transient TypeSerializer<IN> inputTypeSerializer;

	protected transient TypeSerializer<OUT> outputTypeSerializer;

	protected transient ByteArrayInputStreamWithPos bais;

	protected transient DataInputViewStreamWrapper baisWrapper;

	protected transient ByteArrayOutputStreamWithPos baos;

	protected transient DataOutputViewStreamWrapper baosWrapper;

	protected transient StreamRecordCollector streamRecordCollector;

	public StatelessOneInputPythonFunctionOperator(
		Configuration config,
		TypeInformation<IN> inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config);
		this.pythonFunctionInfo = pythonFunctionInfo;
		jobOptions = config.toMap();
		this.inputTypeInfo = inputTypeInfo;
		this.outputTypeInfo = outputTypeInfo;
	}

	@Override
	public void open() throws Exception {
		super.open();
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);

		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);

		this.inputTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(inputTypeInfo);
		this.outputTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(outputTypeInfo);

		this.streamRecordCollector = new StreamRecordCollector(output);
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {

		String coderUrn = null;
		int functionType = this.pythonFunctionInfo.getFunctionType();
		if (functionType == FlinkFnApi.UserDefinedDataStreamFunction.FunctionType.MAP.getNumber()) {
			coderUrn = DATA_STREAM_MAP_FUNCTION_CODER_URN;
		} else if (functionType == FlinkFnApi.UserDefinedDataStreamFunction.FunctionType.FLAT_MAP.getNumber()) {
			coderUrn = DATA_STREAM_FLAT_MAP_FUNCTION_CODER_URN;
		}

		return new BeamDataStreamPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			inputTypeInfo,
			outputTypeInfo,
			DATA_STREAM_STATELESS_PYTHON_FUNCTION_URN,
			getUserDefinedDataStreamFunctionProto(pythonFunctionInfo, getRuntimeContext(), Collections.EMPTY_MAP),
			coderUrn,
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
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		bais.setBuffer(rawResult, 0, length);
		streamRecordCollector.collect(outputTypeSerializer.deserialize(baisWrapper));
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		inputTypeSerializer.serialize(element.getValue(), baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}
}
