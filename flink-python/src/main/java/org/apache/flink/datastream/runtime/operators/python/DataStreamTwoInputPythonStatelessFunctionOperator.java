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

package org.apache.flink.datastream.runtime.operators.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.datastream.runtime.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.datastream.runtime.runners.python.beam.BeamDataStreamPythonStatelessFunctionRunner;
import org.apache.flink.datastream.runtime.typeutils.python.PythonTypeUtils;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.operators.python.AbstractTwoInputPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;

import java.util.Map;

/**
 * {@link DataStreamTwoInputPythonStatelessFunctionOperator} is responsible for launching beam
 * runner which will start a python harness to execute two-input user defined python function.
 */
public class DataStreamTwoInputPythonStatelessFunctionOperator<IN1, IN2, OUT>
	extends AbstractTwoInputPythonFunctionOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	private static final String DATA_STREAM_STATELESS_PYTHON_FUNCTION_URN =
		"flink:transform:datastream_stateless_function:v1";
	private static final String DATA_STREAM_MAP_FUNCTION_CODER_URN = "flink:coder:datastream:map_function:v1";
	private static final String DATA_STREAM_FLAT_MAP_FUNCTION_CODER_URN = "flink:coder:datastream:flatmap_function:v1";


	private final DataStreamPythonFunctionInfo pythonFunctionInfo;

	private final TypeInformation<OUT> outputTypeInfo;

	private final Map<String, String> jobOptions;
	private transient TypeSerializer<OUT> outputTypeSerializer;

	private transient ByteArrayInputStreamWithPos bais;

	private transient DataInputViewStreamWrapper baisWrapper;

	private transient ByteArrayOutputStreamWithPos baos;

	private transient DataOutputViewStreamWrapper baosWrapper;

	private transient StreamRecordCollector streamRecordCollector;

	private transient TypeSerializer<Row> runnerInputTypeSerializer;

	private final TypeInformation<Row> runnerInputTypeInfo;

	private transient Row reuseRow;

	// True if input1 and input2 are KeyedStream
	private boolean isKeyedStream;

	private final TypeInformation<IN1> inputTypeInfo1;
	private final TypeInformation<IN2> inputTypeInfo2;

	public DataStreamTwoInputPythonStatelessFunctionOperator(
		Configuration config,
		TypeInformation<IN1> inputTypeInfo1,
		TypeInformation<IN2> inputTypeInfo2,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo,
		boolean isKeyedStream) {
		super(config);
		this.pythonFunctionInfo = pythonFunctionInfo;
		jobOptions = config.toMap();
		this.outputTypeInfo = outputTypeInfo;
		this.isKeyedStream = isKeyedStream;
		this.inputTypeInfo1 = inputTypeInfo1;
		this.inputTypeInfo2 = inputTypeInfo2;
		// The row contains three field. The first field indicate left input or right input
		// The second field contains left input and the third field contains right input.
		runnerInputTypeInfo = initRunnerInputTypeInfo();
	}

	@Override
	public void open() throws Exception {
		super.open();
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);

		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);
		this.outputTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(outputTypeInfo);
		runnerInputTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerInputTypeInfo);

		reuseRow = new Row(3);
		this.streamRecordCollector = new StreamRecordCollector(output);
	}

	private TypeInformation<Row> initRunnerInputTypeInfo() {
		if (isKeyedStream) {
			// since we wrap a keyed field for python KeyedStream, we need to extract the
			// corresponding data input type.
			return new RowTypeInfo(
				Types.BOOLEAN,
				((RowTypeInfo) inputTypeInfo1).getTypeAt(1),
				((RowTypeInfo) inputTypeInfo2).getTypeAt(1));
		} else {
			return new RowTypeInfo(Types.BOOLEAN, inputTypeInfo1, inputTypeInfo2);
		}
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {

		String coderUrn;
		int functionType = this.pythonFunctionInfo.getFunctionType();
		if (functionType == FlinkFnApi.UserDefinedDataStreamFunction.FunctionType.CO_MAP.getNumber()) {
			coderUrn = DATA_STREAM_MAP_FUNCTION_CODER_URN;
		} else if (functionType == FlinkFnApi.UserDefinedDataStreamFunction.FunctionType.CO_FLAT_MAP.getNumber()) {
			coderUrn = DATA_STREAM_FLAT_MAP_FUNCTION_CODER_URN;
		} else {
			throw new RuntimeException("Function Type for ConnectedStream should be Map or FlatMap");
		}

		return new BeamDataStreamPythonStatelessFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			runnerInputTypeInfo,
			outputTypeInfo,
			DATA_STREAM_STATELESS_PYTHON_FUNCTION_URN,
			getUserDefinedDataStreamFunctionsProto(),
			coderUrn,
			jobOptions,
			getFlinkMetricContainer()
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

	private Object getStreamRecordValue(StreamRecord<?> element) throws Exception {
		if (isKeyedStream) {
			// since we wrap a keyed field for python KeyedStream, we need to extract the
			// corresponding data input.
			return ((Row) element.getValue()).getField(1);
		} else {
			return element.getValue();
		}
	}

	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		// construct combined row.
		reuseRow.setField(0, true);
		reuseRow.setField(1, getStreamRecordValue(element));
		reuseRow.setField(2, null);  // need to set null since it is a reuse row.
		processElement();
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		// construct combined row.
		reuseRow.setField(0, false);
		reuseRow.setField(1, null); // need to set null since it is a reuse row.
		reuseRow.setField(2, getStreamRecordValue(element));
		processElement();
	}

	private void processElement() throws Exception {
		runnerInputTypeSerializer.serialize(reuseRow, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	protected FlinkFnApi.UserDefinedDataStreamFunctions getUserDefinedDataStreamFunctionsProto() {
		FlinkFnApi.UserDefinedDataStreamFunctions.Builder builder = FlinkFnApi.UserDefinedDataStreamFunctions.newBuilder();
		builder.addUdfs(getUserDefinedDataStreamFunctionProto(pythonFunctionInfo));
		return builder.build();
	}

	private FlinkFnApi.UserDefinedDataStreamFunction getUserDefinedDataStreamFunctionProto(
		DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo) {
		FlinkFnApi.UserDefinedDataStreamFunction.Builder builder =
			FlinkFnApi.UserDefinedDataStreamFunction.newBuilder();
		builder.setFunctionType(FlinkFnApi.UserDefinedDataStreamFunction.FunctionType.forNumber(
			dataStreamPythonFunctionInfo.getFunctionType()));
		builder.setPayload(ByteString.copyFrom(
			dataStreamPythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
		return builder.build();
	}
}
