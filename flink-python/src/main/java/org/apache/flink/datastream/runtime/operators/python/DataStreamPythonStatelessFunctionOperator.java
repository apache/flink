package org.apache.flink.datastream.runtime.operators.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

import com.google.protobuf.ByteString;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * DataStreamPythonFunctionOperator is responsible for launching beam runner which will start a python harness to
 * execute user defined python function.
 */
public class DataStreamPythonStatelessFunctionOperator<IN, OUT> extends AbstractPythonFunctionOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private static final String DATA_STREAM_STATELESS_PYTHON_FUNCTION_URN = "flink:transform:datastream_stateless_function:v1";
	private static final String DATA_STREAM_MAP_FUNCTION_CODER_URN = "flink:coder:datastream:map_function:v1";
	private static final String DATA_STREAM_FLAT_MAP_FUNCTION_CODER_URN = "flink:coder:datastream:flatmap_function:v1";


	protected final DataStreamPythonFunctionInfo pythonFunctionInfo;

	private final TypeInformation<IN> inputTypeInfo;

	private final TypeInformation<OUT> outputTypeInfo;

	private final Map<String, String> jobOptions;

	private transient TypeSerializer<IN> inputTypeSerializer;

	private transient TypeSerializer<OUT> outputTypeSerializer;

	protected transient LinkedBlockingQueue<byte[]> userDefinedFunctionResultQueue;

	protected transient ByteArrayInputStreamWithPos bais;

	protected transient DataInputViewStreamWrapper baisWrapper;

	protected transient ByteArrayOutputStreamWithPos baos;

	protected transient DataOutputViewStreamWrapper baosWrapper;

	protected transient StreamRecordCollector streamRecordCollector;

	public DataStreamPythonStatelessFunctionOperator(
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

		return new BeamDataStreamPythonStatelessFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			inputTypeInfo,
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

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		inputTypeSerializer.serialize(element.getValue(), baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	private FlinkFnApi.UserDefinedDataStreamFunctions getUserDefinedDataStreamFunctionsProto() {
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
