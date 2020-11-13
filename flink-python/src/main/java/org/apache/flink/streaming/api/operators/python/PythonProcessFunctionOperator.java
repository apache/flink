package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import java.util.Collections;

/**
 * {@link PythonProcessFunctionOperator} is responsible for launching beam runner which will start
 * a python harness to execute user defined python ProcessFunction.
 */
public class PythonProcessFunctionOperator<IN, OUT> extends OneInputPythonFunctionOperator<IN, OUT>{

	private static final long serialVersionUID = 1L;

	private static final String PROCESS_FUNCTION_URN = "flink:transform:process_function:v1";

	private static final String FLAT_MAP_CODER_URN = "flink:coder:flat_map:v1";

	/**
	 * The TypeInformation of python worker input data.
	 */
	private transient TypeInformation runnerInputTypeInfo;

	/**
	 * The TypeInformation of python worker output data.
	 */
	private transient TypeInformation runnerOutputTypeInfo;

	/**
	 * Serializer to serialize input data for python worker.
	 */
	private transient TypeSerializer runnerInputSerializer;

	/**
	 * Serializer to deserialize output data from python worker.
	 */
	private transient TypeSerializer runnerOutputSerializer;

	/**
	 * Reusable row for normal data runner inputs.
	 */
	private transient Row reusableInput;

	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	private long currentWatermark = Long.MIN_VALUE;

	public PythonProcessFunctionOperator(
		Configuration config,
		TypeInformation<IN> inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config, inputTypeInfo, outputTypeInfo, pythonFunctionInfo);
	}

	@Override
	public void open() throws Exception {
		runnerInputTypeInfo = Types.ROW(Types.LONG, Types.LONG, inputTypeInfo);
		runnerOutputTypeInfo = outputTypeInfo;
		runnerInputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerInputTypeInfo);
		runnerOutputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerOutputTypeInfo);
		reusableInput = new Row(3);
		super.open();
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
		return new BeamDataStreamPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			runnerInputTypeInfo,
			runnerOutputTypeInfo,
			PROCESS_FUNCTION_URN,
			PythonOperatorUtils.getUserDefinedDataStreamFunctionProto(
				pythonFunctionInfo, getRuntimeContext(), Collections.EMPTY_MAP),
			FLAT_MAP_CODER_URN,
			jobOptions,
			getFlinkMetricContainer(),
			null,
			null,
			getContainingTask().getEnvironment().getMemoryManager(),
			getOperatorConfig().getManagedMemoryFractionOperatorUseCaseOfSlot(
				ManagedMemoryUseCase.PYTHON,
				getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration(),
				getContainingTask().getEnvironment().getUserCodeClassLoader().asClassLoader()));
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		if (PythonOperatorUtils.endOfLastFlatMap(length, rawResult)) {
			bufferedTimestamp.poll();
		} else {
			bais.setBuffer(rawResult, 0, length);
			Object runnerOutput = runnerOutputSerializer.deserialize(baisWrapper);
			collector.setAbsoluteTimestamp(bufferedTimestamp.peek());
			collector.collect(runnerOutput);
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		bufferedTimestamp.offer(element.getTimestamp());
		reusableInput.setField(0, element.getTimestamp());
		reusableInput.setField(1, currentWatermark);
		reusableInput.setField(2, element.getValue());
		runnerInputSerializer.serialize(reusableInput, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		currentWatermark = mark.getTimestamp();
	}
}
