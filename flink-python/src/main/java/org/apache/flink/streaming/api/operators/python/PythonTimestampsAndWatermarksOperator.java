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

import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.operators.TimestampsAndWatermarksOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.types.Row;

import java.util.Collections;

/**
 * A stream operator that may do one or both of the following: extract timestamps from
 * events and generate watermarks by user specify TimestampAssigner and WatermarkStrategy.
 *
 * <p>These two responsibilities run in the same operator rather than in two different ones,
 * because the implementation of the timestamp assigner and the watermark generator is
 * frequently in the same class (and should be run in the same instance), even though the
 * separate interfaces support the use of different classes.
 *
 * @param <IN> The type of the input elements
 */
public class PythonTimestampsAndWatermarksOperator<IN> extends StatelessOneInputPythonFunctionOperator<IN, IN>
	implements ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	/**
	 * A user specified watermarkStrategy.
	 */
	private final WatermarkStrategy<IN> watermarkStrategy;

	/**
	 * The TypeInformation of python worker input data.
	 */
	private final TypeInformation runnerInputTypeInfo;

	/**
	 * The TypeInformation of python worker output data.
	 */
	private final TypeInformation runnerOutputTypeInfo;

	/**
	 * Serializer to serialize input data for python worker.
	 */
	private transient TypeSerializer runnerInputSerializer;

	/**
	 * Serializer to deserialize output data from python worker.
	 */
	private transient TypeSerializer runnerOutputSerializer;

	/**
	 * The watermark generator, initialized during runtime.
	 */
	private transient WatermarkGenerator<IN> watermarkGenerator;

	/**
	 * The watermark output gateway, initialized during runtime.
	 */
	private transient WatermarkOutput watermarkOutput;

	/**
	 * The interval (in milliseconds) for periodic watermark probes. Initialized during runtime.
	 */
	private transient long watermarkInterval;

	/**
	 * Reusable row for normal data runner inputs.
	 */
	private transient Row reusableInput;

	/**
	 * Reusable StreamRecord for data with new timestamp calculated in TimestampAssigner.
	 */
	private transient StreamRecord<IN> reusableStreamRecord;

	/**
	 * Whether to emit intermediate watermarks or only one final watermark at the end of
	 * input.
	 */
	private boolean emitProgressiveWatermarks = true;

	public PythonTimestampsAndWatermarksOperator(
		Configuration config,
		TypeInformation<IN> inputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo,
		WatermarkStrategy<IN> watermarkStrategy) {
		super(config, inputTypeInfo, inputTypeInfo, pythonFunctionInfo);

		this.watermarkStrategy = watermarkStrategy;
		this.runnerInputTypeInfo = Types.ROW(Types.LONG, inputTypeInfo);
		this.runnerOutputTypeInfo = Types.ROW(Types.LONG, inputTypeInfo);
	}

	@Override
	public void open() throws Exception {
		super.open();
		runnerInputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerInputTypeInfo);
		runnerOutputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerOutputTypeInfo);
		reusableInput = new Row(2);
		reusableStreamRecord = new StreamRecord<>(null);
		watermarkGenerator = emitProgressiveWatermarks ?
			watermarkStrategy.createWatermarkGenerator(this::getMetricGroup) :
			new NoWatermarksGenerator<>();
		watermarkOutput = new TimestampsAndWatermarksOperator.WatermarkEmitter(output,
			getContainingTask().getStreamStatusMaintainer());

		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0 && emitProgressiveWatermarks) {
			final long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		final long previousTimestamp = element.hasTimestamp() ?
			element.getTimestamp() : Long.MIN_VALUE;

		reusableInput.setField(0, previousTimestamp);
		reusableInput.setField(1, value);

		runnerInputSerializer.serialize(reusableInput, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		bais.setBuffer(rawResult, 0, length);
		Row runnerOutput = (Row) runnerOutputSerializer.deserialize(baisWrapper);
		long newTimestamp = (Long) runnerOutput.getField(0);
		IN originalData = (IN) runnerOutput.getField(1);
		reusableStreamRecord.replace(originalData, newTimestamp);
		output.collect(reusableStreamRecord);
		watermarkGenerator.onEvent(originalData, newTimestamp, watermarkOutput);
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
		return new BeamDataStreamPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			runnerInputTypeInfo,
			runnerOutputTypeInfo,
			DATA_STREAM_STATELESS_FUNCTION_URN,
			PythonOperatorUtils.getUserDefinedDataStreamFunctionProto(pythonFunctionInfo, getRuntimeContext(), Collections.EMPTY_MAP),
			MAP_CODER_URN,
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

	public void configureEmitProgressiveWatermarks(boolean emitProgressiveWatermarks) {
		this.emitProgressiveWatermarks = emitProgressiveWatermarks;
	}

	@Override
	public void onProcessingTime(long timestamp) {
		watermarkGenerator.onPeriodicEmit(watermarkOutput);
		final long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	@Override
	public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) {
		if (mark.getTimestamp() == Long.MAX_VALUE) {
			watermarkOutput.emitWatermark(Watermark.MAX_WATERMARK);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		watermarkGenerator.onPeriodicEmit(watermarkOutput);
	}
}
