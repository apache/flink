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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamStatefulPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * {@link StatefulPythonFunctionOperator} is responsible for launching beam runner which will start
 * a python harness to execute user defined python function. It is also able to handle the timer and
 * state request from the python stateful user defined function.
 * */
public class StatefulPythonFunctionOperator<OUT> extends AbstractOneInputPythonFunctionOperator<Row, OUT>
	implements ResultTypeQueryable<OUT>, Triggerable<Row, VoidNamespace> {

	private static final Logger LOGGER = LoggerFactory.getLogger(StatefulPythonFunctionOperator.class);

	protected static final String DATA_STREAM_STATEFUL_PYTHON_FUNCTION_URN =
		"flink:transform:datastream_stateful_function:v1";

	protected static final String DATA_STREAM_STATEFUL_MAP_FUNCTION_CODER_URN =
		"flink:coder:datastream:stateful_map_function:v1";

	protected final DataStreamPythonFunctionInfo pythonFunctionInfo;
	private final TypeInformation runnerInputTypeInfo;
	private final TypeInformation runnerOutputTypeInfo;
	private final TypeInformation<OUT> outputTypeInfo;
	private final TypeInformation<Row> keyTypeInfo;
	private TypeSerializer runnerInputSerializer;
	private TypeSerializer runnerOutputSerializer;
	private TypeSerializer keyTypeSerializer;

	private transient TimerService timerservice;

	protected final Map<String, String> jobOptions;

	protected transient ByteArrayInputStreamWithPos bais;

	protected transient DataInputViewStreamWrapper baisWrapper;

	protected transient ByteArrayOutputStreamWithPos baos;

	protected transient DataOutputViewStreamWrapper baosWrapper;

	protected transient StreamRecordCollector streamRecordCollector;

	protected Row reusableInput;
	protected Row reusableTimerData;

	public StatefulPythonFunctionOperator(
		Configuration config,
		RowTypeInfo inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config);
		this.jobOptions = config.toMap();
		this.pythonFunctionInfo = pythonFunctionInfo;
		this.outputTypeInfo = outputTypeInfo;
		this.keyTypeInfo = new RowTypeInfo(inputTypeInfo.getTypeAt(0));
		this.keyTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(keyTypeInfo);
		// inputType: normal data/ timer data, timerType: proc/event time, currentWatermark, keyData, real data
		this.runnerInputTypeInfo = Types.ROW(Types.INT, Types.LONG, Types.LONG, this.keyTypeInfo, inputTypeInfo);
		this.runnerOutputTypeInfo = Types.ROW(Types.INT, Types.LONG, this.keyTypeInfo, outputTypeInfo);
	}

	@Override
	public void open() throws Exception {
		super.open();
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);

		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);
		runnerInputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerInputTypeInfo);
		runnerOutputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerOutputTypeInfo);

		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService("user-timers",
			VoidNamespaceSerializer.INSTANCE, this);

		this.streamRecordCollector = new StreamRecordCollector(output);
		timerservice = new SimpleTimerService(internalTimerService);
		reusableInput = new Row(5);
		reusableTimerData = new Row(5);
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return outputTypeInfo;
	}

	@Override
	public void onEventTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
		processTimer(false, timer);
		checkInvokeFinishBundleByCount();
	}

	@Override
	public void onProcessingTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
		processTimer(true, timer);
		checkInvokeFinishBundleByCount();
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
		return new BeamDataStreamStatefulPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			runnerInputTypeInfo,
			runnerOutputTypeInfo,
			DATA_STREAM_STATEFUL_PYTHON_FUNCTION_URN,
			PythonOperatorUtils.getUserDefinedDataStreamStatefulFunctionProto(
				pythonFunctionInfo, getRuntimeContext(), Collections.EMPTY_MAP, keyTypeInfo),
			DATA_STREAM_STATEFUL_MAP_FUNCTION_CODER_URN,
			jobOptions,
			getFlinkMetricContainer(),
			getKeyedStateBackend(),
			keyTypeSerializer,
			getContainingTask().getEnvironment().getMemoryManager(),
			getOperatorConfig().getManagedMemoryFractionOperatorUseCaseOfSlot(
				ManagedMemoryUseCase.PYTHON,
				getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration(),
				getContainingTask().getEnvironment().getUserCodeClassLoader().asClassLoader()));
	}

	@Override
	public PythonEnv getPythonEnv() {
		return this.pythonFunctionInfo.getPythonFunction().getPythonEnv();
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		bais.setBuffer(rawResult, 0, length);
		Row runnerOutput = (Row) runnerOutputSerializer.deserialize(baisWrapper);
		if (runnerOutput.getField(0) != null) {
			registerTimer(runnerOutput);
		} else {
			streamRecordCollector.collect(runnerOutput.getField(3));
		}

	}

	@Override
	public void processElement(StreamRecord<Row> element) throws Exception {
		LOGGER.info("Current watermark: " + timerservice.currentWatermark());
		reusableInput.setField(2, timerservice.currentWatermark());
		reusableInput.setField(4, element.getValue());
		runnerInputSerializer.serialize(reusableInput, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	private void processTimer(boolean procTime, InternalTimer<Row, VoidNamespace> timer) throws Exception {
		long time = timer.getTimestamp();
		Row timerKey = Row.of(timer.getKey());
		if (procTime) {
			reusableTimerData.setField(0, 0);
		} else {
			reusableTimerData.setField(0, 1);
		}
		reusableTimerData.setField(1, time);
		reusableTimerData.setField(2, timerservice.currentWatermark());
		reusableTimerData.setField(3, timerKey);
		runnerInputSerializer.serialize(reusableTimerData, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
	}

	private void registerTimer(Row row) {
		synchronized (getKeyedStateBackend()) {
			int type = (int) row.getField(0);
			long time = (long) row.getField(1);
			Object timerKey = ((Row) (row.getField(2))).getField(0);
			setCurrentKey(timerKey);
			if (type == 0) {
				this.timerservice.registerProcessingTimeTimer(time);
			} else {
				this.timerservice.registerEventTimeTimer(time);
			}
		}
	}
}
