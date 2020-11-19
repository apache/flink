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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import java.util.Collections;

/**
 * {@link PythonKeyedProcessFunctionOperator} is responsible for launching beam runner which will start
 * a python harness to execute user defined python function. It is also able to handle the timer and
 * state request from the python stateful user defined function.
 */
@Internal
public class PythonKeyedProcessFunctionOperator<OUT> extends OneInputPythonFunctionOperator<Row, OUT>
	implements ResultTypeQueryable<OUT>, Triggerable<Row, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private static final String KEYED_PROCESS_FUNCTION_URN = "flink:transform:keyed_process_function:v1";

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
	 * The TypeInformation of current key.
	 */
	private transient TypeInformation<Row> keyTypeInfo;

	/**
	 * Serializer to serialize input data for python worker.
	 */
	private transient TypeSerializer runnerInputSerializer;

	/**
	 * Serializer to deserialize output data from python worker.
	 */
	private transient TypeSerializer runnerOutputSerializer;

	/**
	 * Serializer for current key.
	 */
	private transient TypeSerializer keyTypeSerializer;

	/**
	 * TimerService for current operator to register or fire timer.
	 */
	private transient TimerService timerService;

	/**
	 * Reusable row for normal data runner inputs.
	 */
	private transient Row reusableInput;

	/**
	 * Reusable row for timer data runner inputs.
	 */
	private transient Row reusableTimerData;

	public PythonKeyedProcessFunctionOperator(
		Configuration config,
		RowTypeInfo inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config, inputTypeInfo, outputTypeInfo, pythonFunctionInfo);
	}

	@Override
	public void open() throws Exception {
		keyTypeInfo = new RowTypeInfo(((RowTypeInfo) inputTypeInfo).getTypeAt(0));
		// inputType: normal data/ timer data, timerType: proc/event time, currentWatermark, keyData, real data
		runnerInputTypeInfo = Types.ROW(Types.INT, Types.LONG, Types.LONG, keyTypeInfo, inputTypeInfo);
		runnerOutputTypeInfo = Types.ROW(Types.INT, Types.LONG, keyTypeInfo, outputTypeInfo);
		keyTypeInfo = new RowTypeInfo(((RowTypeInfo) inputTypeInfo).getTypeAt(0));
		keyTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(keyTypeInfo);
		runnerInputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerInputTypeInfo);
		runnerOutputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerOutputTypeInfo);

		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService("user-timers",
			VoidNamespaceSerializer.INSTANCE, this);
		timerService = new SimpleTimerService(internalTimerService);
		reusableInput = new Row(5);
		reusableTimerData = new Row(5);
		super.open();
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return outputTypeInfo;
	}

	@Override
	public void onEventTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
		bufferedTimestamp.offer(timer.getTimestamp());
		processTimer(false, timer);
	}

	@Override
	public void onProcessingTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
		bufferedTimestamp.offer(Long.MIN_VALUE);
		processTimer(true, timer);
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
		return new BeamDataStreamPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			runnerInputTypeInfo,
			runnerOutputTypeInfo,
			KEYED_PROCESS_FUNCTION_URN,
			PythonOperatorUtils.getUserDefinedDataStreamStatefulFunctionProto(
				pythonFunctionInfo, getRuntimeContext(), Collections.EMPTY_MAP, keyTypeInfo),
			FLAT_MAP_CODER_URN,
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
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		if (PythonOperatorUtils.endOfLastFlatMap(length, rawResult)) {
			bufferedTimestamp.poll();
		} else {
			bais.setBuffer(rawResult, 0, length);
			Row runnerOutput = (Row) runnerOutputSerializer.deserialize(baisWrapper);
			if (runnerOutput.getField(0) != null) {
				registerTimer(runnerOutput);
			} else {
				collector.setAbsoluteTimestamp(bufferedTimestamp.peek());
				collector.collect(runnerOutput.getField(3));
			}
		}
	}

	@Override
	public void processElement(StreamRecord<Row> element) throws Exception {
		bufferedTimestamp.offer(element.getTimestamp());
		if (element.hasTimestamp()) {
			reusableInput.setField(1, element.getTimestamp());
		}
		reusableInput.setField(2, timerService.currentWatermark());
		reusableInput.setField(4, element.getValue());
		runnerInputSerializer.serialize(reusableInput, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	/**
	 * It is responsible to send timer data to python worker when a registered timer is fired. The
	 * input data is a Row containing 4 fields: TimerFlag 0 for proc time, 1 for event time;
	 * Timestamp of the fired timer; Current watermark and the key of the timer.
	 *
	 * @param procTime Whether is it a proc time timer, otherwise event time timer.
	 * @param timer    The fired timer.
	 * @throws Exception The runnerInputSerializer might throw exception.
	 */
	private void processTimer(boolean procTime, InternalTimer<Row, VoidNamespace> timer) throws Exception {
		long time = timer.getTimestamp();
		Row timerKey = Row.of(timer.getKey());
		if (procTime) {
			reusableTimerData.setField(0, 1);
		} else {
			reusableTimerData.setField(0, 0);
		}
		reusableTimerData.setField(1, time);
		reusableTimerData.setField(2, timerService.currentWatermark());
		reusableTimerData.setField(3, timerKey);
		runnerInputSerializer.serialize(reusableTimerData, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	/**
	 * Handler the timer registration request from python user defined function. Before registering
	 * the timer, we must set the current key to be the key when the timer is register in python
	 * side.
	 *
	 * @param row The timer registration request data.
	 */
	private void registerTimer(Row row) {
		synchronized (getKeyedStateBackend()) {
			int type = (int) row.getField(0);
			long time = (long) row.getField(1);
			Object timerKey = ((Row) (row.getField(2))).getField(0);
			setCurrentKey(timerKey);
			if (type == 0) {
				this.timerService.registerProcessingTimeTimer(time);
			} else {
				this.timerService.registerEventTimeTimer(time);
			}
		}
	}
}
