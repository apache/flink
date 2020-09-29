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

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.python.AbstractOneInputPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.functions.CleanupState;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.python.utils.PythonOperatorUtils;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.runtime.runners.python.beam.BeamTableStatefulPythonFunctionRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.toProtoType;

/**
 *  The Python AggregateFunction operator for the blink planner.
 */
public class PythonStreamGroupAggregateOperator
	extends AbstractOneInputPythonFunctionOperator<RowData, RowData>
	implements Triggerable<RowData, VoidNamespace>, CleanupState {

	private static final long serialVersionUID = 1L;

	@VisibleForTesting
	protected static final String FLINK_AGGREGATE_FUNCTION_SCHEMA_CODER_URN =
		"flink:coder:schema:aggregate_function:v1";

	@VisibleForTesting
	protected static final String STREAM_GROUP_AGGREGATE_URN = "flink:transform:stream_group_aggregate:v1";

	@VisibleForTesting
	protected static final byte NORMAL_RECORD = 0;

	private static final byte TRIGGER_TIMER = 1;

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
	private final Map<String, String> jobOptions;

	private final PythonFunctionInfo[] aggregateFunctions;

	/**
	 * The array of the key indexes.
	 */
	private final int[] grouping;

	/**
	 * The index of a count aggregate used to calculate the number of accumulated rows.
	 */
	private final int indexOfCountStar;

	/**
	 * Generate retract messages if true.
	 */
	private final boolean generateUpdateBefore;

	/**
	 * The minimum time in milliseconds until state which was not updated will be retained.
	 */
	private final long minRetentionTime;

	/**
	 * The maximum time in milliseconds until state which was not updated will be retained.
	 */
	private final long maxRetentionTime;

	/**
	 * The maximum NUMBER of the states cached in Python side.
	 */
	private int stateCacheSize;

	/**
	 * Indicates whether state cleaning is enabled. Can be calculated from the `minRetentionTime`.
	 */
	private final boolean stateCleaningEnabled;

	private transient Object keyForTimerService;

	/**
	 * The user-defined function input logical type.
	 */
	protected transient RowType userDefinedFunctionInputType;

	/**
	 * The TypeSerializer for udf execution results.
	 */
	protected transient TypeSerializer<RowData> udfOutputTypeSerializer;

	/**
	 * The TypeSerializer for udf input elements.
	 */
	protected transient TypeSerializer<RowData> udfInputTypeSerializer;

	/**
	 * Reusable InputStream used to holding the execution results to be deserialized.
	 */
	private transient ByteArrayInputStreamWithPos bais;

	/**
	 * InputStream Wrapper.
	 */
	private transient DataInputViewStreamWrapper baisWrapper;

	/**
	 * Reusable OutputStream used to holding the serialized input elements.
	 */
	private transient ByteArrayOutputStreamWithPos baos;

	/**
	 * OutputStream Wrapper.
	 */
	private transient DataOutputViewStreamWrapper baosWrapper;

	private transient TimerService timerService;

	// holds the latest registered cleanup timer
	private transient ValueState<Long> cleanupTimeState;

	private transient UpdatableRowData reuseRowData;
	private transient UpdatableRowData reuseTimerRowData;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordRowDataWrappingCollector rowDataWrapper;

	public PythonStreamGroupAggregateOperator(
			Configuration config,
			RowType inputType,
			RowType outputType,
			PythonFunctionInfo[] aggregateFunctions,
			int[] grouping,
			int indexOfCountStar,
			boolean generateUpdateBefore,
			long minRetentionTime,
			long maxRetentionTime) {
		super(config);
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.aggregateFunctions = aggregateFunctions;
		this.jobOptions = buildJobOptions(config);
		this.grouping = grouping;
		this.indexOfCountStar = indexOfCountStar;
		this.generateUpdateBefore = generateUpdateBefore;
		this.minRetentionTime = minRetentionTime;
		this.maxRetentionTime = maxRetentionTime;
		this.stateCleaningEnabled = minRetentionTime > 1;
	}

	/**
	 * As the beam state gRPC service will access the KeyedStateBackend in parallel with this operator, we must
	 * override this method to prevent changing the current key of the KeyedStateBackend while the beam service
	 * is handling requests.
	 */
	@Override
	public void setCurrentKey(Object key) {
		keyForTimerService = key;
	}

	@Override
	public Object getCurrentKey() {
		return keyForTimerService;
	}

	@Override
	public void open() throws Exception {
		List<RowType.RowField> fields = new ArrayList<>();
		fields.add(new RowType.RowField("record_type", new TinyIntType()));
		fields.add(new RowType.RowField("row", inputType));
		fields.add(new RowType.RowField("timestamp", new BigIntType()));
		fields.add(new RowType.RowField("key", getKeyType()));
		userDefinedFunctionInputType = new RowType(fields);
		udfInputTypeSerializer = PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionInputType);
		udfOutputTypeSerializer = PythonTypeUtils.toBlinkTypeSerializer(outputType);
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);
		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);
		timerService = new SimpleTimerService(
			getInternalTimerService("state-clean-timer", VoidNamespaceSerializer.INSTANCE, this));
		// The structure is:  [type]|[normal record]|[timestamp of timer]|[row key of timer]
		// If the type is 'NORMAL_RECORD', store the RowData object in the 2nd column.
		// If the type is 'TRIGGER_TIMER', store the timestamp in 3rd column and the row key in 4th column.
		reuseRowData = new UpdatableRowData(GenericRowData.of(NORMAL_RECORD, null, null, null), 4);
		reuseTimerRowData = new UpdatableRowData(GenericRowData.of(TRIGGER_TIMER, null, null, null), 4);
		rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
		initCleanupTimeState();
		super.open();
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
		return new BeamTableStatefulPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			userDefinedFunctionInputType,
			outputType,
			STREAM_GROUP_AGGREGATE_URN,
			getUserDefinedFunctionsProto(),
			FLINK_AGGREGATE_FUNCTION_SCHEMA_CODER_URN,
			jobOptions,
			getFlinkMetricContainer(),
			getKeyedStateBackend(),
			getKeySerializer());
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		RowData value = element.getValue();
		processElementInternal(value);
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawUdfResult = resultTuple.f0;
		int length = resultTuple.f1;
		bais.setBuffer(rawUdfResult, 0, length);
		RowData udfResult = udfOutputTypeSerializer.deserialize(baisWrapper);
		rowDataWrapper.collect(udfResult);
	}

	/**
	 * Invoked when an event-time timer fires.
	 */
	@Override
	public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) {
		// ignore
	}

	/**
	 * Invoked when a processing-time timer fires.
	 */
	@Override
	public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
		if (stateCleaningEnabled) {
			RowData key = timer.getKey();
			long timestamp = timer.getTimestamp();
			reuseTimerRowData.setLong(2, timestamp);
			reuseTimerRowData.setField(3, key);
			udfInputTypeSerializer.serialize(reuseTimerRowData, baosWrapper);
			pythonFunctionRunner.process(baos.toByteArray());
			baos.reset();
			elementCount++;
		}
	}

	@Override
	public PythonEnv getPythonEnv() {
		return aggregateFunctions[0].getPythonFunction().getPythonEnv();
	}

	private void registerProcessingCleanupTimer(long currentTime) throws Exception {
		if (stateCleaningEnabled) {
			synchronized (getKeyedStateBackend()) {
				getKeyedStateBackend().setCurrentKey(getCurrentKey());
				registerProcessingCleanupTimer(
					cleanupTimeState,
					currentTime,
					minRetentionTime,
					maxRetentionTime,
					timerService
				);
			}
		}
	}

	private void initCleanupTimeState() {
		if (stateCleaningEnabled) {
			ValueStateDescriptor<Long> inputCntDescriptor =
				new ValueStateDescriptor<>("PythonGroupAggregateCleanupTime", Types.LONG);
			cleanupTimeState = getRuntimeContext().getState(inputCntDescriptor);
		}
	}

	private Map<String, String> buildJobOptions(Configuration config) {
		Map<String, String> jobOptions = new HashMap<>();
		if (config.containsKey("table.exec.timezone")) {
			jobOptions.put("table.exec.timezone", config.getString("table.exec.timezone", null));
		}
		stateCacheSize = config.get(PythonOptions.STATE_CACHE_SIZE);
		jobOptions.put(PythonOptions.STATE_CACHE_SIZE.key(), String.valueOf(stateCacheSize));
		return jobOptions;
	}

	@VisibleForTesting
	protected TypeSerializer getKeySerializer() {
		return PythonTypeUtils.toBlinkTypeSerializer(getKeyType());
	}

	private void processElementInternal(RowData value) throws Exception {
		long currentTime = timerService.currentProcessingTime();
		registerProcessingCleanupTimer(currentTime);
		reuseRowData.setField(1, value);
		udfInputTypeSerializer.serialize(reuseRowData, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
	}

	protected RowType getKeyType() {
		RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
			grouping,
			InternalTypeInfo.of(inputType));
		return selector.getProducedType().toRowType();
	}

	/**
	 * Gets the proto representation of the Python user-defined aggregate functions to be executed.
	 */
	public FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
			FlinkFnApi.UserDefinedAggregateFunctions.newBuilder();
		for (PythonFunctionInfo pythonFunctionInfo : aggregateFunctions) {
			builder.addUdfs(PythonOperatorUtils.getUserDefinedFunctionProto(pythonFunctionInfo));
		}
		builder.setMetricEnabled(getPythonConfig().isMetricEnabled());
		builder.addAllGrouping(Arrays.stream(grouping).boxed().collect(Collectors.toList()));
		builder.setGenerateUpdateBefore(generateUpdateBefore);
		builder.setIndexOfCountStar(indexOfCountStar);
		builder.setKeyType(toProtoType(getKeyType()));
		builder.setStateCleaningEnabled(stateCleaningEnabled);
		builder.setStateCacheSize(stateCacheSize);
		return builder.build();
	}
}
