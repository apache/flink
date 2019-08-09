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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Process Function for RANGE clause event-time bounded OVER window.
 *
 * <p>E.g.:
 * SELECT rowtime, b, c,
 * min(c) OVER
 * (PARTITION BY b ORDER BY rowtime
 * RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW),
 * max(c) OVER
 * (PARTITION BY b ORDER BY rowtime
 * RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW)
 * FROM T.
 */
public class RowTimeRangeBoundedPrecedingFunction<K> extends KeyedProcessFunctionWithCleanupState<K, BaseRow, BaseRow> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RowTimeRowsBoundedPrecedingFunction.class);

	private final GeneratedAggsHandleFunction genAggsHandler;
	private final LogicalType[] accTypes;
	private final LogicalType[] inputFieldTypes;
	private final long precedingOffset;
	private final int rowTimeIdx;

	private transient JoinedRow output;

	// the state which keeps the last triggering timestamp
	private transient ValueState<Long> lastTriggeringTsState;

	// the state which used to materialize the accumulator for incremental calculation
	private transient ValueState<BaseRow> accState;

	// the state which keeps all the data that are not expired.
	// The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
	// the second element of tuple is a list that contains the entire data of all the rows belonging
	// to this time stamp.
	private transient MapState<Long, List<BaseRow>> inputState;

	private transient AggsHandleFunction function;

	public RowTimeRangeBoundedPrecedingFunction(
			long minRetentionTime,
			long maxRetentionTime,
			GeneratedAggsHandleFunction genAggsHandler,
			LogicalType[] accTypes,
			LogicalType[] inputFieldTypes,
			long precedingOffset,
			int rowTimeIdx) {
		super(minRetentionTime, maxRetentionTime);
		Preconditions.checkNotNull(precedingOffset);
		this.genAggsHandler = genAggsHandler;
		this.accTypes = accTypes;
		this.inputFieldTypes = inputFieldTypes;
		this.precedingOffset = precedingOffset;
		this.rowTimeIdx = rowTimeIdx;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

		output = new JoinedRow();

		ValueStateDescriptor<Long> lastTriggeringTsDescriptor = new ValueStateDescriptor<Long>(
			"lastTriggeringTsState",
			Types.LONG);
		lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);

		BaseRowTypeInfo accTypeInfo = new BaseRowTypeInfo(accTypes);
		ValueStateDescriptor<BaseRow> accStateDesc = new ValueStateDescriptor<BaseRow>("accState", accTypeInfo);
		accState = getRuntimeContext().getState(accStateDesc);

		// input element are all binary row as they are came from network
		BaseRowTypeInfo inputType = new BaseRowTypeInfo(inputFieldTypes);
		ListTypeInfo<BaseRow> rowListTypeInfo = new ListTypeInfo<BaseRow>(inputType);
		MapStateDescriptor<Long, List<BaseRow>> inputStateDesc = new MapStateDescriptor<Long, List<BaseRow>>(
			"inputState",
			Types.LONG,
			rowListTypeInfo);
		inputState = getRuntimeContext().getMapState(inputStateDesc);

		initCleanupTimeState("RowTimeBoundedRangeOverCleanupTime");
	}

	@Override
	public void processElement(
			BaseRow input,
			KeyedProcessFunction<K, BaseRow, BaseRow>.Context ctx,
			Collector<BaseRow> out) throws Exception {
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());

		// triggering timestamp for trigger calculation
		long triggeringTs = input.getLong(rowTimeIdx);

		Long lastTriggeringTs = lastTriggeringTsState.value();
		if (lastTriggeringTs == null) {
			lastTriggeringTs = 0L;
		}

		// check if the data is expired, if not, save the data and register event time timer
		if (triggeringTs > lastTriggeringTs) {
			List<BaseRow> data = inputState.get(triggeringTs);
			if (null != data) {
				data.add(input);
				inputState.put(triggeringTs, data);
			} else {
				data = new ArrayList<BaseRow>();
				data.add(input);
				inputState.put(triggeringTs, data);
				// register event time timer
				ctx.timerService().registerEventTimeTimer(triggeringTs);
			}
		}
	}

	@Override
	public void onTimer(
			long timestamp,
			KeyedProcessFunction<K, BaseRow, BaseRow>.OnTimerContext ctx,
			Collector<BaseRow> out) throws Exception {
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());

		if (isProcessingTimeTimer(ctx)) {
			if (stateCleaningEnabled) {

				Iterator<Long> keysIt = inputState.keys().iterator();
				Long lastProcessedTime = lastTriggeringTsState.value();
				if (lastProcessedTime == null) {
					lastProcessedTime = 0L;
				}

				// is data left which has not been processed yet?
				boolean noRecordsToProcess = true;
				while (keysIt.hasNext() && noRecordsToProcess) {
					if (keysIt.next() > lastProcessedTime) {
						noRecordsToProcess = false;
					}
				}

				if (noRecordsToProcess) {
					// we clean the state
					cleanupState(inputState, accState, lastTriggeringTsState);
					function.cleanup();
				} else {
					// There are records left to process because a watermark has not been received yet.
					// This would only happen if the input stream has stopped. So we don't need to clean up.
					// We leave the state as it is and schedule a new cleanup timer
					registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());
				}
			}
			return;
		}

		// gets all window data from state for the calculation
		List<BaseRow> inputs = inputState.get(timestamp);

		if (null != inputs) {

			int dataListIndex = 0;
			BaseRow accumulators = accState.value();

			// initialize when first run or failover recovery per key
			if (null == accumulators) {
				accumulators = function.createAccumulators();
			}
			// set accumulators in context first
			function.setAccumulators(accumulators);

			// keep up timestamps of retract data
			List<Long> retractTsList = new ArrayList<Long>();

			// do retraction
			Iterator<Long> dataTimestampIt = inputState.keys().iterator();
			while (dataTimestampIt.hasNext()) {
				Long dataTs = dataTimestampIt.next();
				Long offset = timestamp - dataTs;
				if (offset > precedingOffset) {
					List<BaseRow> retractDataList = inputState.get(dataTs);
					if (retractDataList != null) {
						dataListIndex = 0;
						while (dataListIndex < retractDataList.size()) {
							BaseRow retractRow = retractDataList.get(dataListIndex);
							function.retract(retractRow);
							dataListIndex += 1;
						}
						retractTsList.add(dataTs);
					} else {
						// Does not retract values which are outside of window if the state is cleared already.
						LOG.warn("The state is cleared because of state ttl. " +
							"This will result in incorrect result. " +
							"You can increase the state ttl to avoid this.");
					}
				}
			}

			// do accumulation
			dataListIndex = 0;
			while (dataListIndex < inputs.size()) {
				BaseRow curRow = inputs.get(dataListIndex);
				// accumulate current row
				function.accumulate(curRow);
				dataListIndex += 1;
			}

			// get aggregate result
			BaseRow aggValue = function.getValue();

			// copy forwarded fields to output row and emit output row
			dataListIndex = 0;
			while (dataListIndex < inputs.size()) {
				BaseRow curRow = inputs.get(dataListIndex);
				output.replace(curRow, aggValue);
				out.collect(output);
				dataListIndex += 1;
			}

			// remove the data that has been retracted
			dataListIndex = 0;
			while (dataListIndex < retractTsList.size()) {
				inputState.remove(retractTsList.get(dataListIndex));
				dataListIndex += 1;
			}

			// update the value of accumulators for future incremental computation
			accumulators = function.getAccumulators();
			accState.update(accumulators);
		}
		lastTriggeringTsState.update(timestamp);

		// update cleanup timer
		registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());
	}

	@Override
	public void close() throws Exception {
		if (null != function) {
			function.close();
		}
	}
}
