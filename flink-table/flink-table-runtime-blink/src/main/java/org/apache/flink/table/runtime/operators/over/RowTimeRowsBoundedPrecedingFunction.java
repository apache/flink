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
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Process Function for ROWS clause event-time bounded OVER window.
 *
 * <p>E.g.:
 * SELECT rowtime, b, c,
 * min(c) OVER
 * (PARTITION BY b ORDER BY rowtime
 * ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
 * max(c) OVER
 * (PARTITION BY b ORDER BY rowtime
 * ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
 * FROM T.
 */
public class RowTimeRowsBoundedPrecedingFunction<K> extends KeyedProcessFunctionWithCleanupState<K, RowData, RowData> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RowTimeRowsBoundedPrecedingFunction.class);

	private final GeneratedAggsHandleFunction genAggsHandler;
	private final LogicalType[] accTypes;
	private final LogicalType[] inputFieldTypes;
	private final long precedingOffset;
	private final int rowTimeIdx;

	private transient JoinedRowData output;

	// the state which keeps the last triggering timestamp
	private transient ValueState<Long> lastTriggeringTsState;

	// the state which keeps the count of data
	private transient ValueState<Long> counterState;

	// the state which used to materialize the accumulator for incremental calculation
	private transient ValueState<RowData> accState;

	// the state which keeps all the data that are not expired.
	// The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
	// the second element of tuple is a list that contains the entire data of all the rows belonging
	// to this time stamp.
	private transient MapState<Long, List<RowData>> inputState;

	private transient AggsHandleFunction function;

	public RowTimeRowsBoundedPrecedingFunction(
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

		output = new JoinedRowData();

		ValueStateDescriptor<Long> lastTriggeringTsDescriptor = new ValueStateDescriptor<Long>(
			"lastTriggeringTsState",
			Types.LONG);
		lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);

		ValueStateDescriptor<Long> dataCountStateDescriptor = new ValueStateDescriptor<Long>(
			"processedCountState",
			Types.LONG);
		counterState = getRuntimeContext().getState(dataCountStateDescriptor);

		RowDataTypeInfo accTypeInfo = new RowDataTypeInfo(accTypes);
		ValueStateDescriptor<RowData> accStateDesc = new ValueStateDescriptor<RowData>("accState", accTypeInfo);
		accState = getRuntimeContext().getState(accStateDesc);

		// input element are all binary row as they are came from network
		RowDataTypeInfo inputType = new RowDataTypeInfo(inputFieldTypes);
		ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<RowData>(inputType);
		MapStateDescriptor<Long, List<RowData>> inputStateDesc = new MapStateDescriptor<Long, List<RowData>>(
			"inputState",
			Types.LONG,
			rowListTypeInfo);
		inputState = getRuntimeContext().getMapState(inputStateDesc);

		initCleanupTimeState("RowTimeBoundedRowsOverCleanupTime");
	}

	@Override
	public void processElement(
			RowData input,
			KeyedProcessFunction<K, RowData, RowData>.Context ctx,
			Collector<RowData> out) throws Exception {
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
			List<RowData> data = inputState.get(triggeringTs);
			if (null != data) {
				data.add(input);
				inputState.put(triggeringTs, data);
			} else {
				data = new ArrayList<RowData>();
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
			KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
			Collector<RowData> out) throws Exception {
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
					// We clean the state
					cleanupState(inputState, accState, counterState, lastTriggeringTsState);
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
		List<RowData> inputs = inputState.get(timestamp);

		if (null != inputs) {

			Long dataCount = counterState.value();
			if (dataCount == null) {
				dataCount = 0L;
			}

			RowData accumulators = accState.value();
			if (accumulators == null) {
				accumulators = function.createAccumulators();
			}
			// set accumulators in context first
			function.setAccumulators(accumulators);

			List<RowData> retractList = null;
			long retractTs = Long.MAX_VALUE;
			int retractCnt = 0;
			int i = 0;

			while (i < inputs.size()) {
				RowData input = inputs.get(i);
				RowData retractRow = null;
				if (dataCount >= precedingOffset) {
					if (null == retractList) {
						// find the smallest timestamp
						retractTs = Long.MAX_VALUE;
						for (Long dataTs : inputState.keys()) {
							if (dataTs < retractTs) {
								retractTs = dataTs;
								// get the oldest rows to retract them
								retractList = inputState.get(dataTs);
							}
						}
					}

					if (retractList != null) {
						retractRow = retractList.get(retractCnt);
						retractCnt += 1;

						// remove retracted values from state
						if (retractList.size() == retractCnt) {
							inputState.remove(retractTs);
							retractList = null;
							retractCnt = 0;
						}
					}
				} else {
					dataCount += 1;
				}

				// retract old row from accumulators
				if (null != retractRow) {
					function.retract(retractRow);
				}

				// accumulate current row
				function.accumulate(input);

				// prepare output row
				output.replace(input, function.getValue());
				out.collect(output);

				i += 1;
			}

			// update all states
			if (inputState.contains(retractTs)) {
				if (retractCnt > 0) {
					retractList.subList(0, retractCnt).clear();
					inputState.put(retractTs, retractList);
				}
			}
			counterState.update(dataCount);
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
