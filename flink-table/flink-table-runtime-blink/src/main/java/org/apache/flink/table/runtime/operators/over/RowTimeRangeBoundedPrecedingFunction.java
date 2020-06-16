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
public class RowTimeRangeBoundedPrecedingFunction<K> extends KeyedProcessFunction<K, RowData, RowData> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RowTimeRangeBoundedPrecedingFunction.class);

	private final GeneratedAggsHandleFunction genAggsHandler;
	private final LogicalType[] accTypes;
	private final LogicalType[] inputFieldTypes;
	private final long precedingOffset;
	private final int rowTimeIdx;

	private transient JoinedRowData output;

	// the state which keeps the last triggering timestamp
	private transient ValueState<Long> lastTriggeringTsState;

	// the state which used to materialize the accumulator for incremental calculation
	private transient ValueState<RowData> accState;

	// the state which keeps the safe timestamp to cleanup states
	private transient ValueState<Long> cleanupTsState;

	// the state which keeps all the data that are not expired.
	// The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
	// the second element of tuple is a list that contains the entire data of all the rows belonging
	// to this time stamp.
	private transient MapState<Long, List<RowData>> inputState;

	private transient AggsHandleFunction function;

	public RowTimeRangeBoundedPrecedingFunction(
			GeneratedAggsHandleFunction genAggsHandler,
			LogicalType[] accTypes,
			LogicalType[] inputFieldTypes,
			long precedingOffset,
			int rowTimeIdx) {
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

		ValueStateDescriptor<Long> cleanupTsStateDescriptor = new ValueStateDescriptor<>(
			"cleanupTsState",
			Types.LONG
		);
		this.cleanupTsState = getRuntimeContext().getState(cleanupTsStateDescriptor);
	}

	@Override
	public void processElement(
			RowData input,
			KeyedProcessFunction<K, RowData, RowData>.Context ctx,
			Collector<RowData> out) throws Exception {
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
			registerCleanupTimer(ctx, triggeringTs);
		}
	}

	private void registerCleanupTimer(
			KeyedProcessFunction<K, RowData, RowData>.Context ctx,
			long timestamp) throws Exception {
		// calculate safe timestamp to cleanup states
		long minCleanupTimestamp = timestamp + precedingOffset + 1;
		long maxCleanupTimestamp = timestamp + (long) (precedingOffset * 1.5) + 1;
		// update timestamp and register timer if needed
		Long curCleanupTimestamp = cleanupTsState.value();
		if (curCleanupTimestamp == null || curCleanupTimestamp < minCleanupTimestamp) {
			// we don't delete existing timer since it may delete timer for data processing
			// TODO Use timer with namespace to distinguish timers
			ctx.timerService().registerEventTimeTimer(maxCleanupTimestamp);
			cleanupTsState.update(maxCleanupTimestamp);
		}
	}

	@Override
	public void onTimer(
			long timestamp,
			KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
			Collector<RowData> out) throws Exception {
		Long cleanupTimestamp = cleanupTsState.value();
		// if cleanupTsState has not been updated then it is safe to cleanup states
		if (cleanupTimestamp != null && cleanupTimestamp <= timestamp) {
			inputState.clear();
			accState.clear();
			lastTriggeringTsState.clear();
			cleanupTsState.clear();
			function.cleanup();
			return;
		}

		// gets all window data from state for the calculation
		List<RowData> inputs = inputState.get(timestamp);

		if (null != inputs) {

			int dataListIndex = 0;
			RowData accumulators = accState.value();

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
					List<RowData> retractDataList = inputState.get(dataTs);
					if (retractDataList != null) {
						dataListIndex = 0;
						while (dataListIndex < retractDataList.size()) {
							RowData retractRow = retractDataList.get(dataListIndex);
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
				RowData curRow = inputs.get(dataListIndex);
				// accumulate current row
				function.accumulate(curRow);
				dataListIndex += 1;
			}

			// get aggregate result
			RowData aggValue = function.getValue();

			// copy forwarded fields to output row and emit output row
			dataListIndex = 0;
			while (dataListIndex < inputs.size()) {
				RowData curRow = inputs.get(dataListIndex);
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
	}

	@Override
	public void close() throws Exception {
		if (null != function) {
			function.close();
		}
	}
}
