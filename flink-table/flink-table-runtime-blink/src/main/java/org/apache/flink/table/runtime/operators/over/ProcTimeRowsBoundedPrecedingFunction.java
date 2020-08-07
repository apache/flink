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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
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
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Process Function for ROW clause processing-time bounded OVER window.
 *
 * <p>E.g.:
 * SELECT currtime, b, c,
 * min(c) OVER
 * (PARTITION BY b ORDER BY proctime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
 * max(c) OVER
 * (PARTITION BY b ORDER BY proctime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
 * FROM T.
 */
public class ProcTimeRowsBoundedPrecedingFunction<K> extends KeyedProcessFunctionWithCleanupState<K, RowData, RowData> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ProcTimeRowsBoundedPrecedingFunction.class);

	private final GeneratedAggsHandleFunction genAggsHandler;
	private final LogicalType[] accTypes;
	private final LogicalType[] inputFieldTypes;
	private final long precedingOffset;

	private transient AggsHandleFunction function;

	private transient ValueState<RowData> accState;
	private transient MapState<Long, List<RowData>> inputState;
	private transient ValueState<Long> counterState;
	private transient ValueState<Long> smallestTsState;

	private transient JoinedRowData output;

	public ProcTimeRowsBoundedPrecedingFunction(
			long minRetentionTime,
			long maxRetentionTime,
			GeneratedAggsHandleFunction genAggsHandler,
			LogicalType[] accTypes,
			LogicalType[] inputFieldTypes,
			long precedingOffset) {
		super(minRetentionTime, maxRetentionTime);
		Preconditions.checkArgument(precedingOffset > 0);
		this.genAggsHandler = genAggsHandler;
		this.accTypes = accTypes;
		this.inputFieldTypes = inputFieldTypes;
		this.precedingOffset = precedingOffset;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

		output = new JoinedRowData();

		// input element are all binary row as they are came from network
		InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
		// We keep the elements received in a Map state keyed
		// by the ingestion time in the operator.
		// we also keep counter of processed elements
		// and timestamp of oldest element
		ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<RowData>(inputType);
		MapStateDescriptor<Long, List<RowData>> mapStateDescriptor = new MapStateDescriptor<Long, List<RowData>>(
			"inputState", BasicTypeInfo.LONG_TYPE_INFO, rowListTypeInfo);
		inputState = getRuntimeContext().getMapState(mapStateDescriptor);

		InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
		ValueStateDescriptor<RowData> stateDescriptor =
			new ValueStateDescriptor<RowData>("accState", accTypeInfo);
		accState = getRuntimeContext().getState(stateDescriptor);

		ValueStateDescriptor<Long> processedCountDescriptor = new ValueStateDescriptor<Long>(
			"processedCountState",
			Types.LONG);
		counterState = getRuntimeContext().getState(processedCountDescriptor);

		ValueStateDescriptor<Long> smallestTimestampDescriptor = new ValueStateDescriptor<Long>(
			"smallestTSState",
			Types.LONG);
		smallestTsState = getRuntimeContext().getState(smallestTimestampDescriptor);

		initCleanupTimeState("ProcTimeBoundedRowsOverCleanupTime");
	}

	@Override
	public void processElement(
			RowData input,
			KeyedProcessFunction<K, RowData, RowData>.Context ctx,
			Collector<RowData> out) throws Exception {
		long currentTime = ctx.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, currentTime);

		// initialize state for the processed element
		RowData accumulators = accState.value();
		if (accumulators == null) {
			accumulators = function.createAccumulators();
		}
		// set accumulators in context first
		function.setAccumulators(accumulators);

		// get smallest timestamp
		Long smallestTs = smallestTsState.value();
		if (smallestTs == null) {
			smallestTs = currentTime;
			smallestTsState.update(smallestTs);
		}
		// get previous counter value
		Long counter = counterState.value();
		if (counter == null) {
			counter = 0L;
		}

		if (counter == precedingOffset) {
			List<RowData> retractList = inputState.get(smallestTs);
			if (retractList != null) {
				// get oldest element beyond buffer size
				// and if oldest element exist, retract value
				RowData retractRow = retractList.get(0);
				function.retract(retractRow);
				retractList.remove(0);
			} else {
				// Does not retract values which are outside of window if the state is cleared already.
				LOG.warn("The state is cleared because of state ttl. " +
					"This will result in incorrect result. " +
					"You can increase the state ttl to avoid this.");
			}
			// if reference timestamp list not empty, keep the list
			if (retractList != null && !retractList.isEmpty()) {
				inputState.put(smallestTs, retractList);
			} // if smallest timestamp list is empty, remove and find new smallest
			else {
				inputState.remove(smallestTs);
				Iterator<Long> iter = inputState.keys().iterator();
				long currentTs = 0L;
				long newSmallestTs = Long.MAX_VALUE;
				while (iter.hasNext()) {
					currentTs = iter.next();
					if (currentTs < newSmallestTs) {
						newSmallestTs = currentTs;
					}
				}
				smallestTsState.update(newSmallestTs);
			}
		} // we update the counter only while buffer is getting filled
		else {
			counter += 1;
			counterState.update(counter);
		}

		// update map state, counter and timestamp
		List<RowData> currentTimeState = inputState.get(currentTime);
		if (currentTimeState != null) {
			currentTimeState.add(input);
			inputState.put(currentTime, currentTimeState);
		} else { // add new input
			List<RowData> newList = new ArrayList<RowData>();
			newList.add(input);
			inputState.put(currentTime, newList);
		}

		// accumulate current row
		function.accumulate(input);
		// update the value of accumulators for future incremental computation
		accumulators = function.getAccumulators();
		accState.update(accumulators);

		// prepare output row
		RowData aggValue = function.getValue();
		output.replace(input, aggValue);
		out.collect(output);
	}

	@Override
	public void onTimer(
			long timestamp,
			KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
			Collector<RowData> out) throws Exception {
		if (stateCleaningEnabled) {
			cleanupState(inputState, accState, counterState, smallestTsState);
			function.cleanup();
		}
	}

	@Override
	public void close() throws Exception {
		if (null != function) {
			function.close();
		}
	}
}
