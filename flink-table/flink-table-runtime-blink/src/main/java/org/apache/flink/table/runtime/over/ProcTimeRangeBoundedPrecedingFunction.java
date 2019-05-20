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

package org.apache.flink.table.runtime.over;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.generated.AggsHandleFunction;
import org.apache.flink.table.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Process Function used for the aggregate in bounded proc-time OVER window.
 *
 * <p>E.g.:
 * SELECT currtime, b, c,
 * min(c) OVER
 * (PARTITION BY b ORDER BY proctime
 * RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW),
 * max(c) OVER
 * (PARTITION BY b ORDER BY proctime
 * RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW)
 * FROM T.
 */
public class ProcTimeRangeBoundedPrecedingFunction<K> extends KeyedProcessFunctionWithCleanupState<K, BaseRow, BaseRow> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ProcTimeRangeBoundedPrecedingFunction.class);

	private final GeneratedAggsHandleFunction genAggsHandler;
	private final InternalType[] accTypes;
	private final InternalType[] inputFieldTypes;
	private final long precedingTimeBoundary;

	private transient ValueState<BaseRow> accState;
	private transient MapState<Long, List<BaseRow>> inputState;

	private transient AggsHandleFunction function;
	private transient JoinedRow output;

	public ProcTimeRangeBoundedPrecedingFunction(
			long minRetentionTime,
			long maxRetentionTime,
			GeneratedAggsHandleFunction genAggsHandler,
			InternalType[] accTypes,
			InternalType[] inputFieldTypes,
			long precedingTimeBoundary) {
		super(minRetentionTime, maxRetentionTime);
		this.genAggsHandler = genAggsHandler;
		this.accTypes = accTypes;
		this.inputFieldTypes = inputFieldTypes;
		this.precedingTimeBoundary = precedingTimeBoundary;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

		output = new JoinedRow();

		// input element are all binary row as they are came from network
		BaseRowTypeInfo inputType = new BaseRowTypeInfo(inputFieldTypes);
		// we keep the elements received in a map state indexed based on their ingestion time
		ListTypeInfo<BaseRow> rowListTypeInfo = new ListTypeInfo<BaseRow>(inputType);
		MapStateDescriptor<Long, List<BaseRow>> mapStateDescriptor = new MapStateDescriptor<Long, List<BaseRow>>(
			"inputState", BasicTypeInfo.LONG_TYPE_INFO, rowListTypeInfo);
		inputState = getRuntimeContext().getMapState(mapStateDescriptor);

		BaseRowTypeInfo accTypeInfo = new BaseRowTypeInfo(accTypes);
		ValueStateDescriptor<BaseRow> stateDescriptor =
			new ValueStateDescriptor<BaseRow>("accState", accTypeInfo);
		accState = getRuntimeContext().getState(stateDescriptor);

		initCleanupTimeState("ProcTimeBoundedRangeOverCleanupTime");
	}

	@Override
	public void processElement(
			BaseRow input,
			KeyedProcessFunction<K, BaseRow, BaseRow>.Context ctx,
			Collector<BaseRow> out) throws Exception {
		long currentTime = ctx.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, currentTime);

		// buffer the event incoming event

		// add current element to the window list of elements with corresponding timestamp
		List<BaseRow> rowList = inputState.get(currentTime);
		// null value means that this si the first event received for this timestamp
		if (rowList == null) {
			rowList = new ArrayList<BaseRow>();
			// register timer to process event once the current millisecond passed
			ctx.timerService().registerProcessingTimeTimer(currentTime + 1);
		}
		rowList.add(input);
		inputState.put(currentTime, rowList);
	}

	@Override
	public void onTimer(
			long timestamp,
			KeyedProcessFunction<K, BaseRow, BaseRow>.OnTimerContext ctx,
			Collector<BaseRow> out) throws Exception {
		if (needToCleanupState(timestamp)) {
			// clean up and return
			cleanupState(inputState, accState);
			function.cleanup();
			return;
		}

		// remove timestamp set outside of ProcessFunction.
		((TimestampedCollector) out).eraseTimestamp();

		// we consider the original timestamp of events
		// that have registered this time trigger 1 ms ago

		long currentTime = timestamp - 1;

		// get the list of elements of current proctime
		List<BaseRow> currentElements = inputState.get(currentTime);

		// Expired clean-up timers pass the needToCleanupState check.
		// Perform a null check to verify that we have data to process.
		if (null == currentElements) {
			return;
		}

		// initialize the accumulators
		BaseRow accumulators = accState.value();

		if (null == accumulators) {
			accumulators = function.createAccumulators();
		}

		// set accumulators in context first
		function.setAccumulators(accumulators);

		// update the elements to be removed and retract them from aggregators
		long limit = currentTime - precedingTimeBoundary;

		// we iterate through all elements in the window buffer based on timestamp keys
		// when we find timestamps that are out of interest, we retrieve corresponding elements
		// and eliminate them. Multiple elements could have been received at the same timestamp
		// the removal of old elements happens only once per proctime as onTimer is called only once
		Iterator<Long> iter = inputState.keys().iterator();
		List<Long> markToRemove = new ArrayList<Long>();
		while (iter.hasNext()) {
			Long elementKey = iter.next();
			if (elementKey < limit) {
				// element key outside of window. Retract values
				List<BaseRow> elementsRemove = inputState.get(elementKey);
				if (elementsRemove != null) {
					int iRemove = 0;
					while (iRemove < elementsRemove.size()) {
						BaseRow retractRow = elementsRemove.get(iRemove);
						function.retract(retractRow);
						iRemove += 1;
					}
				} else {
					// Does not retract values which are outside of window if the state is cleared already.
					LOG.warn("The state is cleared because of state ttl. " +
						"This will result in incorrect result. " +
						"You can increase the state ttl to avoid this.");
				}

				// mark element for later removal not to modify the iterator over MapState
				markToRemove.add(elementKey);
			}
		}

		// need to remove in 2 steps not to have concurrent access errors via iterator to the MapState
		int i = 0;
		while (i < markToRemove.size()) {
			inputState.remove(markToRemove.get(i));
			i += 1;
		}

		// add current elements to aggregator. Multiple elements might
		// have arrived in the same proctime
		// the same accumulator value will be computed for all elements
		int iElemenets = 0;
		while (iElemenets < currentElements.size()) {
			BaseRow input = currentElements.get(iElemenets);
			function.accumulate(input);
			iElemenets += 1;
		}

		// we need to build the output and emit for every event received at this proctime
		iElemenets = 0;
		BaseRow aggValue = function.getValue();
		while (iElemenets < currentElements.size()) {
			BaseRow input = currentElements.get(iElemenets);
			output.replace(input, aggValue);
			out.collect(output);
			iElemenets += 1;
		}

		// update the value of accumulators for future incremental computation
		accumulators = function.getAccumulators();
		accState.update(accumulators);
	}

	@Override
	public void close() throws Exception {
		if (null != function) {
			function.close();
		}
	}
}
