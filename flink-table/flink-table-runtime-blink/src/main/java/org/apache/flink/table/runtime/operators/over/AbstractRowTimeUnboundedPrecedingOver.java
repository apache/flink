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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * A basic implementation to support unbounded event-time over-window.
 */
public abstract class AbstractRowTimeUnboundedPrecedingOver<K> extends KeyedProcessFunctionWithCleanupState<K, RowData, RowData> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractRowTimeUnboundedPrecedingOver.class);

	private final GeneratedAggsHandleFunction genAggsHandler;
	private final LogicalType[] accTypes;
	private final LogicalType[] inputFieldTypes;
	private final int rowTimeIdx;

	protected transient JoinedRowData output;
	// state to hold the accumulators of the aggregations
	private transient ValueState<RowData> accState;
	// state to hold rows until the next watermark arrives
	private transient MapState<Long, List<RowData>> inputState;
	// list to sort timestamps to access rows in timestamp order
	private transient LinkedList<Long> sortedTimestamps;

	protected transient AggsHandleFunction function;

	public AbstractRowTimeUnboundedPrecedingOver(
			long minRetentionTime,
			long maxRetentionTime,
			GeneratedAggsHandleFunction genAggsHandler,
			LogicalType[] accTypes,
			LogicalType[] inputFieldTypes,
			int rowTimeIdx) {
		super(minRetentionTime, maxRetentionTime);
		this.genAggsHandler = genAggsHandler;
		this.accTypes = accTypes;
		this.inputFieldTypes = inputFieldTypes;
		this.rowTimeIdx = rowTimeIdx;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

		output = new JoinedRowData();

		sortedTimestamps = new LinkedList<Long>();

		// initialize accumulator state
		RowDataTypeInfo accTypeInfo = new RowDataTypeInfo(accTypes);
		ValueStateDescriptor<RowData> accStateDesc =
			new ValueStateDescriptor<RowData>("accState", accTypeInfo);
		accState = getRuntimeContext().getState(accStateDesc);

		// input element are all binary row as they are came from network
		RowDataTypeInfo inputType = new RowDataTypeInfo(inputFieldTypes);
		ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<RowData>(inputType);
		MapStateDescriptor<Long, List<RowData>> inputStateDesc = new MapStateDescriptor<Long, List<RowData>>(
			"inputState",
			Types.LONG,
			rowListTypeInfo);
		inputState = getRuntimeContext().getMapState(inputStateDesc);

		initCleanupTimeState("RowTimeUnboundedOverCleanupTime");
	}

	/**
	 * Puts an element from the input stream into state if it is not late.
	 * Registers a timer for the next watermark.
	 *
	 * @param input The input value.
	 * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting
	 *              TimerService for registering timers and querying the time. The
	 *              context is only valid during the invocation of this method, do not store it.
	 * @param out   The collector for returning result values.
	 * @throws Exception
	 */
	@Override
	public void processElement(
			RowData input,
			KeyedProcessFunction<K, RowData, RowData>.Context ctx,
			Collector<RowData> out) throws Exception {
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());

		long timestamp = input.getLong(rowTimeIdx);
		long curWatermark = ctx.timerService().currentWatermark();

		// discard late record
		if (timestamp > curWatermark) {
			// ensure every key just registers one timer
			// default watermark is Long.Min, avoid overflow we use zero when watermark < 0
			long triggerTs = curWatermark < 0 ? 0 : curWatermark + 1;
			ctx.timerService().registerEventTimeTimer(triggerTs);

			// put row into state
			List<RowData> rowList = inputState.get(timestamp);
			if (rowList == null) {
				rowList = new ArrayList<RowData>();
			}
			rowList.add(input);
			inputState.put(timestamp, rowList);
		}
	}

	@Override
	public void onTimer(
			long timestamp,
			KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
			Collector<RowData> out) throws Exception {
		if (isProcessingTimeTimer(ctx)) {
			if (stateCleaningEnabled) {

				// we check whether there are still records which have not been processed yet
				if (inputState.isEmpty()) {
					// we clean the state
					cleanupState(inputState, accState);
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

		Iterator<Long> keyIterator = inputState.keys().iterator();
		if (keyIterator.hasNext()) {
			Long curWatermark = ctx.timerService().currentWatermark();
			boolean existEarlyRecord = false;

			// sort the record timestamps
			do {
				Long recordTime = keyIterator.next();
				// only take timestamps smaller/equal to the watermark
				if (recordTime <= curWatermark) {
					insertToSortedList(recordTime);
				} else {
					existEarlyRecord = true;
				}
			} while (keyIterator.hasNext());

			// get last accumulator
			RowData lastAccumulator = accState.value();
			if (lastAccumulator == null) {
				// initialize accumulator
				lastAccumulator = function.createAccumulators();
			}
			// set accumulator in function context first
			function.setAccumulators(lastAccumulator);

			// emit the rows in order
			while (!sortedTimestamps.isEmpty()) {
				Long curTimestamp = sortedTimestamps.removeFirst();
				List<RowData> curRowList = inputState.get(curTimestamp);
				if (curRowList != null) {
					// process the same timestamp datas, the mechanism is different according ROWS or RANGE
					processElementsWithSameTimestamp(curRowList, out);
				} else {
					// Ignore the same timestamp datas if the state is cleared already.
					LOG.warn("The state is cleared because of state ttl. " +
						"This will result in incorrect result. " +
						"You can increase the state ttl to avoid this.");
				}
				inputState.remove(curTimestamp);
			}

			// update acc state
			lastAccumulator = function.getAccumulators();
			accState.update(lastAccumulator);

			// if are are rows with timestamp > watermark, register a timer for the next watermark
			if (existEarlyRecord) {
				ctx.timerService().registerEventTimeTimer(curWatermark + 1);
			}
		}

		// update cleanup timer
		registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());
	}

	/**
	 * Inserts timestamps in order into a linked list.
	 * If timestamps arrive in order (as in case of using the RocksDB state backend) this is just
	 * an append with O(1).
	 */
	private void insertToSortedList(Long recordTimestamp) {
		ListIterator<Long> listIterator = sortedTimestamps.listIterator(sortedTimestamps.size());
		boolean isContinue = true;
		while (listIterator.hasPrevious() && isContinue) {
			Long timestamp = listIterator.previous();
			if (recordTimestamp >= timestamp) {
				listIterator.next();
				listIterator.add(recordTimestamp);
				isContinue = false;
			}
		}

		if (isContinue) {
			sortedTimestamps.addFirst(recordTimestamp);
		}
	}

	/**
	 * Process the same timestamp datas, the mechanism is different between
	 * rows and range window.
	 */
	protected abstract void processElementsWithSameTimestamp(
		List<RowData> curRowList,
		Collector<RowData> out) throws Exception;

	@Override
	public void close() throws Exception {
		if (null != function) {
			function.close();
		}
	}
}
