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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.ACCUMULATE_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.RETRACT_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.isAccumulateMsg;

/**
 * Aggregate Function used for the groupby (without window) aggregate.
 */
public class GroupAggFunction extends KeyedProcessFunctionWithCleanupState<BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = -4767158666069797704L;

	/**
	 * The code generated function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genAggsHandler;

	/**
	 * The code generated equaliser used to equal BaseRow.
	 */
	private final GeneratedRecordEqualiser genRecordEqualiser;

	/**
	 * The accumulator types.
	 */
	private final LogicalType[] accTypes;

	/**
	 * Used to count the number of added and retracted input records.
	 */
	private final RecordCounter recordCounter;

	/**
	 * Whether this operator will generate retraction.
	 */
	private final boolean generateRetraction;

	/**
	 * Reused output row.
	 */
	private transient JoinedRow resultRow = null;

	// function used to handle all aggregates
	private transient AggsHandleFunction function = null;

	// function used to equal BaseRow
	private transient RecordEqualiser equaliser = null;

	// stores the accumulators
	private transient ValueState<BaseRow> accState = null;

	/**
	 * Creates a {@link GroupAggFunction}.
	 *
	 * @param minRetentionTime minimal state idle retention time.
	 * @param maxRetentionTime maximal state idle retention time.
	 * @param genAggsHandler The code generated function used to handle aggregates.
	 * @param genRecordEqualiser The code generated equaliser used to equal BaseRow.
	 * @param accTypes The accumulator types.
	 * @param indexOfCountStar The index of COUNT(*) in the aggregates.
	 *                          -1 when the input doesn't contain COUNT(*), i.e. doesn't contain retraction messages.
	 *                          We make sure there is a COUNT(*) if input stream contains retraction.
	 * @param generateRetraction Whether this operator will generate retraction.
	 */
	public GroupAggFunction(
			long minRetentionTime,
			long maxRetentionTime,
			GeneratedAggsHandleFunction genAggsHandler,
			GeneratedRecordEqualiser genRecordEqualiser,
			LogicalType[] accTypes,
			int indexOfCountStar,
			boolean generateRetraction) {
		super(minRetentionTime, maxRetentionTime);
		this.genAggsHandler = genAggsHandler;
		this.genRecordEqualiser = genRecordEqualiser;
		this.accTypes = accTypes;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
		this.generateRetraction = generateRetraction;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		// instantiate function
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(getRuntimeContext()));
		// instantiate equaliser
		equaliser = genRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

		BaseRowTypeInfo accTypeInfo = new BaseRowTypeInfo(accTypes);
		ValueStateDescriptor<BaseRow> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
		accState = getRuntimeContext().getState(accDesc);

		initCleanupTimeState("GroupAggregateCleanupTime");

		resultRow = new JoinedRow();
	}

	@Override
	public void processElement(BaseRow input, Context ctx, Collector<BaseRow> out) throws Exception {
		long currentTime = ctx.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, currentTime);

		BaseRow currentKey = ctx.getCurrentKey();

		boolean firstRow;
		BaseRow accumulators = accState.value();
		if (null == accumulators) {
			firstRow = true;
			accumulators = function.createAccumulators();
		} else {
			firstRow = false;
		}

		// set accumulators to handler first
		function.setAccumulators(accumulators);
		// get previous aggregate result
		BaseRow prevAggValue = function.getValue();

		// update aggregate result and set to the newRow
		if (isAccumulateMsg(input)) {
			// accumulate input
			function.accumulate(input);
		} else {
			// retract input
			function.retract(input);
		}
		// get current aggregate result
		BaseRow newAggValue = function.getValue();

		// get accumulator
		accumulators = function.getAccumulators();

		if (!recordCounter.recordCountIsZero(accumulators)) {
			// we aggregated at least one record for this key

			// update the state
			accState.update(accumulators);

			// if this was not the first row and we have to emit retractions
			if (!firstRow) {
				if (!stateCleaningEnabled && equaliser.equalsWithoutHeader(prevAggValue, newAggValue)) {
					// newRow is the same as before and state cleaning is not enabled.
					// We do not emit retraction and acc message.
					// If state cleaning is enabled, we have to emit messages to prevent too early
					// state eviction of downstream operators.
					return;
				} else {
					// retract previous result
					if (generateRetraction) {
						// prepare retraction message for previous row
						resultRow.replace(currentKey, prevAggValue).setHeader(RETRACT_MSG);
						out.collect(resultRow);
					}
				}
			}
			// emit the new result
			resultRow.replace(currentKey, newAggValue).setHeader(ACCUMULATE_MSG);
			out.collect(resultRow);

		} else {
			// we retracted the last record for this key
			// sent out a delete message
			if (!firstRow) {
				// prepare delete message for previous row
				resultRow.replace(currentKey, prevAggValue).setHeader(RETRACT_MSG);
				out.collect(resultRow);
			}
			// and clear all state
			accState.clear();
			// cleanup dataview under current key
			function.cleanup();
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<BaseRow> out) throws Exception {
		if (stateCleaningEnabled) {
			cleanupState(accState);
			function.cleanup();
		}
	}

	@Override
	public void close() throws Exception {
		if (function != null) {
			function.close();
		}
	}
}
