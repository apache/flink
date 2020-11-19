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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;
import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * Aggregate Function used for the groupby (without window) aggregate.
 */
public class GroupAggFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

	private static final long serialVersionUID = -4767158666069797704L;

	/**
	 * The code generated function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genAggsHandler;

	/**
	 * The code generated equaliser used to equal RowData.
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
	 * Whether this operator will generate UPDATE_BEFORE messages.
	 */
	private final boolean generateUpdateBefore;

	/**
	 * State idle retention time which unit is MILLISECONDS.
	 */
	private final long stateRetentionTime;

	/**
	 * Reused output row.
	 */
	private transient JoinedRowData resultRow = null;

	// function used to handle all aggregates
	private transient AggsHandleFunction function = null;

	// function used to equal RowData
	private transient RecordEqualiser equaliser = null;

	// stores the accumulators
	private transient ValueState<RowData> accState = null;

	/**
	 * Creates a {@link GroupAggFunction}.
	 *
	 * @param genAggsHandler The code generated function used to handle aggregates.
	 * @param genRecordEqualiser The code generated equaliser used to equal RowData.
	 * @param accTypes The accumulator types.
	 * @param indexOfCountStar The index of COUNT(*) in the aggregates.
	 *                          -1 when the input doesn't contain COUNT(*), i.e. doesn't contain retraction messages.
	 *                          We make sure there is a COUNT(*) if input stream contains retraction.
	 * @param generateUpdateBefore Whether this operator will generate UPDATE_BEFORE messages.
	 * @param stateRetentionTime state idle retention time which unit is MILLISECONDS.
	 */
	public GroupAggFunction(
			GeneratedAggsHandleFunction genAggsHandler,
			GeneratedRecordEqualiser genRecordEqualiser,
			LogicalType[] accTypes,
			int indexOfCountStar,
			boolean generateUpdateBefore,
			long stateRetentionTime) {
		this.genAggsHandler = genAggsHandler;
		this.genRecordEqualiser = genRecordEqualiser;
		this.accTypes = accTypes;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
		this.generateUpdateBefore = generateUpdateBefore;
		this.stateRetentionTime = stateRetentionTime;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		// instantiate function
		StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(getRuntimeContext(), ttlConfig));
		// instantiate equaliser
		equaliser = genRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

		InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
		ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
		if (ttlConfig.isEnabled()){
			accDesc.enableTimeToLive(ttlConfig);
		}
		accState = getRuntimeContext().getState(accDesc);

		resultRow = new JoinedRowData();
	}

	@Override
	public void processElement(RowData input, Context ctx, Collector<RowData> out) throws Exception {
		RowData currentKey = ctx.getCurrentKey();
		boolean firstRow;
		RowData accumulators = accState.value();
		if (null == accumulators) {
			// Don't create a new accumulator for a retraction message. This
			// might happen if the retraction message is the first message for the
			// key or after a state clean up.
			if (isRetractMsg(input)) {
				return;
			}
			firstRow = true;
			accumulators = function.createAccumulators();
		} else {
			firstRow = false;
		}

		// set accumulators to handler first
		function.setAccumulators(accumulators);
		// get previous aggregate result
		RowData prevAggValue = function.getValue();

		// update aggregate result and set to the newRow
		if (isAccumulateMsg(input)) {
			// accumulate input
			function.accumulate(input);
		} else {
			// retract input
			function.retract(input);
		}
		// get current aggregate result
		RowData newAggValue = function.getValue();

		// get accumulator
		accumulators = function.getAccumulators();

		if (!recordCounter.recordCountIsZero(accumulators)) {
			// we aggregated at least one record for this key

			// update the state
			accState.update(accumulators);

			// if this was not the first row and we have to emit retractions
			if (!firstRow) {
				if (stateRetentionTime <= 0 && equaliser.equals(prevAggValue, newAggValue)) {
					// newRow is the same as before and state cleaning is not enabled.
					// We do not emit retraction and acc message.
					// If state cleaning is enabled, we have to emit messages to prevent too early
					// state eviction of downstream operators.
					return;
				} else {
					// retract previous result
					if (generateUpdateBefore) {
						// prepare UPDATE_BEFORE message for previous row
						resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.UPDATE_BEFORE);
						out.collect(resultRow);
					}
					// prepare UPDATE_AFTER message for new row
					resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.UPDATE_AFTER);
				}
			} else {
				// this is the first, output new result
				// prepare INSERT message for new row
				resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.INSERT);
			}

			out.collect(resultRow);

		} else {
			// we retracted the last record for this key
			// sent out a delete message
			if (!firstRow) {
				// prepare delete message for previous row
				resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.DELETE);
				out.collect(resultRow);
			}
			// and clear all state
			accState.clear();
			// cleanup dataview under current key
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
