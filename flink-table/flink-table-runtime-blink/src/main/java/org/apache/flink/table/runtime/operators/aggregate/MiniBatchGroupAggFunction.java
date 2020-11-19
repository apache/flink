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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;
import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * Aggregate Function used for the groupby (without window) aggregate in miniBatch mode.
 *
 * <p>This function buffers input row in heap HashMap, and aggregates them when minibatch invoked.
 */
public class MiniBatchGroupAggFunction extends MapBundleFunction<RowData, List<RowData>, RowData, RowData> {

	private static final long serialVersionUID = 7455939331036508477L;

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
	 * The input row type.
	 */
	private final RowType inputType;

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
	private transient JoinedRowData resultRow = new JoinedRowData();

	// serializer used to deep copy input row
	private transient TypeSerializer<RowData> inputRowSerializer;

	// function used to handle all aggregates
	private transient AggsHandleFunction function = null;

	// function used to equal RowData
	private transient RecordEqualiser equaliser = null;

	// stores the accumulators
	private transient ValueState<RowData> accState = null;

	/**
	 * Creates a {@link MiniBatchGroupAggFunction}.
	 *
	 * @param genAggsHandler The code generated function used to handle aggregates.
	 * @param genRecordEqualiser The code generated equaliser used to equal RowData.
	 * @param accTypes The accumulator types.
	 * @param inputType The input row type.
	 * @param indexOfCountStar The index of COUNT(*) in the aggregates.
	 *                          -1 when the input doesn't contain COUNT(*), i.e. doesn't contain retraction messages.
	 *                          We make sure there is a COUNT(*) if input stream contains retraction.
	 * @param generateUpdateBefore Whether this operator will generate UPDATE_BEFORE messages.
	 * @param stateRetentionTime state idle retention time which unit is MILLISECONDS.
	 */
	public MiniBatchGroupAggFunction(
			GeneratedAggsHandleFunction genAggsHandler,
			GeneratedRecordEqualiser genRecordEqualiser,
			LogicalType[] accTypes,
			RowType inputType,
			int indexOfCountStar,
			boolean generateUpdateBefore,
			long stateRetentionTime) {
		this.genAggsHandler = genAggsHandler;
		this.genRecordEqualiser = genRecordEqualiser;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
		this.accTypes = accTypes;
		this.inputType = inputType;
		this.generateUpdateBefore = generateUpdateBefore;
		this.stateRetentionTime = stateRetentionTime;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		// instantiate function
		StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
		function = genAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext(), ttlConfig));
		// instantiate equaliser
		equaliser = genRecordEqualiser.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());

		InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
		ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
		if (ttlConfig.isEnabled()){
			accDesc.enableTimeToLive(ttlConfig);
		}
		accState = ctx.getRuntimeContext().getState(accDesc);

		inputRowSerializer = InternalSerializers.create(inputType);

		resultRow = new JoinedRowData();
	}

	@Override
	public List<RowData> addInput(@Nullable List<RowData> value, RowData input) throws Exception {
		List<RowData> bufferedRows = value;
		if (value == null) {
			bufferedRows = new ArrayList<>();
		}
		// input row maybe reused, we need deep copy here
		bufferedRows.add(inputRowSerializer.copy(input));
		return bufferedRows;
	}

	@Override
	public void finishBundle(Map<RowData, List<RowData>> buffer, Collector<RowData> out) throws Exception {
		for (Map.Entry<RowData, List<RowData>> entry : buffer.entrySet()) {
			RowData currentKey = entry.getKey();
			List<RowData> inputRows = entry.getValue();

			boolean firstRow = false;

			// step 1: get the accumulator for the current key

			// set current key to access state under the key
			ctx.setCurrentKey(currentKey);
			RowData acc = accState.value();
			if (acc == null) {
				// Don't create a new accumulator for a retraction message. This
				// might happen if the retraction message is the first message for the
				// key or after a state clean up.
				Iterator<RowData> inputIter = inputRows.iterator();
				while (inputIter.hasNext()) {
					RowData current = inputIter.next();
					if (isRetractMsg(current)) {
						inputIter.remove(); // remove all the beginning retraction messages
					} else {
						break;
					}
				}
				if (inputRows.isEmpty()) {
					return;
				}
				acc = function.createAccumulators();
				firstRow = true;
			}

			// step 2: accumulate
			function.setAccumulators(acc);

			// get previous aggregate result
			RowData prevAggValue = function.getValue();

			for (RowData input : inputRows) {
				if (isAccumulateMsg(input)) {
					function.accumulate(input);
				} else {
					function.retract(input);
				}
			}

			// get current aggregate result
			RowData newAggValue = function.getValue();

			// get updated accumulator
			acc = function.getAccumulators();

			if (!recordCounter.recordCountIsZero(acc)) {
				// we aggregated at least one record for this key

				// update acc to state
				accState.update(acc);

				// if this was not the first row and we have to emit retractions
				if (!firstRow) {
					if (!equaliser.equals(prevAggValue, newAggValue)) {
						// new row is not same with prev row
						if (generateUpdateBefore) {
							// prepare UPDATE_BEFORE message for previous row
							resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.UPDATE_BEFORE);
							out.collect(resultRow);
						}
						// prepare UPDATE_AFTER message for new row
						resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.UPDATE_AFTER);
						out.collect(resultRow);
					}
					// new row is same with prev row, no need to output
				} else {
					// this is the first, output new result
					// prepare INSERT message for new row
					resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.INSERT);
					out.collect(resultRow);
				}

			} else {
				// we retracted the last record for this key
				// if this is not first row sent out a DELETE message
				if (!firstRow) {
					// prepare DELETE message for previous row
					resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.DELETE);
					out.collect(resultRow);
				}
				// and clear all state
				accState.clear();
				// cleanup dataview under current key
				function.cleanup();
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (function != null) {
			function.close();
		}
	}
}
