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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.ACCUMULATE_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.RETRACT_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.isAccumulateMsg;

/**
 * Aggregate Function used for the groupby (without window) aggregate in miniBatch mode.
 *
 * <p>This function buffers input row in heap HashMap, and aggregates them when minibatch invoked.
 */
public class MiniBatchGroupAggFunction extends MapBundleFunction<BaseRow, List<BaseRow>, BaseRow, BaseRow> {

	private static final long serialVersionUID = 7455939331036508477L;

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
	 * The input row type.
	 */
	private final RowType inputType;

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
	private transient JoinedRow resultRow = new JoinedRow();

	// serializer used to deep copy input row
	private transient TypeSerializer<BaseRow> inputRowSerializer;

	// function used to handle all aggregates
	private transient AggsHandleFunction function = null;

	// function used to equal BaseRow
	private transient RecordEqualiser equaliser = null;

	// stores the accumulators
	private transient ValueState<BaseRow> accState = null;

	/**
	 * Creates a {@link MiniBatchGroupAggFunction}.
	 *
	 * @param genAggsHandler The code generated function used to handle aggregates.
	 * @param genRecordEqualiser The code generated equaliser used to equal BaseRow.
	 * @param accTypes The accumulator types.
	 * @param inputType The input row type.
	 * @param indexOfCountStar The index of COUNT(*) in the aggregates.
	 *                          -1 when the input doesn't contain COUNT(*), i.e. doesn't contain retraction messages.
	 *                          We make sure there is a COUNT(*) if input stream contains retraction.
	 * @param generateRetraction Whether this operator will generate retraction.
	 */
	public MiniBatchGroupAggFunction(
			GeneratedAggsHandleFunction genAggsHandler,
			GeneratedRecordEqualiser genRecordEqualiser,
			LogicalType[] accTypes,
			RowType inputType,
			int indexOfCountStar,
			boolean generateRetraction) {
		this.genAggsHandler = genAggsHandler;
		this.genRecordEqualiser = genRecordEqualiser;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
		this.accTypes = accTypes;
		this.inputType = inputType;
		this.generateRetraction = generateRetraction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		// instantiate function
		function = genAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));
		// instantiate equaliser
		equaliser = genRecordEqualiser.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());

		BaseRowTypeInfo accTypeInfo = new BaseRowTypeInfo(accTypes);
		ValueStateDescriptor<BaseRow> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
		accState = ctx.getRuntimeContext().getState(accDesc);

		//noinspection unchecked
		inputRowSerializer = (TypeSerializer) InternalSerializers.create(
				inputType, ctx.getRuntimeContext().getExecutionConfig());

		resultRow = new JoinedRow();
	}

	@Override
	public List<BaseRow> addInput(@Nullable List<BaseRow> value, BaseRow input) throws Exception {
		List<BaseRow> bufferedRows = value;
		if (value == null) {
			bufferedRows = new ArrayList<>();
		}
		// input row maybe reused, we need deep copy here
		bufferedRows.add(inputRowSerializer.copy(input));
		return bufferedRows;
	}

	@Override
	public void finishBundle(Map<BaseRow, List<BaseRow>> buffer, Collector<BaseRow> out) throws Exception {
		for (Map.Entry<BaseRow, List<BaseRow>> entry : buffer.entrySet()) {
			BaseRow currentKey = entry.getKey();
			List<BaseRow> inputRows = entry.getValue();

			boolean firstRow = false;

			// step 1: get the accumulator for the current key

			// set current key to access state under the key
			ctx.setCurrentKey(currentKey);
			BaseRow acc = accState.value();
			if (acc == null) {
				acc = function.createAccumulators();
				firstRow = true;
			}

			// step 2: accumulate
			function.setAccumulators(acc);

			// get previous aggregate result
			BaseRow prevAggValue = function.getValue();

			for (BaseRow input : inputRows) {
				if (isAccumulateMsg(input)) {
					function.accumulate(input);
				} else {
					function.retract(input);
				}
			}

			// get current aggregate result
			BaseRow newAggValue = function.getValue();

			// get updated accumulator
			acc = function.getAccumulators();

			if (!recordCounter.recordCountIsZero(acc)) {
				// we aggregated at least one record for this key

				// update acc to state
				accState.update(acc);

				// if this was not the first row and we have to emit retractions
				if (!firstRow) {
					if (!equaliser.equalsWithoutHeader(prevAggValue, newAggValue)) {
						// new row is not same with prev row
						if (generateRetraction) {
							// prepare retraction message for previous row
							resultRow.replace(currentKey, prevAggValue).setHeader(RETRACT_MSG);
							out.collect(resultRow);
						}
						// prepare accumulation message for new row
						resultRow.replace(currentKey, newAggValue).setHeader(ACCUMULATE_MSG);
						out.collect(resultRow);
					}
					// new row is same with prev row, no need to output
				} else {
					// this is the first, output new result

					// prepare accumulation message for new row
					resultRow.replace(currentKey, newAggValue).setHeader(ACCUMULATE_MSG);
					out.collect(resultRow);
				}

			} else {
				// we retracted the last record for this key
				// if this is not first row sent out a delete message
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
	}

	@Override
	public void close() throws Exception {
		if (function != null) {
			function.close();
		}
	}
}
