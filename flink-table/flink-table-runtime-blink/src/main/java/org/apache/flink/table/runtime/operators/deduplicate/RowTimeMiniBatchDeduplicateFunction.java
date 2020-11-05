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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.checkInsertOnly;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.isDuplicate;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.updateDeduplicateResult;

/**
 * This function is used to get the first or last row for every key partition in miniBatch mode.
 */
public class RowTimeMiniBatchDeduplicateFunction
		extends MiniBatchDeduplicateFunctionBase<RowData, RowData, List<RowData>, RowData, RowData> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<RowData> serializer;
	private final boolean generateUpdateBefore;
	private final boolean generateInsert;
	private final int rowtimeIndex;
	private final boolean keepLastRow;

	public RowTimeMiniBatchDeduplicateFunction(
			InternalTypeInfo<RowData> typeInfo,
			TypeSerializer<RowData> serializer,
			long minRetentionTime,
			int rowtimeIndex,
			boolean generateUpdateBefore,
			boolean generateInsert,
			boolean keepLastRow) {
		super(typeInfo, minRetentionTime);
		this.serializer = serializer;
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
		this.rowtimeIndex = rowtimeIndex;
		this.keepLastRow = keepLastRow;
	}

	@Override
	public List<RowData> addInput(@Nullable List<RowData> value, RowData input) throws Exception {
		if (value == null) {
			value = new ArrayList<>();
		}
		value.add(serializer.copy(input));
		return value;
	}

	@Override
	public void finishBundle(Map<RowData, List<RowData>> buffer, Collector<RowData> out) throws Exception {
		for (Map.Entry<RowData, List<RowData>> entry : buffer.entrySet()) {
			RowData currentKey = entry.getKey();
			List<RowData> bufferedRows = entry.getValue();
			ctx.setCurrentKey(currentKey);
			miniBatchDeduplicateOnRowTime(
					state,
					bufferedRows,
					out,
					generateUpdateBefore,
					generateInsert,
					rowtimeIndex,
					keepLastRow);
		}
	}

	/**
	 * Processes element to deduplicate on keys with row time semantic, sends current element if it is last
	 * or first row, retracts previous element if needed.
	 *
	 * @param state                 state of function
	 * @param bufferedRows          latest row received by deduplicate function
	 * @param out                   underlying collector
	 * @param generateUpdateBefore  flag to generate UPDATE_BEFORE message or not
	 * @param generateInsert        flag to gennerate INSERT message or not
	 * @param rowtimeIndex          the index of rowtime field
	 * @param keepLastRow			flag to keep last row or keep first row
	 */
	private static void miniBatchDeduplicateOnRowTime(
			ValueState<RowData> state,
			List<RowData> bufferedRows,
			Collector<RowData> out,
			boolean generateUpdateBefore,
			boolean generateInsert,
			int rowtimeIndex,
			boolean keepLastRow) throws Exception {
		if (bufferedRows.isEmpty()) {
			return;
		}
		RowData preRow = state.value();
		//Note: we output all changelog here rather than comparing the first and the last
		// record in buffer then output at most two changelog.
		// The motivation is we need all changelog in versioned table of temporal join.
		for (RowData currentRow : bufferedRows) {
			checkInsertOnly(currentRow);
			if (isDuplicate(preRow, currentRow, rowtimeIndex, keepLastRow)){
				updateDeduplicateResult(
						generateUpdateBefore,
						generateInsert,
						preRow,
						currentRow,
						out);
				preRow = currentRow;
			}
		}
		state.update(preRow);
	}
}
