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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.checkInsertOnly;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.isDuplicate;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.updateDeduplicateResult;

/**
 * This function is used to deduplicate on keys and keeps only first or last row on row time.
 */
public class RowTimeDeduplicateFunction
		extends DeduplicateFunctionBase<RowData, RowData, RowData, RowData> {

	private static final long serialVersionUID = 1L;

	private final boolean generateUpdateBefore;
	private final boolean generateInsert;
	private final int rowtimeIndex;
	private final boolean keepLastRow;

	public RowTimeDeduplicateFunction(
			InternalTypeInfo<RowData> typeInfo,
			long minRetentionTime,
			int rowtimeIndex,
			boolean generateUpdateBefore,
			boolean generateInsert,
			boolean keepLastRow) {
		super(typeInfo, null, minRetentionTime);
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
		this.rowtimeIndex = rowtimeIndex;
		this.keepLastRow = keepLastRow;
	}

	@Override
	public void processElement(RowData input, Context ctx, Collector<RowData> out) throws Exception {
		deduplicateOnRowTime(
				state,
				input,
				out,
				generateUpdateBefore,
				generateInsert,
				rowtimeIndex,
				keepLastRow);
	}

	/**
	 * Processes element to deduplicate on keys with row time semantic, sends current element if it is last
	 * or first row, retracts previous element if needed.
	 *
	 * @param state                 state of function
	 * @param currentRow            latest row received by deduplicate function
	 * @param out                   underlying collector
	 * @param generateUpdateBefore  flag to generate UPDATE_BEFORE message or not
	 * @param generateInsert        flag to gennerate INSERT message or not
	 * @param rowtimeIndex          the index of rowtime field
	 * @param keepLastRow			flag to keep last row or keep first row
	 */
	public static void deduplicateOnRowTime(
			ValueState<RowData> state,
			RowData currentRow,
			Collector<RowData> out,
			boolean generateUpdateBefore,
			boolean generateInsert,
			int rowtimeIndex,
			boolean keepLastRow) throws Exception {
		checkInsertOnly(currentRow);
		RowData preRow = state.value();

		if (isDuplicate(preRow, currentRow, rowtimeIndex, keepLastRow)) {
			updateDeduplicateResult(
					generateUpdateBefore,
					generateInsert,
					preRow,
					currentRow,
					out);
			state.update(currentRow);
		}
	}
}
