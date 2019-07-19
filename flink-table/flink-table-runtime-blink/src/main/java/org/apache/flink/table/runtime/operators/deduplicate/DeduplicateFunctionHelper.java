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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Utility for deduplicate function.
 */
class DeduplicateFunctionHelper {

	/**
	 * Processes element to deduplicate on keys, sends current element as last row, retracts previous element if
	 * needed.
	 *
	 * @param currentRow latest row received by deduplicate function
	 * @param generateRetraction whether need to send retract message to downstream
	 * @param state state of function
	 * @param out underlying collector
	 * @throws Exception
	 */
	static void processLastRow(BaseRow currentRow, boolean generateRetraction, ValueState<BaseRow> state,
			Collector<BaseRow> out) throws Exception {
		// Check message should be accumulate
		Preconditions.checkArgument(BaseRowUtil.isAccumulateMsg(currentRow));
		if (generateRetraction) {
			// state stores complete row if generateRetraction is true
			BaseRow preRow = state.value();
			state.update(currentRow);
			if (preRow != null) {
				preRow.setHeader(BaseRowUtil.RETRACT_MSG);
				out.collect(preRow);
			}
		}
		out.collect(currentRow);
	}

	/**
	 * Processes element to deduplicate on keys, sends current element if it is first row.
	 *
	 * @param currentRow latest row received by deduplicate function
	 * @param state state of function
	 * @param out underlying collector
	 * @throws Exception
	 */
	static void processFirstRow(BaseRow currentRow, ValueState<Boolean> state, Collector<BaseRow> out)
			throws Exception {
		// Check message should be accumulate
		Preconditions.checkArgument(BaseRowUtil.isAccumulateMsg(currentRow));
		// ignore record with timestamp bigger than preRow
		if (state.value() != null) {
			return;
		}
		state.update(true);
		out.collect(currentRow);
	}

	private DeduplicateFunctionHelper() {

	}
}
