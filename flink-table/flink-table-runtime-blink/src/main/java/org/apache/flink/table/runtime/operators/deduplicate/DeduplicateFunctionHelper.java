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
import org.apache.flink.types.RowKind;
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
	 * @param generateUpdateBefore whether need to send UPDATE_BEFORE message for updates
	 * @param state state of function, null if generateUpdateBefore is false
	 * @param out underlying collector
	 */
	static void processLastRowOnInsertOnly(
			RowData currentRow,
			boolean generateUpdateBefore,
			boolean generateInsert,
			ValueState<RowData> state,
			Collector<RowData> out) throws Exception {
		// check message should be insert only.
		Preconditions.checkArgument(currentRow.getRowKind() == RowKind.INSERT);

		if (generateUpdateBefore || generateInsert) {
			// use state to keep the previous row content if we need to generate UPDATE_BEFORE
			// or use to distinguish the first row, if we need to generate INSERT
			RowData preRow = state.value();
			state.update(currentRow);
			if (preRow == null) {
				// the first row, send INSERT message
				currentRow.setRowKind(RowKind.INSERT);
				out.collect(currentRow);
			} else {
				if (generateUpdateBefore) {
					preRow.setRowKind(RowKind.UPDATE_BEFORE);
					out.collect(preRow);
				}
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(currentRow);
			}
		} else {
			// always send UPDATE_AFTER if INSERT is not needed
			currentRow.setRowKind(RowKind.UPDATE_AFTER);
			out.collect(currentRow);
		}
	}

	/**
	 * Processes element to deduplicate on keys, sends current element as last row, retracts previous element if
	 * needed.
	 *
	 * <p>Note: we don't support stateless mode yet. Because this is not safe for Kafka tombstone
	 * messages which doesn't contain full content. This can be a future improvement if the
	 * downstream (e.g. sink) doesn't require full content for DELETE messages.
	 *
	 * @param currentRow latest row received by deduplicate function
	 * @param generateUpdateBefore whether need to send UPDATE_BEFORE message for updates
	 * @param state state of function
	 * @param out underlying collector
	 */
	static void processLastRowOnChangelog(
			RowData currentRow,
			boolean generateUpdateBefore,
			ValueState<RowData> state,
			Collector<RowData> out) throws Exception {
		RowData preRow = state.value();
		RowKind currentKind = currentRow.getRowKind();
		if (currentKind == RowKind.INSERT || currentKind == RowKind.UPDATE_AFTER) {
			if (preRow == null) {
				// the first row, send INSERT message
				currentRow.setRowKind(RowKind.INSERT);
				out.collect(currentRow);
			} else {
				if (generateUpdateBefore) {
					preRow.setRowKind(RowKind.UPDATE_BEFORE);
					out.collect(preRow);
				}
				currentRow.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(currentRow);
			}
			// normalize row kind
			currentRow.setRowKind(RowKind.INSERT);
			// save to state
			state.update(currentRow);
		} else {
			// DELETE or UPDATER_BEFORE
			if (preRow != null) {
				// always set to DELETE because this row has been removed
				// even the the input is UPDATE_BEFORE, there may no UPDATE_AFTER after it.
				preRow.setRowKind(RowKind.DELETE);
				// output the preRow instead of currentRow,
				// because preRow always contains the full content.
				// currentRow may only contain key parts (e.g. Kafka tombstone records).
				out.collect(preRow);
				// clear state as the row has been removed
				state.clear();
			}
			// nothing to do if removing a non-existed row
		}
	}

	/**
	 * Processes element to deduplicate on keys, sends current element if it is first row.
	 *
	 * @param currentRow latest row received by deduplicate function
	 * @param state state of function
	 * @param out underlying collector
	 */
	static void processFirstRow(
			RowData currentRow,
			ValueState<Boolean> state,
			Collector<RowData> out) throws Exception {
		// check message should be insert only.
		Preconditions.checkArgument(currentRow.getRowKind() == RowKind.INSERT);
		// ignore record if it is not first row
		if (state.value() != null) {
			return;
		}
		state.update(true);
		// emit the first row which is INSERT message
		out.collect(currentRow);
	}

	private DeduplicateFunctionHelper() {

	}
}
