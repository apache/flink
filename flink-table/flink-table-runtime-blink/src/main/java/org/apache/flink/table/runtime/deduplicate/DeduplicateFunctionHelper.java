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

package org.apache.flink.table.runtime.deduplicate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Utility for deduplicate function.
 */
class DeduplicateFunctionHelper {

	static void processLastRow(BaseRow preRow, BaseRow currentRow, boolean generateRetraction,
			boolean stateCleaningEnabled, ValueState<BaseRow> pkRow, RecordEqualiser equaliser,
			Collector<BaseRow> out) throws Exception {
		// should be accumulate msg.
		Preconditions.checkArgument(BaseRowUtil.isAccumulateMsg(currentRow));
		// ignore same record
		if (!stateCleaningEnabled && preRow != null &&
				equaliser.equalsWithoutHeader(preRow, currentRow)) {
			return;
		}
		pkRow.update(currentRow);
		if (preRow != null && generateRetraction) {
			preRow.setHeader(BaseRowUtil.RETRACT_MSG);
			out.collect(preRow);
		}
		out.collect(currentRow);
	}

	static void processFirstRow(BaseRow preRow, BaseRow currentRow, ValueState<BaseRow> pkRow,
			Collector<BaseRow> out) throws Exception {
		// should be accumulate msg.
		Preconditions.checkArgument(BaseRowUtil.isAccumulateMsg(currentRow));
		// ignore record with timestamp bigger than preRow
		if (preRow != null) {
			return;
		}

		pkRow.update(currentRow);
		out.collect(currentRow);
	}

	private DeduplicateFunctionHelper() {

	}
}
