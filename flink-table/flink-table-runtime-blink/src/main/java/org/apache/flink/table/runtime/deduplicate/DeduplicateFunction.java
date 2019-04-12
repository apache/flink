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
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.deduplicate.DeduplicateFunctionHelper.processFirstRow;
import static org.apache.flink.table.runtime.deduplicate.DeduplicateFunctionHelper.processLastRow;

/**
 * This function is used to deduplicate on keys and keeps only first row or last row.
 */
public class DeduplicateFunction
		extends KeyedProcessFunctionWithCleanupState<BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = 4950071982706870944L;

	private final BaseRowTypeInfo rowTypeInfo;
	private final boolean generateRetraction;
	private final boolean keepLastRow;

	// state stores complete row if keep last row and generate retraction is true,
	// else stores a flag to indicate whether key appears before.
	private ValueState state;

	public DeduplicateFunction(long minRetentionTime, long maxRetentionTime, BaseRowTypeInfo rowTypeInfo,
			boolean generateRetraction, boolean keepLastRow) {
		super(minRetentionTime, maxRetentionTime);
		this.rowTypeInfo = rowTypeInfo;
		this.generateRetraction = generateRetraction;
		this.keepLastRow = keepLastRow;
	}

	@Override
	public void open(Configuration configure) throws Exception {
		super.open(configure);
		String stateName = keepLastRow ? "DeduplicateFunctionKeepLastRow" : "DeduplicateFunctionKeepFirstRow";
		initCleanupTimeState(stateName);
		ValueStateDescriptor stateDesc = null;
		if (keepLastRow && generateRetraction) {
			// if need generate retraction and keep last row, stores complete row into state
			stateDesc = new ValueStateDescriptor("deduplicateFunction", rowTypeInfo);
		} else {
			// else stores a flag to indicator whether pk appears before.
			stateDesc = new ValueStateDescriptor("fistValueState", Types.BOOLEAN);
		}
		state = getRuntimeContext().getState(stateDesc);
	}

	@Override
	public void processElement(BaseRow input, Context ctx, Collector<BaseRow> out) throws Exception {
		long currentTime = ctx.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, currentTime);

		if (keepLastRow) {
			processLastRow(input, generateRetraction, state, out);
		} else {
			processFirstRow(input, state, out);
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<BaseRow> out) throws Exception {
		if (stateCleaningEnabled) {
			cleanupState(state);
		}
	}
}
