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
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processFirstRow;

/**
 * This function is used to deduplicate on keys and keeps only first row.
 */
public class DeduplicateKeepFirstRowFunction
		extends KeyedProcessFunctionWithCleanupState<BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = 5865777137707602549L;

	// state stores a boolean flag to indicate whether key appears before.
	private ValueState<Boolean> state;

	public DeduplicateKeepFirstRowFunction(long minRetentionTime, long maxRetentionTime) {
		super(minRetentionTime, maxRetentionTime);
	}

	@Override
	public void open(Configuration configure) throws Exception {
		super.open(configure);
		initCleanupTimeState("DeduplicateFunctionKeepFirstRow");
		ValueStateDescriptor<Boolean> stateDesc = new ValueStateDescriptor<>("existsState", Types.BOOLEAN);
		state = getRuntimeContext().getState(stateDesc);
	}

	@Override
	public void processElement(BaseRow input, Context ctx, Collector<BaseRow> out) throws Exception {
		long currentTime = ctx.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, currentTime);
		processFirstRow(input, state, out);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<BaseRow> out) throws Exception {
		if (stateCleaningEnabled) {
			cleanupState(state);
		}
	}
}
