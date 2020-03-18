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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processLastRow;

/**
 * This function is used to deduplicate on keys and keeps only last row.
 */
public class DeduplicateKeepLastRowFunction
		extends KeyedProcessFunctionWithCleanupState<BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = -291348892087180350L;
	private final BaseRowTypeInfo rowTypeInfo;
	private final boolean generateRetraction;

	// state stores complete row.
	private ValueState<BaseRow> state;

	public DeduplicateKeepLastRowFunction(
			long minRetentionTime,
			long maxRetentionTime,
			BaseRowTypeInfo rowTypeInfo,
			boolean generateRetraction) {
		super(minRetentionTime, maxRetentionTime);
		this.rowTypeInfo = rowTypeInfo;
		this.generateRetraction = generateRetraction;
	}

	@Override
	public void open(Configuration configure) throws Exception {
		super.open(configure);
		if (generateRetraction) {
			// state stores complete row if need generate retraction, otherwise do not need a state
			initCleanupTimeState("DeduplicateFunctionKeepLastRow");
			ValueStateDescriptor<BaseRow> stateDesc = new ValueStateDescriptor<>("preRowState", rowTypeInfo);
			state = getRuntimeContext().getState(stateDesc);
		}
	}

	@Override
	public void processElement(BaseRow input, Context ctx, Collector<BaseRow> out) throws Exception {
		if (generateRetraction) {
			long currentTime = ctx.timerService().currentProcessingTime();
			// register state-cleanup timer
			registerProcessingCleanupTimer(ctx, currentTime);
		}
		processLastRow(input, generateRetraction, state, out);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<BaseRow> out) throws Exception {
		if (stateCleaningEnabled) {
			cleanupState(state);
		}
	}
}
