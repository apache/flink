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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/**
 * A function that processes elements of a stream, and could cleanup state.
 *
 * @param <K> Type of the key.
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 */
public abstract class KeyedProcessFunctionWithCleanupState<K, IN, OUT> extends KeyedProcessFunction<K, IN, OUT> {

	protected final long minRetentionTime;
	protected final long maxRetentionTime;
	protected final boolean stateCleaningEnabled;

	// holds the latest registered cleanup timer
	private ValueState<Long> cleanupTimeState;

	public KeyedProcessFunctionWithCleanupState(long minRetentionTime, long maxRetentionTime) {
		this.minRetentionTime = minRetentionTime;
		this.maxRetentionTime = maxRetentionTime;
		this.stateCleaningEnabled = minRetentionTime > 1;
	}

	protected void initCleanupTimeState(String stateName) {
		if (stateCleaningEnabled) {
			ValueStateDescriptor<Long> inputCntDescriptor = new ValueStateDescriptor(stateName, Types.LONG);
			cleanupTimeState = getRuntimeContext().getState(inputCntDescriptor);
		}
	}

	protected void registerProcessingCleanupTimer(Context ctx, long currentTime) throws Exception {
		if (stateCleaningEnabled) {
			// last registered timer
			Long curCleanupTime = cleanupTimeState.value();

			// check if a cleanup timer is registered and that the current cleanup timer won't delete state we need to keep
			if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
				// we need to register a new (later) timer
				Long cleanupTime = currentTime + maxRetentionTime;
				// register timer and remember clean-up time
				ctx.timerService().registerProcessingTimeTimer(cleanupTime);
				cleanupTimeState.update(cleanupTime);
			}
		}
	}

	protected boolean isProcessingTimeTimer(OnTimerContext ctx) {
		return ctx.timeDomain() == TimeDomain.PROCESSING_TIME;
	}

	protected boolean needToCleanupState(long timestamp) throws Exception {
		if (stateCleaningEnabled) {
			Long cleanupTime = cleanupTimeState.value();
			// check that the triggered timer is the last registered processing time timer.
			return null != cleanupTime && timestamp == cleanupTime;
		} else {
			return false;
		}
	}

	protected void cleanupState(State... states) {
		for (State state : states) {
			state.clear();
		}
		this.cleanupTimeState.clear();
	}

}
