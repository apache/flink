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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

/**
 * Base interface for clean up state, both for {@link ProcessFunction} and {@link CoProcessFunction}.
 */
public interface CleanupState {

	default void registerProcessingCleanupTimer(
			ValueState<Long> cleanupTimeState,
			long currentTime,
			long minRetentionTime,
			long maxRetentionTime,
			TimerService timerService) throws Exception {

		// last registered timer
		Long curCleanupTime = cleanupTimeState.value();

		// check if a cleanup timer is registered and
		// that the current cleanup timer won't delete state we need to keep
		if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
			// we need to register a new (later) timer
			long cleanupTime = currentTime + maxRetentionTime;
			// register timer and remember clean-up time
			timerService.registerProcessingTimeTimer(cleanupTime);
			// delete expired timer
			if (curCleanupTime != null) {
				timerService.deleteProcessingTimeTimer(curCleanupTime);
			}
			cleanupTimeState.update(cleanupTime);
		}
	}
}
