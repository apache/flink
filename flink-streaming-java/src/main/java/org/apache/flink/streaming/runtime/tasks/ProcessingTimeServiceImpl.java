/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;

class ProcessingTimeServiceImpl implements ProcessingTimeService {
	private final TimerService timerService;
	private final Function<ProcessingTimeCallback, ProcessingTimeCallback> processingTimeCallbackWrapper;

	ProcessingTimeServiceImpl(
			TimerService timerService,
			Function<ProcessingTimeCallback, ProcessingTimeCallback> processingTimeCallbackWrapper) {
		this.timerService = timerService;
		this.processingTimeCallbackWrapper = processingTimeCallbackWrapper;
	}

	@Override
	public long getCurrentProcessingTime() {
		return timerService.getCurrentProcessingTime();
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		return timerService.registerTimer(timestamp, processingTimeCallbackWrapper.apply(target));
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		return timerService.scheduleAtFixedRate(processingTimeCallbackWrapper.apply(callback), initialDelay, period);
	}
}
