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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Internal
class ProcessingTimeServiceImpl implements ProcessingTimeService {

	private final TimerService timerService;

	private final Function<ProcessingTimeCallback, ProcessingTimeCallback> processingTimeCallbackWrapper;

	private final AtomicInteger numRunningTimers;

	private final CompletableFuture<Void> quiesceCompletedFuture;

	private volatile boolean quiesced;

	ProcessingTimeServiceImpl(
			TimerService timerService,
			Function<ProcessingTimeCallback, ProcessingTimeCallback> processingTimeCallbackWrapper) {
		this.timerService = timerService;
		this.processingTimeCallbackWrapper = processingTimeCallbackWrapper;

		this.numRunningTimers = new AtomicInteger(0);
		this.quiesceCompletedFuture = new CompletableFuture<>();
		this.quiesced = false;
	}

	@Override
	public long getCurrentProcessingTime() {
		return timerService.getCurrentProcessingTime();
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		if (isQuiesced()) {
			return new NeverCompleteFuture(
				ProcessingTimeServiceUtil.getProcessingTimeDelay(timestamp, getCurrentProcessingTime()));
		}

		return timerService.registerTimer(
			timestamp, addQuiesceProcessingToCallback(processingTimeCallbackWrapper.apply(target)));
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		if (isQuiesced()) {
			return new NeverCompleteFuture(initialDelay);
		}

		return timerService.scheduleAtFixedRate(
			addQuiesceProcessingToCallback(processingTimeCallbackWrapper.apply(callback)), initialDelay, period);
	}

	@Override
	public CompletableFuture<Void> quiesce() {
		if (!quiesced) {
			quiesced = true;

			if (numRunningTimers.get() == 0) {
				quiesceCompletedFuture.complete(null);
			}
		}

		return quiesceCompletedFuture;
	}

	private boolean isQuiesced() {
		return quiesced;
	}

	private ProcessingTimeCallback addQuiesceProcessingToCallback(ProcessingTimeCallback callback) {

		return timestamp -> {
			if (isQuiesced()) {
				return;
			}

			numRunningTimers.incrementAndGet();
			try {
				// double check to deal with the race condition:
				// before executing the previous line to increase the number of running timers,
				// the quiesce-completed future is already completed as the number of running
				// timers is 0 and "quiesced" is true
				if (!isQuiesced()) {
					callback.onProcessingTime(timestamp);
				}
			} finally {
				if (numRunningTimers.decrementAndGet() == 0 && isQuiesced()) {
					quiesceCompletedFuture.complete(null);
				}
			}
		};
	}
}
