/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A processing time service whose timers never fire so all timers are included in savepoints.
 */
@Internal
public final class NeverFireProcessingTimeService implements TimerService {
	private static final NeverCompleteFuture FUTURE = new NeverCompleteFuture(Long.MAX_VALUE);

	private AtomicBoolean shutdown = new AtomicBoolean(true);

	@Override
	public long getCurrentProcessingTime() {
		return System.currentTimeMillis();
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		return FUTURE;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(
		ProcessingTimeCallback callback, long initialDelay, long period) {
		return FUTURE;
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(
		ProcessingTimeCallback callback, long initialDelay, long period) {
		return FUTURE;
	}

	@Override
	public boolean isTerminated() {
		return shutdown.get();
	}

	@Override
	public CompletableFuture<Void> quiesce() {
		shutdown.set(true);
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void shutdownService() {
		shutdown.set(true);
	}

	@Override
	public boolean shutdownServiceUninterruptible(long timeoutMs) {
		shutdown.set(true);
		return shutdown.get();
	}
}
