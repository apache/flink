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

package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TimeServiceProvider} which assigns as current processing time the result of calling
 * {@link System#currentTimeMillis()} and registers timers using a {@link ScheduledThreadPoolExecutor}.
 */
public class DefaultTimeServiceProvider extends TimeServiceProvider {

	/** The executor service that schedules and calls the triggers of this task*/
	private final ScheduledExecutorService timerService;

	public static DefaultTimeServiceProvider create (ScheduledExecutorService executor) {
		return new DefaultTimeServiceProvider(executor);
	}

	private DefaultTimeServiceProvider(ScheduledExecutorService threadPoolExecutor) {
		this.timerService = threadPoolExecutor;
	}

	@Override
	public long getCurrentProcessingTime() {
		return System.currentTimeMillis();
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, Runnable target) {
		long delay = Math.max(timestamp - getCurrentProcessingTime(), 0);
		return timerService.schedule(target, delay, TimeUnit.MILLISECONDS);
	}

	@Override
	public void shutdownService() throws Exception {
		if (!timerService.isTerminated()) {
			StreamTask.LOG.info("Timer service is shutting down.");
		}
		timerService.shutdownNow();
	}
}
