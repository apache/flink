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

package org.apache.flink.runtime.testutils;

import org.apache.flink.runtime.concurrent.Executors;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This implements a variant of the {@link Executors#directExecutor()} that also implements the
 * {@link ScheduledExecutorService} interface.
 * Scheduled executables are actually scheduled, all other (call / execute) execute synchronously.
 */
public class DirectScheduledExecutorService extends AbstractExecutorService implements ScheduledExecutorService {

	private final ScheduledExecutorService scheduledService = 
			java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

	// ------------------------------------------------------------------------
	//  Direct Executor 
	// ------------------------------------------------------------------------

	@Override
	public void execute(Runnable command) {
		if (!isShutdown()) {
			command.run();
		} else {
			throw new RejectedExecutionException();
		}
	}

	// ------------------------------------------------------------------------
	//  Scheduled Executor 
	// ------------------------------------------------------------------------


	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return scheduledService.schedule(command, delay, unit);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		return scheduledService.schedule(callable, delay, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return scheduledService.scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return scheduledService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

	// ------------------------------------------------------------------------
	//  Shutdown 
	// ------------------------------------------------------------------------

	@Override
	public void shutdown() {
		scheduledService.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return scheduledService.shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		return scheduledService.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return scheduledService.isTerminated();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return scheduledService.awaitTermination(timeout, unit);
	}
}
