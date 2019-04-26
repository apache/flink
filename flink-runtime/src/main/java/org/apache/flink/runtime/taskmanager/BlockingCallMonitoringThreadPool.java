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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Thread Pool used to monitor the number of in-flight calls that block and wait for another task executed
 * by the same pool in order to get unblocked. When a call (blocking or non-blocking) is submitted, the size
 * of the pool is set to {@code 1 + activeBlockingCalls}. This allows the thread pool size to follow the needs
 * of the system and to avoid any redundant idle threads consuming resources.
 */
public class BlockingCallMonitoringThreadPool {

	private static final Logger LOG = LoggerFactory.getLogger(BlockingCallMonitoringThreadPool.class);

	private final AtomicInteger inFlightBlockingCallCounter = new AtomicInteger(0);

	private final ThreadPoolExecutor executor;

	public BlockingCallMonitoringThreadPool() {
		this(Executors.defaultThreadFactory());
	}

	public BlockingCallMonitoringThreadPool(final ThreadFactory dispatcherThreadFactory) {
		this.executor = new ThreadPoolExecutor(
				1,
				1,
				10L,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(),
				checkNotNull(dispatcherThreadFactory));
	}

	public CompletableFuture<?> submit(final Runnable runnable, final boolean blocking) {
		if (blocking) {
			return submitBlocking(runnable);
		} else {
			return submit(runnable);
		}
	}

	private CompletableFuture<?> submit(final Runnable task) {
		adjustThreadPoolSize(inFlightBlockingCallCounter.get());
		return CompletableFuture.runAsync(task, executor);
	}

	private CompletableFuture<?> submitBlocking(final Runnable task) {
		adjustThreadPoolSize(inFlightBlockingCallCounter.incrementAndGet());
		return CompletableFuture.runAsync(task, executor).whenComplete(
				(ignored, e) -> inFlightBlockingCallCounter.decrementAndGet());
	}

	private void adjustThreadPoolSize(final int activeBlockingCalls) {
		if (activeBlockingCalls > 1) {
			LOG.debug("There are {} active threads with blocking calls", activeBlockingCalls);
		}

		final int newPoolSize = 1 + activeBlockingCalls;

		// We have to reset the core pool size because (quoted from the official docs):
		// ``
		// If there are more than corePoolSize but less than maximumPoolSize threads running,
		// ** a new thread will be created ONLY IF THE QUEUE IS FULL **.
		// ``

		// ensure that regardless of whether we increase/reduce the pool size, maximum is always >= core
		if (newPoolSize < executor.getCorePoolSize()) {
			executor.setCorePoolSize(newPoolSize);
			executor.setMaximumPoolSize(newPoolSize);
		} else {
			executor.setMaximumPoolSize(newPoolSize);
			executor.setCorePoolSize(newPoolSize);
		}
	}

	public void shutdown() {
		executor.shutdown();
	}

	public boolean isShutdown() {
		return executor.isShutdown();
	}

	public void shutdownNow() {
		executor.shutdownNow();
	}

	@VisibleForTesting
	int getMaximumPoolSize() {
		return executor.getMaximumPoolSize();
	}

	@VisibleForTesting
	int getQueueSize() {
		return executor.getQueue().size();
	}
}
