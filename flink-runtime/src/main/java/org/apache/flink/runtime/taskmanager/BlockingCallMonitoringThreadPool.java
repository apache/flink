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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Thread Pool used by the {@link Task} to launch checkpointing tasks, namely
 * the {@link Task#triggerCheckpointBarrier(long, long, CheckpointOptions, boolean)}
 * and the {@link Task#notifyCheckpointComplete(long)} callback.
 *
 * <p>This thread pool monitors the number of in-flight calls executing a {@link CheckpointType#SYNC_SAVEPOINT},
 * and adjusts its pool size accordingly, so that the minimum number of threads are allocated. This is to
 * avoid any redundant idle threads consuming resources. In addition,this thread pool logs (in debug level)
 * when more than 1 such calls are in-flight.
 */
public class BlockingCallMonitoringThreadPool {

	private static final Logger LOG = LoggerFactory.getLogger(BlockingCallMonitoringThreadPool.class);

	private final AtomicInteger inFlightBlockingCallCounter = new AtomicInteger(0);

	private final ThreadPoolExecutor executor;

	public BlockingCallMonitoringThreadPool(final ThreadFactory dispatcherThreadFactory) {
		this.executor = new ThreadPoolExecutor(
				1,
				1,
				10L,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(),
				checkNotNull(dispatcherThreadFactory));
	}

	public void submit(final Runnable runnable, final boolean blocking) {
		if (blocking) {
			submitBlocking(runnable);
		} else {
			submit(runnable);
		}
	}

	private void submit(final Runnable task) {
		adjustThreadPoolSize(inFlightBlockingCallCounter.get());
		executor.execute(task);
	}

	private void submitBlocking(final Runnable task) {
		adjustThreadPoolSize(inFlightBlockingCallCounter.incrementAndGet());
		CompletableFuture.runAsync(task, executor).whenComplete(
				(ignored, e) -> inFlightBlockingCallCounter.decrementAndGet());
	}

	private void adjustThreadPoolSize(final int activeBlockingCalls) {
		if (activeBlockingCalls > 1) {
			LOG.debug("There are {} active threads with blocking calls", activeBlockingCalls);
		}

		final int newPoolSize = executor.getCorePoolSize() + activeBlockingCalls;

		// We have to reset the core pool size because (quoted from javadoc):
		// ``
		// If there are more than corePoolSize but less than maximumPoolSize threads running,
		// ** a new thread will be created only if the queue is full **.
		// ``

		executor.setCorePoolSize(newPoolSize);
		executor.setMaximumPoolSize(newPoolSize);
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
}
