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

package org.apache.flink.runtime.concurrent;

import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import javax.annotation.Nonnull;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Adapter class for a {@link ScheduledExecutorService} or {@link ScheduledExecutor} which shall be used as a
 * {@link ComponentMainThreadExecutor}. It enhances the given executor with an assert that the current thread is the
 * main thread of the executor.
 */
public class ComponentMainThreadExecutorServiceAdapter implements ComponentMainThreadExecutor {

	private final ScheduledExecutor scheduledExecutor;

	/** A runnable that should assert that the current thread is the expected main thread. */
	private final Runnable mainThreadCheck;

	public ComponentMainThreadExecutorServiceAdapter(
			final ScheduledExecutorService scheduledExecutorService,
			final Thread mainThread) {
		this(new ScheduledExecutorServiceAdapter(scheduledExecutorService), mainThread);
	}

	public ComponentMainThreadExecutorServiceAdapter(
			final ScheduledExecutor scheduledExecutor,
			final Thread mainThread) {
		this.scheduledExecutor = scheduledExecutor;
		this.mainThreadCheck = () -> {
			assert MainThreadValidatorUtil.isRunningInExpectedThread(mainThread);
		};
	}

	public static ComponentMainThreadExecutor forMainThread() {
		final Thread main = Thread.currentThread();
		return new ComponentMainThreadExecutorServiceAdapter(new DirectScheduledExecutorService() {
			@Override
			public void execute(Runnable command) {
				assert MainThreadValidatorUtil.isRunningInExpectedThread(main);
				super.execute(command);
			}
		}, main);
	}

	/**
	 * Creates a test executor that delegates to the given {@link ScheduledExecutorService}. The given executor must
	 * execute all submissions with the same thread.
	 */
	public static ComponentMainThreadExecutor forSingleThreadExecutor(
			@Nonnull ScheduledExecutorService singleThreadExecutor) {
		final Thread thread = CompletableFuture.supplyAsync(Thread::currentThread, singleThreadExecutor).join();
		return new ComponentMainThreadExecutorServiceAdapter(singleThreadExecutor, thread);
	}

	@Override
	public void assertRunningInMainThread() {
		mainThreadCheck.run();
	}

	@Override
	public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
		return scheduledExecutor.schedule(command, delay, unit);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
		return scheduledExecutor.schedule(callable, delay, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
		return scheduledExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay, final TimeUnit unit) {
		return scheduledExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

	@Override
	public void execute(final Runnable command) {
		scheduledExecutor.execute(command);
	}
}
