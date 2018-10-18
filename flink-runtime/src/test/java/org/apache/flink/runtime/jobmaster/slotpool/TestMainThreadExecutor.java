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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.MainThreadExecutable;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestMainThreadExecutor implements MainThreadExecutable {

	private final ScheduledExecutorService scheduledExecutor;

	public TestMainThreadExecutor(ScheduledExecutorService scheduledExecutor) {
		this.scheduledExecutor = scheduledExecutor;
	}

	public TestMainThreadExecutor() {
		this(Executors.newSingleThreadScheduledExecutor());
	}

	@Override
	public void runAsync(Runnable runnable) {
		scheduledExecutor.execute(runnable);
	}

	@Override
	public <V> CompletableFuture<V> callAsync(Callable<V> callable, Time callTimeout) {
		final CompletableFuture<V> resultFuture = new CompletableFuture<>();
		scheduledExecutor.schedule(() -> resultFuture.complete(callable.call()), 0L, TimeUnit.MILLISECONDS);
		return resultFuture;
	}

	@Override
	public void scheduleRunAsync(Runnable runnable, long delay) {
		scheduledExecutor.schedule(runnable, delay, TimeUnit.MILLISECONDS);
	}
}
