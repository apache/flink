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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * An implementation of {@link org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor} for tests.
 */
public class TestComponentMainThreadExecutor extends ComponentMainThreadExecutorServiceAdapter {

	public TestComponentMainThreadExecutor(
		@Nonnull ScheduledExecutorService scheduledExecutorService,
		@Nonnull Thread mainThread) {

		super(scheduledExecutorService, () -> {
			assert MainThreadValidatorUtil.isRunningInExpectedThread(mainThread);
		});
	}

	public static TestComponentMainThreadExecutor forMainThread() {
		final Thread main = Thread.currentThread();
		return new TestComponentMainThreadExecutor(new DirectScheduledExecutorService() {
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
	public static TestComponentMainThreadExecutor forSingleThreadExecutor(
		@Nonnull ScheduledExecutorService singleThreadExecutor) {
		try {
			Thread thread = CompletableFuture.supplyAsync(Thread::currentThread, singleThreadExecutor).get();
			return new TestComponentMainThreadExecutor(singleThreadExecutor, thread);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
