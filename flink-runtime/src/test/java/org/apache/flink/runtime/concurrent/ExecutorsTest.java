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

package org.apache.flink.runtime.concurrent;

import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.testutils.executor.TestExecutorResource;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Tests for {@link Executors}.
 */
public class ExecutorsTest {

	@Rule
	public final TestExecutorResource executorResource = new TestExecutorResource(
		() -> Executors.newCachedThreadPool(1, new ExecutorThreadFactory()));

	/**
	 * Tests that the {@link ExecutorService} returned by {@link Executors#newCachedThreadPool(int, ThreadFactory)}
	 * allows tasks to be queued. In a prior implementation the executor used a synchronous queue, rejecting tasks with
	 * an exception if no thread was available to process it.
	 */
	@Test
	public void testNewCachedThreadPoolDoesNotRejectTasksExceedingActiveThreadCount() throws InterruptedException {
		Executor executor = executorResource.getExecutor();

		BlockerSync sync = new BlockerSync();
		try {
			// submit the first blocking task, which should block the single pool thread
			executor.execute(sync::blockNonInterruptible);

			// the thread is now blocked
			sync.awaitBlocker();

			// this task should not be rejected
			executor.execute(sync::blockNonInterruptible);
		} finally {
			sync.releaseBlocker();
		}
	}
}
