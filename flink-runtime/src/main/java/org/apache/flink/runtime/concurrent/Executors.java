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

import org.apache.flink.runtime.util.ExecutorThreadFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import scala.concurrent.ExecutionContext;

/**
 * Collection of {@link Executor}, {@link ExecutorService} and {@link ExecutionContext} implementations.
 */
public class Executors {

	/**
	 * Return a direct executor. The direct executor directly executes the runnable in the calling
	 * thread.
	 *
	 * @return Direct executor
	 */
	public static Executor directExecutor() {
		return DirectExecutorService.INSTANCE;
	}

	/**
	 * Return a new direct executor service.
	 *
	 * <p>The direct executor service directly executes the runnables and the callables in the calling
	 * thread.
	 *
	 * @return New direct executor service
	 */
	public static ExecutorService newDirectExecutorService() {
		return new DirectExecutorService();
	}

	/**
	 * Return a direct execution context. The direct execution context executes the runnable directly
	 * in the calling thread.
	 *
	 * @return Direct execution context.
	 */
	public static ExecutionContext directExecutionContext() {
		return DirectExecutionContext.INSTANCE;
	}

	/**
	 * Returns a new cached thread pool with the desired maximum size.
	 *
	 * <p>This method is a variation of {@link java.util.concurrent.Executors#newFixedThreadPool(int, ThreadFactory)},
	 * with the minimum pool size set to 0.
	 * In that respect it is similar to {@link java.util.concurrent.Executors#newCachedThreadPool()}, but it uses a
	 * {@link LinkedBlockingQueue} instead to allow tasks to be queued, instead of failing with an exception if the pool
	 * is saturated.
	 *
	 * @see ExecutorThreadFactory
	 * @param maxPoolSize maximum size of the thread pool
	 * @param threadFactory thread factory to use
	 * @return new cached thread pool
	 */
	public static ExecutorService newCachedThreadPool(int maxPoolSize, ThreadFactory threadFactory) {
		return new ThreadPoolExecutor(0, maxPoolSize,
			60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<>(),
			threadFactory);
	}

	/**
	 * Direct execution context.
	 */
	private static class DirectExecutionContext implements ExecutionContext {

		static final DirectExecutionContext INSTANCE = new DirectExecutionContext();

		private DirectExecutionContext() {}

		@Override
		public void execute(Runnable runnable) {
			runnable.run();
		}

		@Override
		public void reportFailure(Throwable cause) {
			throw new IllegalStateException("Error in direct execution context.", cause);
		}

		@Override
		public ExecutionContext prepare() {
			return this;
		}
	}
}
