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

package org.apache.flink.runtime.concurrent;

import javax.annotation.Nonnull;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Interface for an executor that runs tasks in the main thread of an {@link org.apache.flink.runtime.rpc.RpcEndpoint}.
 */
public interface ComponentMainThreadExecutor extends ScheduledExecutor {

	/**
	 * Returns true if the method was called in the thread of this executor.
	 */
	void assertRunningInMainThread();

	/**
	 * Dummy implementation of ComponentMainThreadExecutor.
	 */
	final class DummyComponentMainThreadExecutor implements ComponentMainThreadExecutor {

		/** Customized message for the exception that is thrown on method invocation. */
		private final String exceptionMessageOnInvocation;

		public DummyComponentMainThreadExecutor(String exceptionMessageOnInvocation) {
			this.exceptionMessageOnInvocation = exceptionMessageOnInvocation;
		}

		@Override
		public void assertRunningInMainThread() {
			throw createException();
		}

		@Override
		public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
			throw createException();
		}

		@Override
		public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
			throw createException();
		}

		@Override
		public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
			throw createException();
		}

		@Override
		public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
			throw createException();
		}

		@Override
		public void execute(@Nonnull Runnable command) {
			throw createException();
		}

		private UnsupportedOperationException createException() {
			return new UnsupportedOperationException(exceptionMessageOnInvocation);
		}
	}
}
