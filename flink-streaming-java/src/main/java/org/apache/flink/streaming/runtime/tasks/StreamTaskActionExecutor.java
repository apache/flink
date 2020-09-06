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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.Callable;

/**
 * Executes {@link Runnable}, {@link ThrowingRunnable}, or {@link Callable}.
 * Intended to customize execution in sub-types of {@link org.apache.flink.streaming.runtime.tasks.StreamTask StreamTask},
 * e.g. synchronization in {@link org.apache.flink.streaming.runtime.tasks.SourceStreamTask SourceStreamTask}.
 */
@Internal
public interface StreamTaskActionExecutor {
	void run(RunnableWithException runnable) throws Exception;

	<E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E;

	<R> R call(Callable<R> callable) throws Exception;

	StreamTaskActionExecutor IMMEDIATE = new StreamTaskActionExecutor() {
		@Override
		public void run(RunnableWithException runnable) throws Exception {
			runnable.run();
		}

		@Override
		public <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E {
			runnable.run();
		}

		@Override
		public <R> R call(Callable<R> callable) throws Exception {
			return callable.call();
		}
	};

	/**
	 * Returns an ExecutionDecorator that synchronizes each invocation.
	 */
	static SynchronizedStreamTaskActionExecutor synchronizedExecutor() {
		return synchronizedExecutor(new Object());
	}

	/**
	 * Returns an ExecutionDecorator that synchronizes each invocation on a given object.
	 */
	static SynchronizedStreamTaskActionExecutor synchronizedExecutor(Object mutex) {
		return new SynchronizedStreamTaskActionExecutor(mutex);
	}

	/**
	 * A {@link StreamTaskActionExecutor} that synchronizes every operation on the provided mutex.
	 * @deprecated this class should only be used in {@link SourceStreamTask} which exposes the checkpoint lock as part of Public API.
	 */
	@Deprecated
	class SynchronizedStreamTaskActionExecutor implements StreamTaskActionExecutor {
		private final Object mutex;

		public SynchronizedStreamTaskActionExecutor(Object mutex) {
			this.mutex = mutex;
		}

		@Override
		public void run(RunnableWithException runnable) throws Exception {
			synchronized (mutex) {
				runnable.run();
			}
		}

		@Override
		public <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E {
			synchronized (mutex) {
				runnable.run();
			}
		}

		@Override
		public <R> R call(Callable<R> callable) throws Exception {
			synchronized (mutex) {
				return callable.call();
			}
		}

		/**
		 * @return an object used for mutual exclusion of all operations that involve data and state mutation. (a.k.a. checkpoint lock).
		 */
		public Object getMutex() {
			return mutex;
		}
	}
}
