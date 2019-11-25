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

import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.Callable;

/**
 * Wraps execution of a {@link Runnable}, {@link ThrowingRunnable}, {@link Callable}, or {@link Mail}.
 * Intended to customize execution in sub-types fo {@link org.apache.flink.streaming.runtime.tasks.StreamTask StreamTask},
 * e.g. synchronization in {@link org.apache.flink.streaming.runtime.tasks.SourceStreamTask SourceStreamTask}.
 */
public interface ExecutionDecorator {
	void run(Runnable runnable);

	<E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E;

	<R> R call(Callable<R> callable) throws Exception;

	void dispatch(Mail mail);

	ExecutionDecorator NOP = new ExecutionDecorator() {
		@Override
		public void run(Runnable runnable) {
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

		@Override
		public void dispatch(Mail mail) {
			mail.run();
		}
	};

	/**
	 * A {@link ExecutionDecorator} that synchronizes every operation on the provided mutex.
	 */
	class SynchronizedExecutionDecorator implements ExecutionDecorator {
		private final Object mutex;
		public static SynchronizedExecutionDecorator newInstance() {
			return new SynchronizedExecutionDecorator(new Object());
		}

		public SynchronizedExecutionDecorator(Object mutex) {
			this.mutex = mutex;
		}

		@Override
		public void run(Runnable runnable) {
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

		@Override
		public void dispatch(Mail mail) {
			synchronized (mutex) {
				mail.run();
			}
		}

		@Deprecated
		public Object getMutex() {
			return mutex;
		}
	}
}
