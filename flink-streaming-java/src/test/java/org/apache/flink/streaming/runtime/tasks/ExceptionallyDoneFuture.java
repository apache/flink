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

package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a {@link RunnableFuture} which can only complete exceptionally.
 *
 * @param <V> type of the RunnableFuture
 */
public class ExceptionallyDoneFuture<V> implements RunnableFuture<V> {

	private final Throwable throwable;

	private ExceptionallyDoneFuture(Throwable throwable) {
		this.throwable = throwable;
	}

	@Override
	public void run() {}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public V get() throws ExecutionException {
		throw new ExecutionException(throwable);
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws ExecutionException {
		return get();
	}

	public static <T> ExceptionallyDoneFuture<T> of(Throwable throwable) {
		return new ExceptionallyDoneFuture<>(throwable);
	}
}
