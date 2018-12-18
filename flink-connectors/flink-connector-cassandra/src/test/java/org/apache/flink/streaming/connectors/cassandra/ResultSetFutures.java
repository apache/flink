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

package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to create {@link com.datastax.driver.core.ResultSetFuture}s.
 */
class ResultSetFutures {

	private ResultSetFutures() {
	}

	static ResultSetFuture fromCompletableFuture(CompletableFuture<ResultSet> future) {
		checkNotNull(future);
		return new CompletableResultSetFuture(future);
	}

	private static class CompletableResultSetFuture implements ResultSetFuture {

		private final CompletableFuture<ResultSet> completableFuture;

		CompletableResultSetFuture(CompletableFuture<ResultSet> future) {
			this.completableFuture = future;
		}

		@Override
		public ResultSet getUninterruptibly() {
			try {
				return completableFuture.get();
			} catch (InterruptedException e) {
				return getUninterruptibly();
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public ResultSet getUninterruptibly(long l, TimeUnit timeUnit) throws TimeoutException {
			try {
				return completableFuture.get(l, timeUnit);
			} catch (InterruptedException e) {
				return getUninterruptibly();
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean cancel(boolean b) {
			return completableFuture.cancel(b);
		}

		@Override
		public boolean isCancelled() {
			return completableFuture.isCancelled();
		}

		@Override
		public boolean isDone() {
			return completableFuture.isDone();
		}

		@Override
		public ResultSet get() throws InterruptedException, ExecutionException {
			return completableFuture.get();
		}

		@Override
		public ResultSet get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return completableFuture.get(timeout, unit);
		}

		@Override
		public void addListener(Runnable listener, Executor executor) {
			completableFuture.whenComplete((result, error) -> listener.run());
		}
	}
}
