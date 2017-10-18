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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Test utilities for {@link com.datastax.driver.core.ResultSetFuture} from Datastax Java driver.
 */
public class ResultSetFutures {

	private ResultSetFutures() {
	}

	public static ResultSetFuture immediateFuture(@Nullable ResultSet value) {
		return new ImmediateSuccessfulResultSetFuture(value);
	}

	public static ResultSetFuture immediateFailedFuture(Throwable throwable) {
		checkNotNull(throwable);
		return new ImmediateFailedResultSetFuture(throwable);
	}

	/**
	 * A type of {@link ResultSetFuture} that returns result immediately. Currently there are three types
	 * 1. Future was successful
	 * 2. Future has failed with an exception
	 * 3. Future was cancelled.
	 */
	private abstract static class ImmediateResultSetFuture implements ResultSetFuture {

		@Override
		public ResultSet getUninterruptibly() {
			return null;
		}

		@Override
		public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
			return null;
		}

		private static final Logger log = LoggerFactory.getLogger(ImmediateResultSetFuture.class);

		@Override
		public void addListener(Runnable listener, Executor executor) {
			checkNotNull(listener, "Runnable was null.");
			checkNotNull(executor, "Executor was null.");
			try {
				executor.execute(listener);
			} catch (RuntimeException e) {
				// ResultSetFuture's contract is that it will not throw unchecked
				// exceptions, so log the bad runnable and/or executor and swallow it.
				log.error("RuntimeException while executing runnable "
					+ listener + " with executor " + executor, e);
			}
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public abstract ResultSet get() throws ExecutionException;

		@Override
		public ResultSet get(long timeout, TimeUnit unit) throws ExecutionException {
			checkNotNull(unit);
			return get();
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return true;
		}
	}

	private static class ImmediateSuccessfulResultSetFuture extends ImmediateResultSetFuture {

		@Nullable
		protected final ResultSet value;

		ImmediateSuccessfulResultSetFuture(@Nullable ResultSet value) {
			this.value = value;
		}

		@Override
		public ResultSet get() {
			return value;
		}
	}

	private static class ImmediateFailedResultSetFuture<V> extends ImmediateResultSetFuture {

		protected final Throwable thrown;

		ImmediateFailedResultSetFuture(Throwable thrown) {
			this.thrown = thrown;
		}

		@Override
		public ResultSet get() throws ExecutionException {
			throw new ExecutionException(thrown);
		}
	}
}
