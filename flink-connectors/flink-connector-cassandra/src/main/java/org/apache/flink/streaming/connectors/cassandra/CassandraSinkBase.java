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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CassandraSinkBase is the common abstract class of {@link CassandraPojoSink} and {@link
 * CassandraTupleSink}.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class CassandraSinkBase<IN, V> extends RichSinkFunction<IN> implements CheckpointedFunction {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected transient Cluster cluster;
	protected transient Session session;

	private AtomicReference<Throwable> throwable;
	private FutureCallback<V> callback;
	private Semaphore semaphore;

	private final ClusterBuilder builder;
	private final CassandraSinkBaseConfig config;

	private final CassandraFailureHandler failureHandler;

	CassandraSinkBase(ClusterBuilder builder, CassandraSinkBaseConfig config, CassandraFailureHandler failureHandler) {
		this.builder = builder;
		this.config = config;
		this.failureHandler = Preconditions.checkNotNull(failureHandler);
		ClosureCleaner.clean(builder, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
	}

	@Override
	public void open(Configuration configuration) {
		this.callback = new FutureCallback<V>() {
			@Override
			public void onSuccess(V ignored) {
				semaphore.release();
			}

			@Override
			public void onFailure(Throwable t) {
				throwable.compareAndSet(null, t);
				log.error("Error while sending value.", t);
				semaphore.release();
			}
		};
		this.cluster = builder.getCluster();
		this.session = createSession();

		throwable = new AtomicReference<>();
		semaphore = new Semaphore(config.getMaxConcurrentRequests());
	}

	@Override
	public void close() throws Exception {
		try {
			checkAsyncErrors();
			flush();
			checkAsyncErrors();
		} finally {
			try {
				if (session != null) {
					session.close();
				}
			} catch (Exception e) {
				log.error("Error while closing session.", e);
			}
			try {
				if (cluster != null) {
					cluster.close();
				}
			} catch (Exception e) {
				log.error("Error while closing cluster.", e);
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	@Override
	public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
		checkAsyncErrors();
		flush();
		checkAsyncErrors();
	}

	@Override
	public void invoke(IN value) throws Exception {
		checkAsyncErrors();
		tryAcquire(1);
		final ListenableFuture<V> result;
		try {
			result = send(value);
		} catch (Throwable e) {
			semaphore.release();
			throw e;
		}
		Futures.addCallback(result, callback);
	}

	protected Session createSession() {
		return cluster.connect();
	}

	public abstract ListenableFuture<V> send(IN value);

	private void tryAcquire(int permits) throws InterruptedException, TimeoutException {
		if (!semaphore.tryAcquire(permits, config.getMaxConcurrentRequestsTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
			throw new TimeoutException(
				String.format(
					"Failed to acquire %d out of %d permits to send value in %s.",
					permits,
					config.getMaxConcurrentRequests(),
					config.getMaxConcurrentRequestsTimeout()
				)
			);
		}
	}

	private void checkAsyncErrors() throws Exception {
		final Throwable currentError = throwable.getAndSet(null);
		if (currentError != null) {
			failureHandler.onFailure(currentError);
		}
	}

	private void flush() throws InterruptedException, TimeoutException {
		tryAcquire(config.getMaxConcurrentRequests());
		semaphore.release(config.getMaxConcurrentRequests());
	}

	@VisibleForTesting
	int getAvailablePermits() {
		return semaphore.availablePermits();
	}

	@VisibleForTesting
	int getAcquiredPermits() {
		return config.getMaxConcurrentRequests() - semaphore.availablePermits();
	}
}
