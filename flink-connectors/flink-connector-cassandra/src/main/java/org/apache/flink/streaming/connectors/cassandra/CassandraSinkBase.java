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
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CassandraSinkBase is the common abstract class of {@link CassandraPojoSink} and {@link CassandraTupleSink}.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class CassandraSinkBase<IN, V> extends RichSinkFunction<IN> implements CheckpointedFunction {
	protected final Logger log = LoggerFactory.getLogger(getClass());
	protected transient Cluster cluster;
	protected transient Session session;

	protected transient volatile Throwable asyncError;
	protected transient FutureCallback<V> callback;

	protected final ClusterBuilder builder;

	private final AtomicInteger updatesPending = new AtomicInteger();

	/**
	 * If true, the producer will wait until all outstanding action requests have been sent to C*.
	 */
	private boolean flushOnCheckpoint = true;

	CassandraSinkBase(ClusterBuilder builder) {
		this.builder = builder;
		ClosureCleaner.clean(builder, true);
	}

	/**
	 * If set to true, the Flink producer will wait for all in-flight messages in the Cassandra client
	 * to be acknowledged on a checkpoint, so that producer can guarantee those messages are part of the checkpoint.
	 *
	 * @param isFlush a boolean flag indicating the flushing mode (true = flushPending on checkpoint)
	 */
	public void setFlushOnCheckpoint(boolean isFlush) {
		this.flushOnCheckpoint = isFlush;
	}

	@Override
	public void open(Configuration configuration) {
		this.callback = new FutureCallback<V>() {
			@Override
			public void onSuccess(V ignored) {
				int pending = updatesPending.decrementAndGet();

				if (log.isTraceEnabled()) {
					log.trace("onSuccess {} => {}", pending + 1, pending);
				}

				if (pending == 0) {
					synchronized (updatesPending) {
						updatesPending.notifyAll();
					}
				}
			}

			@Override
			public void onFailure(Throwable t) {
				int pending = updatesPending.decrementAndGet();

				if (log.isTraceEnabled()) {
					log.trace("onFailure {} => {}", pending + 1, pending);
				}

				if (pending == 0) {
					synchronized (updatesPending) {
						updatesPending.notifyAll();
					}
				}
				asyncError = t;

				log.error("Error while sending value.", t);
			}
		};
		this.cluster = builder.getCluster();
		this.session = cluster.connect();
	}

	@Override
	public void invoke(IN value) throws Exception {
		if (asyncError != null) {
			throw new IOException("Error while sending value.", asyncError);
		}
		ListenableFuture<V> result = send(value);
		int pending = updatesPending.incrementAndGet();

		if (log.isTraceEnabled()) {
			log.trace("invoke {} => {}", pending - 1, pending);
		}

		Futures.addCallback(result, callback);
	}

	public abstract ListenableFuture<V> send(IN value);

	@Override
	public void close() throws Exception {
		try {
			if (asyncError != null) {
				throw new IOException("Error while sending value.", asyncError);
			}

			flushPending();

			if (asyncError != null) {
				throw new IOException("Error while sending value.", asyncError);
			}
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

	/**
	 * Flush pending records.
	 */
	protected void flushPending() throws InterruptedException {
		if (log.isTraceEnabled()) {
			log.trace("flush pending records ...");
		}

		while (updatesPending.get() > 0) {
			synchronized (updatesPending) {
				updatesPending.wait();
			}
		}

		if (log.isTraceEnabled()) {
			log.trace("flush pending records ... DONE");
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		//currently nothing
	}

	@Override
	public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
		// check for asynchronous errors and fail the checkpoint if necessary
		checkAsyncErrors();

		//optionally flush the pending records iff the flag is enabled.
		if (flushOnCheckpoint) {
			flushPending();

			// if the flushed requests has errors, we should propagate it also and fail the checkpoint
			checkAsyncErrors();
		}
	}

	private void checkAsyncErrors() throws Exception {
		Throwable error = asyncError;
		if (error != null) {
			// prevent throwing duplicated error
			asyncError = null;
			throw new IllegalStateException("Failed to send data to Cassandra: " + error.getMessage(), error);
		}
	}

	@VisibleForTesting
	protected long getNumOfPendingRecords() {
		return updatesPending.get();
	}
}
