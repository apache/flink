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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The sink function for HBase.
 *
 * <p>This class leverage {@link BufferedMutator} to buffer multiple
 * {@link org.apache.hadoop.hbase.client.Mutation Mutations} before sending the requests to cluster.
 * The buffering strategy can be configured by {@code bufferFlushMaxSizeInBytes},
 * {@code bufferFlushMaxMutations} and {@code bufferFlushIntervalMillis}.</p>
 */
@Internal
public class HBaseSinkFunction<T>
		extends RichSinkFunction<T>
		implements CheckpointedFunction, BufferedMutator.ExceptionListener {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(HBaseSinkFunction.class);

	private final String hTableName;
	private final byte[] serializedConfig;

	private final long bufferFlushMaxSizeInBytes;
	private final long bufferFlushMaxMutations;
	private final long bufferFlushIntervalMillis;
	private final HBaseMutationConverter<T> mutationConverter;

	private transient Connection connection;
	private transient BufferedMutator mutator;

	private transient ScheduledExecutorService executor;
	private transient ScheduledFuture scheduledFuture;
	private transient AtomicLong numPendingRequests;

	private transient volatile boolean closed = false;

	/**
	 * This is set from inside the {@link BufferedMutator.ExceptionListener} if a {@link Throwable}
	 * was thrown.
	 *
	 * <p>Errors will be checked and rethrown before processing each input element, and when the sink is closed.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public HBaseSinkFunction(
			String hTableName,
			org.apache.hadoop.conf.Configuration conf,
			HBaseMutationConverter<T> mutationConverter,
			long bufferFlushMaxSizeInBytes,
			long bufferFlushMaxMutations,
			long bufferFlushIntervalMillis) {
		this.hTableName = hTableName;
		// Configuration is not serializable
		this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(conf);
		this.mutationConverter = mutationConverter;
		this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
		this.bufferFlushMaxMutations = bufferFlushMaxMutations;
		this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("start open ...");
		org.apache.hadoop.conf.Configuration config = prepareRuntimeConfiguration();
		try {
			this.mutationConverter.open();
			this.numPendingRequests = new AtomicLong(0);

			if (null == connection) {
				this.connection = ConnectionFactory.createConnection(config);
			}
			// create a parameter instance, set the table name and custom listener reference.
			BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(hTableName))
				.listener(this);
			if (bufferFlushMaxSizeInBytes > 0) {
				params.writeBufferSize(bufferFlushMaxSizeInBytes);
			}
			this.mutator = connection.getBufferedMutator(params);

			if (bufferFlushIntervalMillis > 0 && bufferFlushMaxMutations != 1) {
				this.executor = Executors.newScheduledThreadPool(
					1, new ExecutorThreadFactory("hbase-upsert-sink-flusher"));
				this.scheduledFuture = this.executor.scheduleWithFixedDelay(() -> {
					if (closed) {
						return;
					}
					try {
						flush();
					} catch (Exception e) {
						// fail the sink and skip the rest of the items
						// if the failure handler decides to throw an exception
						failureThrowable.compareAndSet(null, e);
					}
				}, bufferFlushIntervalMillis, bufferFlushIntervalMillis, TimeUnit.MILLISECONDS);
			}
		} catch (TableNotFoundException tnfe) {
			LOG.error("The table " + hTableName + " not found ", tnfe);
			throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to HBase.", ioe);
			throw new RuntimeException("Cannot create connection to HBase.", ioe);
		}
		LOG.info("end open.");
	}

	private org.apache.hadoop.conf.Configuration prepareRuntimeConfiguration() throws IOException {
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		// and overwrite configuration using serialized configuration from client-side env (`hbase-site.xml` in classpath).
		// user params from client-side have the highest priority
		org.apache.hadoop.conf.Configuration runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, HBaseConfigurationUtil.getHBaseConfiguration());

		// do validation: check key option(s) in final runtime configuration
		if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
			LOG.error("Can not connect to HBase without {} configuration", HConstants.ZOOKEEPER_QUORUM);
			throw new IOException("Check HBase configuration failed, lost: '" + HConstants.ZOOKEEPER_QUORUM + "'!");
		}

		return runtimeConfig;
	}

	private void checkErrorAndRethrow() {
		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occurred in HBaseSink.", cause);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void invoke(T value, Context context) throws Exception {
		checkErrorAndRethrow();

		mutator.mutate(mutationConverter.convertToMutation(value));

		// flush when the buffer number of mutations greater than the configured max size.
		if (bufferFlushMaxMutations > 0 && numPendingRequests.incrementAndGet() >= bufferFlushMaxMutations) {
			flush();
		}
	}

	private void flush() throws IOException {
		// BufferedMutator is thread-safe
		mutator.flush();
		numPendingRequests.set(0);
		checkErrorAndRethrow();
	}

	@Override
	public void close() throws Exception {
		closed = true;

		if (mutator != null) {
			try {
				mutator.close();
			} catch (IOException e) {
				LOG.warn("Exception occurs while closing HBase BufferedMutator.", e);
			}
			this.mutator = null;
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				LOG.warn("Exception occurs while closing HBase Connection.", e);
			}
			this.connection = null;
		}

		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		while (numPendingRequests.get() != 0) {
			flush();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do.
	}

	@Override
	public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
		// fail the sink and skip the rest of the items
		// if the failure handler decides to throw an exception
		failureThrowable.compareAndSet(null, exception);
	}
}
