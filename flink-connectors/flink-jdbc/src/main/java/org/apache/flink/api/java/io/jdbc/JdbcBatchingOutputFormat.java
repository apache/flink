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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.io.jdbc.executor.JdbcBatchStatementExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

class JdbcBatchingOutputFormat<In, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>> extends AbstractJdbcOutputFormat<In> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JdbcBatchingOutputFormat.class);

	static final int DEFAULT_MAX_RETRY_TIMES = 3;

	final JdbcDmlOptions dmlOptions;
	private final JdbcBatchOptions batchOptions;

	private transient JdbcExec jdbcStatementExecutor;
	private transient int batchCount = 0;
	private transient volatile boolean closed = false;

	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;
	private final Function<RuntimeContext, JdbcExec> statementRunnerCreator;
	final Function<In, JdbcIn> jdbcRecordExtractor;

	JdbcBatchingOutputFormat(JdbcConnectionOptions connectionOptions,
								JdbcDmlOptions dmlOptions,
								JdbcBatchOptions batchOptions,
								Function<RuntimeContext, JdbcExec> statementExecutorCreator,
								Function<In, JdbcIn> recordExtractor) {
		super(connectionOptions);
		this.dmlOptions = dmlOptions;
		this.batchOptions = batchOptions;
		this.statementRunnerCreator = statementExecutorCreator;
		this.jdbcRecordExtractor = recordExtractor;
	}

	/**
	 * Connects to the target database and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		jdbcStatementExecutor = statementRunnerCreator.apply(getRuntimeContext());
		try {
			jdbcStatementExecutor.open(connection);
		} catch (SQLException e) {
			throw new IOException("unable to open JDBC writer", e);
		}
		if (batchOptions.getIntervalMs() != 0 && batchOptions.getSize() != 1) {
			this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("jdbc-upsert-output-format"));
			this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
				synchronized (JdbcBatchingOutputFormat.this) {
					if (!closed) {
						try {
							flush();
						} catch (Exception e) {
							flushException = e;
						}
					}
				}
			}, batchOptions.getIntervalMs(), batchOptions.getIntervalMs(), TimeUnit.MILLISECONDS);
		}
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to JDBC failed.", flushException);
		}
	}

	@Override
	public final synchronized void writeRecord(In record) {
		checkFlushException();

		try {
			doWriteRecord(record);
			batchCount++;
			if (batchCount >= batchOptions.getSize()) {
				flush();
			}
		} catch (Exception e) {
			throw new RuntimeException("Writing records to JDBC failed.", e);
		}
	}

	void doWriteRecord(In record) throws SQLException {
		jdbcStatementExecutor.process(jdbcRecordExtractor.apply(record));
	}

	@Override
	public synchronized void flush() throws IOException {
		checkFlushException();

		for (int i = 1; i <= dmlOptions.getMaxRetries(); i++) {
			try {
				attemptFlush();
				batchCount = 0;
				break;
			} catch (SQLException e) {
				LOG.error("JDBC executeBatch error, retry times = {}", i, e);
				if (i >= dmlOptions.getMaxRetries()) {
					throw new IOException(e);
				}
				try {
					Thread.sleep(1000 * i);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IOException("unable to flush; interrupted while doing another attempt", e);
				}
			}
		}
	}

	void attemptFlush() throws SQLException {
		jdbcStatementExecutor.executeBatch();
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 */
	@Override
	public synchronized void close() {
		if (!closed) {
			closed = true;

			checkFlushException();

			if (this.scheduledFuture != null) {
				scheduledFuture.cancel(false);
				this.scheduler.shutdown();
			}

			if (batchCount > 0) {
				try {
					flush();
				} catch (Exception e) {
					throw new RuntimeException("Writing records to JDBC failed.", e);
				}
			}

			try {
				jdbcStatementExecutor.close();
			} catch (SQLException e) {
				LOG.warn("Close JDBC writer failed.", e);
			}
		}
		super.close();
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for a {@link JdbcBatchingOutputFormat}.
	 */
	public static class Builder {
		private JDBCOptions options;
		private String[] fieldNames;
		private String[] keyFields;
		private int[] fieldTypes;
		private int flushMaxSize = DEFAULT_FLUSH_MAX_SIZE;
		private long flushIntervalMills = DEFAULT_FLUSH_INTERVAL_MILLS;
		private int maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JDBCOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * required, field names of this jdbc sink.
		 */
		Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		/**
		 * required, upsert unique keys.
		 */
		Builder setKeyFields(String[] keyFields) {
			this.keyFields = keyFields;
			return this;
		}

		/**
		 * required, field types of this jdbc sink.
		 */
		Builder setFieldTypes(int[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		/**
		 * optional, flush max size (includes all append, upsert and delete records),
		 * over this number of records, will flush data.
		 */
		Builder setFlushMaxSize(int flushMaxSize) {
			this.flushMaxSize = flushMaxSize;
			return this;
		}

		/**
		 * optional, flush interval mills, over this time, asynchronous threads will flush data.
		 */
		Builder setFlushIntervalMills(long flushIntervalMills) {
			this.flushIntervalMills = flushIntervalMills;
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCUpsertOutputFormat
		 */
		public JdbcBatchingOutputFormat<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> build() {
			checkNotNull(options, "No options supplied.");
			checkNotNull(fieldNames, "No fieldNames supplied.");
			JdbcDmlOptions dml = JdbcDmlOptions.builder()
					.withTableName(options.getTableName()).withDialect(options.getDialect())
					.withFieldNames(fieldNames).withKeyFields(keyFields).withFieldTypes(fieldTypes).withMaxRetries(maxRetryTimes).build();
			if (dml.getKeyFields() == null || dml.getKeyFields().length == 0) {
				// warn: don't close over builder fields
				String sql = options.getDialect().getInsertIntoStatement(dml.getTableName(), dml.getFieldNames());
				return new JdbcBatchingOutputFormat<>(
						options,
						dml,
						JdbcBatchOptions.builder().withSize(flushMaxSize).withIntervalMs(flushIntervalMills).build(),
						unused -> JdbcBatchStatementExecutor.simpleRow(sql, dml.getFieldTypes()),
						tuple2 -> {
							Preconditions.checkArgument(tuple2.f0);
							return tuple2.f1;
						});
			} else {
				return new TableJdbcUpsertOutputFormat(
						options,
						dml,
						JdbcBatchOptions.builder().withSize(flushMaxSize).withIntervalMs(flushIntervalMills).build());
			}
		}
	}
}
