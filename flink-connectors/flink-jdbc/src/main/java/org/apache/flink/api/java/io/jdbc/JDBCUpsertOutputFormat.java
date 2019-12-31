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

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.writer.AppendOnlyWriter;
import org.apache.flink.api.java.io.jdbc.writer.JDBCWriter;
import org.apache.flink.api.java.io.jdbc.writer.UpsertWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An upsert OutputFormat for JDBC.
 */
public class JDBCUpsertOutputFormat extends AbstractJDBCOutputFormat<Tuple2<Boolean, Row>> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JDBCUpsertOutputFormat.class);

	static final int DEFAULT_MAX_RETRY_TIMES = 3;

	private final String tableName;
	private final JDBCDialect dialect;
	private final String[] fieldNames;
	private final String[] keyFields;
	private final int[] fieldTypes;

	private final int flushMaxSize;
	private final long flushIntervalMills;
	private final int maxRetryTimes;

	private transient JDBCWriter jdbcWriter;
	private transient int batchCount = 0;
	private transient volatile boolean closed = false;

	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture scheduledFuture;
	private transient volatile Exception flushException;

	public JDBCUpsertOutputFormat(
			JDBCOptions options,
			String[] fieldNames,
			String[] keyFields,
			int[] fieldTypes,
			int flushMaxSize,
			long flushIntervalMills,
			int maxRetryTimes) {
		super(options.getUsername(), options.getPassword(), options.getDriverName(), options.getDbURL());
		this.tableName = options.getTableName();
		this.dialect = options.getDialect();
		this.fieldNames = fieldNames;
		this.keyFields = keyFields;
		this.fieldTypes = fieldTypes;
		this.flushMaxSize = flushMaxSize;
		this.flushIntervalMills = flushIntervalMills;
		this.maxRetryTimes = maxRetryTimes;
	}

	/**
	 * Connects to the target database and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 * @throws IOException Thrown, if the output could not be opened due to an
	 * I/O problem.
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			establishConnection();
			if (keyFields == null || keyFields.length == 0) {
				String insertSQL = dialect.getInsertIntoStatement(tableName, fieldNames);
				jdbcWriter = new AppendOnlyWriter(insertSQL, fieldTypes);
			} else {
				jdbcWriter = UpsertWriter.create(
					dialect, tableName, fieldNames, fieldTypes, keyFields,
					getRuntimeContext().getExecutionConfig().isObjectReuseEnabled());
			}
			jdbcWriter.open(connection);
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}

		if (flushIntervalMills != 0 && flushMaxSize != 1) {
			this.scheduler = Executors.newScheduledThreadPool(
					1, new ExecutorThreadFactory("jdbc-upsert-output-format"));
			this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
				synchronized (JDBCUpsertOutputFormat.this) {
					if (closed) {
						return;
					}
					try {
						flush();
					} catch (Exception e) {
						flushException = e;
					}
				}
			}, flushIntervalMills, flushIntervalMills, TimeUnit.MILLISECONDS);
		}
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to JDBC failed.", flushException);
		}
	}

	@Override
	public synchronized void writeRecord(Tuple2<Boolean, Row> tuple2) throws IOException {
		checkFlushException();

		try {
			jdbcWriter.addRecord(tuple2);
			batchCount++;
			if (batchCount >= flushMaxSize) {
				flush();
			}
		} catch (Exception e) {
			throw new RuntimeException("Writing records to JDBC failed.", e);
		}
	}

	public synchronized void flush() throws Exception {
		checkFlushException();

		for (int i = 1; i <= maxRetryTimes; i++) {
			try {
				jdbcWriter.executeBatch();
				batchCount = 0;
				break;
			} catch (SQLException e) {
				LOG.error("JDBC executeBatch error, retry times = {}", i, e);
				if (i >= maxRetryTimes) {
					throw e;
				}
				Thread.sleep(1000 * i);
			}
		}
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	@Override
	public synchronized void close() throws IOException {
		if (closed) {
			return;
		}
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
			jdbcWriter.close();
		} catch (SQLException e) {
			LOG.warn("Close JDBC writer failed.", e);
		}

		closeDbConnection();
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for a {@link JDBCUpsertOutputFormat}.
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
		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		/**
		 * required, upsert unique keys.
		 */
		public Builder setKeyFields(String[] keyFields) {
			this.keyFields = keyFields;
			return this;
		}

		/**
		 * required, field types of this jdbc sink.
		 */
		public Builder setFieldTypes(int[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		/**
		 * optional, flush max size (includes all append, upsert and delete records),
		 * over this number of records, will flush data.
		 */
		public Builder setFlushMaxSize(int flushMaxSize) {
			this.flushMaxSize = flushMaxSize;
			return this;
		}

		/**
		 * optional, flush interval mills, over this time, asynchronous threads will flush data.
		 */
		public Builder setFlushIntervalMills(long flushIntervalMills) {
			this.flushIntervalMills = flushIntervalMills;
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCUpsertOutputFormat
		 */
		public JDBCUpsertOutputFormat build() {
			checkNotNull(options, "No options supplied.");
			checkNotNull(fieldNames, "No fieldNames supplied.");
			return new JDBCUpsertOutputFormat(
				options, fieldNames, keyFields, fieldTypes, flushMaxSize, flushIntervalMills, maxRetryTimes);
		}
	}
}
