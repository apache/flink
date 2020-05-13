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

import org.apache.flink.api.java.io.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

/**
 * OutputFormat to write Rows into a JDBC database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Row
 * @see DriverManager
 */
/**
 * @deprecated use {@link JdbcBatchingOutputFormat}
 */
@Deprecated
public class JDBCOutputFormat extends AbstractJdbcOutputFormat<Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JDBCOutputFormat.class);

	final JdbcInsertOptions insertOptions;
	private final JdbcExecutionOptions batchOptions;

	private transient volatile boolean closed = false;
	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;

	private transient PreparedStatement upload;
	private transient int batchCount = 0;

	/**
	 * @deprecated use {@link JDBCOutputFormatBuilder builder} instead.
	 */
	@Deprecated
	public JDBCOutputFormat(String username, String password, String drivername, String dbURL, String query, int batchInterval, int[] typesArray) {
		this(new SimpleJdbcConnectionProvider(new JdbcConnectionOptionsBuilder().withUrl(dbURL).withDriverName(drivername).withUsername(username).withPassword(password).build()),
				new JdbcInsertOptions(query, typesArray),
				JdbcExecutionOptions.builder().withBatchSize(batchInterval).build());
	}

	private JDBCOutputFormat(JdbcConnectionProvider connectionProvider, JdbcInsertOptions insertOptions, JdbcExecutionOptions batchOptions) {
		super(connectionProvider);
		this.insertOptions = insertOptions;
		this.batchOptions = batchOptions;
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
		super.open(taskNumber, numTasks);
		try {
			upload = connection.prepareStatement(insertOptions.getQuery());
		} catch (SQLException sqe) {
			throw new IOException("open() failed.", sqe);
		}

		if (batchOptions.getBatchIntervalMs() != DEFAULT_FLUSH_INTERVAL_MILLS && batchOptions.getBatchSize() != 1) {
			this.scheduler = Executors.newScheduledThreadPool(
				1, new ExecutorThreadFactory("jdbc-output-format"));
			this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
				synchronized (JDBCOutputFormat.this) {
					if (closed) {
						return;
					}
					try {
						flush();
					} catch (Exception e) {
						flushException = e;
					}
				}
			}, batchOptions.getBatchIntervalMs(), batchOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
		}
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to JDBC failed.", flushException);
		}
	}

	@Override
	public void writeRecord(Row row) {
		checkFlushException();

		try {
			setRecordToStatement(upload, insertOptions.getFieldTypes(), row);
			upload.addBatch();
		} catch (SQLException e) {
			throw new RuntimeException("Preparation of JDBC statement failed.", e);
		}

		batchCount++;

		if (batchCount >= batchOptions.getBatchSize()) {
			// execute batch
			flush();
		}
	}

	@Override
	public void flush() {
		checkFlushException();

		try {
			upload.executeBatch();
			batchCount = 0;
		} catch (SQLException e) {
			throw new RuntimeException("Execution of JDBC statement failed.", e);
		}
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 */
	@Override
	public void close() {
		if (closed) {
			return;
		}
		closed = true;

		checkFlushException();

		if (this.scheduledFuture != null) {
			scheduledFuture.cancel(false);
			this.scheduler.shutdown();
		}

		if (upload != null) {
			flush();
			try {
				upload.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				upload = null;
			}
		}

		super.close();
	}

	public static JDBCOutputFormatBuilder buildJDBCOutputFormat() {
		return new JDBCOutputFormatBuilder();
	}

	public int[] getFieldTypes() {
		return insertOptions.getFieldTypes();
	}

	/**
	 * Builder for {@link JDBCOutputFormat}.
	 */
	public static class JDBCOutputFormatBuilder {
		private String username;
		private String password;
		private String drivername;
		private String dbURL;
		private String query;
		private int batchInterval = DEFAULT_FLUSH_MAX_SIZE;
		private long flushIntervalMills = DEFAULT_FLUSH_INTERVAL_MILLS;
		private int[] typesArray;

		protected JDBCOutputFormatBuilder() {}

		public JDBCOutputFormatBuilder setUsername(String username) {
			this.username = username;
			return this;
		}

		public JDBCOutputFormatBuilder setPassword(String password) {
			this.password = password;
			return this;
		}

		public JDBCOutputFormatBuilder setDrivername(String drivername) {
			this.drivername = drivername;
			return this;
		}

		public JDBCOutputFormatBuilder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		public JDBCOutputFormatBuilder setQuery(String query) {
			this.query = query;
			return this;
		}

		public JDBCOutputFormatBuilder setBatchInterval(int batchInterval) {
			this.batchInterval = batchInterval;
			return this;
		}

		public JDBCOutputFormatBuilder setSqlTypes(int[] typesArray) {
			this.typesArray = typesArray;
			return this;
		}

		public JDBCOutputFormatBuilder setFlushIntervalMills(long flushIntervalMills) {
			this.flushIntervalMills = flushIntervalMills;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCOutputFormat
		 */
		public JDBCOutputFormat finish() {
			return new JDBCOutputFormat(
				new SimpleJdbcConnectionProvider(buildConnectionOptions()),
				new JdbcInsertOptions(query, typesArray),
				JdbcExecutionOptions.builder().withBatchIntervalMs(flushIntervalMills).withBatchSize(batchInterval).build());
		}

		public JdbcConnectionOptions buildConnectionOptions() {
			if (this.username == null) {
				LOG.info("Username was not supplied.");
			}
			if (this.password == null) {
				LOG.info("Password was not supplied.");
			}

			return new JdbcConnectionOptionsBuilder()
				.withUrl(dbURL)
				.withDriverName(drivername)
				.withUsername(username)
				.withPassword(password)
				.build();
		}
	}

}
