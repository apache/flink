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

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * OutputFormat to write Rows into a JDBC database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Row
 * @see DriverManager
 */
public class JDBCOutputFormat extends RichOutputFormat<Row> {
	private static final long serialVersionUID = 1L;
	static final int DEFAULT_BATCH_INTERVAL = 5000;

	private static final Logger LOG = LoggerFactory.getLogger(JDBCOutputFormat.class);

	private String username;
	private String password;
	private String drivername;
	private String dbURL;
	private String query;
	private int batchInterval = DEFAULT_BATCH_INTERVAL;

	private Connection dbConn;
	private PreparedStatement upload;

	private int batchCount = 0;

	private int[] typesArray;

	public JDBCOutputFormat() {
	}

	@Override
	public void configure(Configuration parameters) {
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
			upload = dbConn.prepareStatement(query);
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}
	}

	private void establishConnection() throws SQLException, ClassNotFoundException {
		Class.forName(drivername);
		if (username == null) {
			dbConn = DriverManager.getConnection(dbURL);
		} else {
			dbConn = DriverManager.getConnection(dbURL, username, password);
		}
	}

	/**
	 * Adds a record to the prepared statement.
	 *
	 * <p>When this method is called, the output format is guaranteed to be opened.
	 *
	 * <p>WARNING: this may fail when no column types specified (because a best effort approach is attempted in order to
	 * insert a null value but it's not guaranteed that the JDBC driver handles PreparedStatement.setObject(pos, null))
	 *
	 * @param row The records to add to the output.
	 * @see PreparedStatement
	 * @throws IOException Thrown, if the records could not be added due to an I/O problem.
	 */
	@Override
	public void writeRecord(Row row) throws IOException {

		if (typesArray != null && typesArray.length > 0 && typesArray.length != row.getArity()) {
			LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
		}
		try {

			if (typesArray == null) {
				// no types provided
				for (int index = 0; index < row.getArity(); index++) {
					LOG.warn("Unknown column type for column {}. Best effort approach to set its value: {}.", index + 1, row.getField(index));
					upload.setObject(index + 1, row.getField(index));
				}
			} else {
				// types provided
				for (int index = 0; index < row.getArity(); index++) {

					if (row.getField(index) == null) {
						upload.setNull(index + 1, typesArray[index]);
					} else {
						// casting values as suggested by http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
						switch (typesArray[index]) {
							case java.sql.Types.NULL:
								upload.setNull(index + 1, typesArray[index]);
								break;
							case java.sql.Types.BOOLEAN:
							case java.sql.Types.BIT:
								upload.setBoolean(index + 1, (boolean) row.getField(index));
								break;
							case java.sql.Types.CHAR:
							case java.sql.Types.NCHAR:
							case java.sql.Types.VARCHAR:
							case java.sql.Types.LONGVARCHAR:
							case java.sql.Types.LONGNVARCHAR:
								upload.setString(index + 1, (String) row.getField(index));
								break;
							case java.sql.Types.TINYINT:
								upload.setByte(index + 1, (byte) row.getField(index));
								break;
							case java.sql.Types.SMALLINT:
								upload.setShort(index + 1, (short) row.getField(index));
								break;
							case java.sql.Types.INTEGER:
								upload.setInt(index + 1, (int) row.getField(index));
								break;
							case java.sql.Types.BIGINT:
								upload.setLong(index + 1, (long) row.getField(index));
								break;
							case java.sql.Types.REAL:
								upload.setFloat(index + 1, (float) row.getField(index));
								break;
							case java.sql.Types.FLOAT:
							case java.sql.Types.DOUBLE:
								upload.setDouble(index + 1, (double) row.getField(index));
								break;
							case java.sql.Types.DECIMAL:
							case java.sql.Types.NUMERIC:
								upload.setBigDecimal(index + 1, (java.math.BigDecimal) row.getField(index));
								break;
							case java.sql.Types.DATE:
								upload.setDate(index + 1, (java.sql.Date) row.getField(index));
								break;
							case java.sql.Types.TIME:
								upload.setTime(index + 1, (java.sql.Time) row.getField(index));
								break;
							case java.sql.Types.TIMESTAMP:
								upload.setTimestamp(index + 1, (java.sql.Timestamp) row.getField(index));
								break;
							case java.sql.Types.BINARY:
							case java.sql.Types.VARBINARY:
							case java.sql.Types.LONGVARBINARY:
								upload.setBytes(index + 1, (byte[]) row.getField(index));
								break;
							default:
								upload.setObject(index + 1, row.getField(index));
								LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.",
									typesArray[index], index + 1, row.getField(index));
								// case java.sql.Types.SQLXML
								// case java.sql.Types.ARRAY:
								// case java.sql.Types.JAVA_OBJECT:
								// case java.sql.Types.BLOB:
								// case java.sql.Types.CLOB:
								// case java.sql.Types.NCLOB:
								// case java.sql.Types.DATALINK:
								// case java.sql.Types.DISTINCT:
								// case java.sql.Types.OTHER:
								// case java.sql.Types.REF:
								// case java.sql.Types.ROWID:
								// case java.sql.Types.STRUC
						}
					}
				}
			}
			upload.addBatch();
			batchCount++;
		} catch (SQLException e) {
			throw new RuntimeException("Preparation of JDBC statement failed.", e);
		}

		if (batchCount >= batchInterval) {
			// execute batch
			flush();
		}
	}

	void flush() {
		try {
			upload.executeBatch();
			batchCount = 0;
		} catch (SQLException e) {
			throw new RuntimeException("Execution of JDBC statement failed.", e);
		}
	}

	int[] getTypesArray() {
		return typesArray;
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	@Override
	public void close() throws IOException {
		if (upload != null) {
			flush();
			// close the connection
			try {
				upload.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				upload = null;
			}
		}

		if (dbConn != null) {
			try {
				dbConn.close();
			} catch (SQLException se) {
				LOG.info("JDBC connection could not be closed: " + se.getMessage());
			} finally {
				dbConn = null;
			}
		}
	}

	public static JDBCOutputFormatBuilder buildJDBCOutputFormat() {
		return new JDBCOutputFormatBuilder();
	}

	/**
	 * Builder for a {@link JDBCOutputFormat}.
	 */
	public static class JDBCOutputFormatBuilder {
		private final JDBCOutputFormat format;

		protected JDBCOutputFormatBuilder() {
			this.format = new JDBCOutputFormat();
		}

		public JDBCOutputFormatBuilder setUsername(String username) {
			format.username = username;
			return this;
		}

		public JDBCOutputFormatBuilder setPassword(String password) {
			format.password = password;
			return this;
		}

		public JDBCOutputFormatBuilder setDrivername(String drivername) {
			format.drivername = drivername;
			return this;
		}

		public JDBCOutputFormatBuilder setDBUrl(String dbURL) {
			format.dbURL = dbURL;
			return this;
		}

		public JDBCOutputFormatBuilder setQuery(String query) {
			format.query = query;
			return this;
		}

		public JDBCOutputFormatBuilder setBatchInterval(int batchInterval) {
			format.batchInterval = batchInterval;
			return this;
		}

		public JDBCOutputFormatBuilder setSqlTypes(int[] typesArray) {
			format.typesArray = typesArray;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCOutputFormat
		 */
		public JDBCOutputFormat finish() {
			if (format.username == null) {
				LOG.info("Username was not supplied.");
			}
			if (format.password == null) {
				LOG.info("Password was not supplied.");
			}
			if (format.dbURL == null) {
				throw new IllegalArgumentException("No database URL supplied.");
			}
			if (format.query == null) {
				throw new IllegalArgumentException("No query supplied.");
			}
			if (format.drivername == null) {
				throw new IllegalArgumentException("No driver supplied.");
			}

			return format;
		}
	}

}
