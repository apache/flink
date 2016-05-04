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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.table.Row;
import org.apache.flink.configuration.Configuration;

/**
 * OutputFormat to write tuples into a database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 * 
 * @param <OUT>
 * @see Tuple
 * @see DriverManager
 */
public class JDBCOutputFormat extends RichOutputFormat<Row> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JDBCOutputFormat.class);

	private String username;
	private String password;
	private String drivername;
	private String dbURL;
	private String query;
	private int batchInterval = 5000;

	private Connection dbConn;
	private PreparedStatement upload;

	private int batchCount = 0;

	public int[] typesArray;

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
			close();
			throw new IllegalArgumentException("open() failed:\t!", sqe);
		} catch (ClassNotFoundException cnfe) {
			close();
			throw new IllegalArgumentException("JDBC-Class not found:\t", cnfe);
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
	 * <p>
	 * When this method is called, the output format is guaranteed to be opened.
	 * 
	 * WARNING: this may fail if the JDBC driver doesn't handle null correctly and no column types specified in the SqlRow
	 *
	 * @param tuple The records to add to the output.
	 * @throws IOException Thrown, if the records could not be added due to an I/O problem.
	 */
	@Override
	public void writeRecord(Row tuple) throws IOException {
		try {
			for (int index = 0; index < tuple.productArity(); index++) {
				if (tuple.productElement(index) == null && typesArray != null && typesArray.length > 0) {
					if (typesArray.length == tuple.productArity()) {
						upload.setNull(index + 1, typesArray[index]);
					} else {
						LOG.warn("Column SQL types array doesn't match arity of SqlRow! Check the passed array...");
					}
				} else {
					//try generic set if no column type available
					//WARNING: this may fail if the JDBC driver doesn't handle null correctly
					upload.setObject(index + 1, tuple.productElement(index));
				}
			}
			upload.addBatch();
			batchCount++;
			if (batchCount >= batchInterval) {
				upload.executeBatch();
				batchCount = 0;
			}
		} catch (SQLException sqe) {
			close();
			throw new IllegalArgumentException("writeRecord() failed", sqe);
		} catch (IllegalArgumentException iae) {
			close();
			throw new IllegalArgumentException("writeRecord() failed", iae);
		}
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	@Override
	public void close() throws IOException {
		try {
			upload.executeBatch();
			batchCount = 0;
		} catch (SQLException se) {
			throw new IllegalArgumentException("close() failed", se);
		} catch (NullPointerException se) {
		}
		try {
			upload.close();
		} catch (SQLException se) {
			LOG.info("Inputformat couldn't be closed - " + se.getMessage());
		} catch (NullPointerException npe) {
		}
		try {
			dbConn.close();
		} catch (SQLException se) {
			LOG.info("Inputformat couldn't be closed - " + se.getMessage());
		} catch (NullPointerException npe) {
		}
	}

	public static JDBCOutputFormatBuilder buildJDBCOutputFormat() {
		return new JDBCOutputFormatBuilder();
	}

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
		Finalizes the configuration and checks validity.
		@return Configured JDBCOutputFormat
		 */
		public JDBCOutputFormat finish() {
			if (format.username == null) {
				LOG.info("Username was not supplied separately.");
			}
			if (format.password == null) {
				LOG.info("Password was not supplied separately.");
			}
			if (format.dbURL == null) {
				throw new IllegalArgumentException("No dababase URL supplied.");
			}
			if (format.query == null) {
				throw new IllegalArgumentException("No query suplied");
			}
			if (format.drivername == null) {
				throw new IllegalArgumentException("No driver supplied");
			}
			
			return format;
		}
	}

}
