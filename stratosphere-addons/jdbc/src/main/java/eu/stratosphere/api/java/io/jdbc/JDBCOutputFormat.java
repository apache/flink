/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.io.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.configuration.Configuration;

/**
 * OutputFormat to write tuples into a database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 * 
 * @param <OUT>
 * @see Tuple
 * @see DriverManager
 */
public class JDBCOutputFormat<OUT extends Tuple> implements OutputFormat<OUT> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(JDBCOutputFormat.class);

	private String username;
	private String password;
	private String drivername;
	private String dbURL;
	private String query;
	private int batchInterval = 5000;

	private Connection dbConn;
	private PreparedStatement upload;

	private SupportedTypes[] types = null;

	private int batchCount = 0;

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

	private enum SupportedTypes {
		BOOLEAN,
		BYTE,
		SHORT,
		INTEGER,
		LONG,
		STRING,
		FLOAT,
		DOUBLE
	}

	/**
	 * Adds a record to the prepared statement.
	 * <p>
	 * When this method is called, the output format is guaranteed to be opened.
	 *
	 * @param tuple The records to add to the output.
	 * @throws IOException Thrown, if the records could not be added due to an I/O problem.
	 */
	@Override
	public void writeRecord(OUT tuple) throws IOException {
		try {
			if (query.split("\\?,").length != tuple.getArity()) {
				close();
				throw new IOException("Tuple size does not match columncount");
			}
			if (types == null) {
				extractTypes(tuple);
			}
			addValues(tuple);
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

	private void extractTypes(OUT tuple) {
		types = new SupportedTypes[tuple.getArity()];
		for (int x = 0; x < tuple.getArity(); x++) {
			types[x] = SupportedTypes.valueOf(tuple.getField(x).getClass().getSimpleName().toUpperCase());
		}
	}

	private void addValues(OUT tuple) throws SQLException {
		for (int index = 0; index < tuple.getArity(); index++) {
			switch (types[index]) {
				case BOOLEAN:
					upload.setBoolean(index + 1, (Boolean) tuple.getField(index));
					break;
				case BYTE:
					upload.setByte(index + 1, (Byte) tuple.getField(index));
					break;
				case SHORT:
					upload.setShort(index + 1, (Short) tuple.getField(index));
					break;
				case INTEGER:
					upload.setInt(index + 1, (Integer) tuple.getField(index));
					break;
				case LONG:
					upload.setLong(index + 1, (Long) tuple.getField(index));
					break;
				case STRING:
					upload.setString(index + 1, (String) tuple.getField(index));
					break;
				case FLOAT:
					upload.setFloat(index + 1, (Float) tuple.getField(index));
					break;
				case DOUBLE:
					upload.setDouble(index + 1, (Double) tuple.getField(index));
					break;
			}
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
