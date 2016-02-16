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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.api.common.io.NonParallelInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.NullValue;

/**
 * InputFormat to read data from a database and generate tuples.
 * The InputFormat has to be configured using the supplied InputFormatBuilder.
 * 
 * @param <OUT>
 * @see Tuple
 * @see DriverManager
 */
public class JDBCInputFormat<OUT extends Tuple> extends RichInputFormat<OUT, InputSplit> implements NonParallelInput {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JDBCInputFormat.class);

	private String username;
	private String password;
	private String drivername;
	private String dbURL;
	private String query;
	private int resultSetType;
	private int resultSetConcurrency;

	private transient Connection dbConn;
	private transient Statement statement;
	private transient ResultSet resultSet;

	private int[] columnTypes = null;

	public JDBCInputFormat() {
	}

	@Override
	public void configure(Configuration parameters) {
	}

	/**
	 * Connects to the source database and executes the query.
	 *
	 * @param ignored
	 * @throws IOException
	 */
	@Override
	public void open(InputSplit ignored) throws IOException {
		try {
			establishConnection();
			statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
			resultSet = statement.executeQuery(query);
		} catch (SQLException se) {
			close();
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC-Class not found. - " + cnfe.getMessage(), cnfe);
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
	 * Closes all resources used.
	 *
	 * @throws IOException Indicates that a resource could not be closed.
	 */
	@Override
	public void close() throws IOException {
		try {
			resultSet.close();
		} catch (SQLException se) {
			LOG.info("Inputformat couldn't be closed - " + se.getMessage());
		} catch (NullPointerException npe) {
		}
		try {
			statement.close();
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

	/**
	 * Checks whether all data has been read.
	 *
	 * @return boolean value indication whether all data has been read.
	 * @throws IOException
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		try {
			if (resultSet.isLast()) {
				close();
				return true;
			}
			return false;
		} catch (SQLException se) {
			throw new IOException("Couldn't evaluate reachedEnd() - " + se.getMessage(), se);
		}
	}

	/**
	 * Stores the next resultSet row in a tuple
	 *
	 * @param tuple
	 * @return tuple containing next row
	 * @throws java.io.IOException
	 */
	@Override
	public OUT nextRecord(OUT tuple) throws IOException {
		try {
			resultSet.next();
			if (columnTypes == null) {
				extractTypes(tuple);
			}
			addValue(tuple);
			return tuple;
		} catch (SQLException se) {
			close();
			throw new IOException("Couldn't read data - " + se.getMessage(), se);
		} catch (NullPointerException npe) {
			close();
			throw new IOException("Couldn't access resultSet", npe);
		}
	}

	private void extractTypes(OUT tuple) throws SQLException, IOException {
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		columnTypes = new int[resultSetMetaData.getColumnCount()];
		if (tuple.getArity() != columnTypes.length) {
			close();
			throw new IOException("Tuple size does not match columncount");
		}
		for (int pos = 0; pos < columnTypes.length; pos++) {
			columnTypes[pos] = resultSetMetaData.getColumnType(pos + 1);
		}
	}

	/**
	 * Enters data value from the current resultSet into a Record.
	 *
	 * @param reuse Target Record.
	 */
	private void addValue(OUT reuse) throws SQLException {
		for (int pos = 0; pos < columnTypes.length; pos++) {
			switch (columnTypes[pos]) {
				case java.sql.Types.NULL:
					reuse.setField(NullValue.getInstance(), pos);
					break;
				case java.sql.Types.BOOLEAN:
					reuse.setField(resultSet.getBoolean(pos + 1), pos);
					break;
				case java.sql.Types.BIT:
					reuse.setField(resultSet.getBoolean(pos + 1), pos);
					break;
				case java.sql.Types.CHAR:
					reuse.setField(resultSet.getString(pos + 1), pos);
					break;
				case java.sql.Types.NCHAR:
					reuse.setField(resultSet.getString(pos + 1), pos);
					break;
				case java.sql.Types.VARCHAR:
					reuse.setField(resultSet.getString(pos + 1), pos);
					break;
				case java.sql.Types.LONGVARCHAR:
					reuse.setField(resultSet.getString(pos + 1), pos);
					break;
				case java.sql.Types.LONGNVARCHAR:
					reuse.setField(resultSet.getString(pos + 1), pos);
					break;
				case java.sql.Types.TINYINT:
					reuse.setField(resultSet.getShort(pos + 1), pos);
					break;
				case java.sql.Types.SMALLINT:
					reuse.setField(resultSet.getShort(pos + 1), pos);
					break;
				case java.sql.Types.BIGINT:
					reuse.setField(resultSet.getLong(pos + 1), pos);
					break;
				case java.sql.Types.INTEGER:
					reuse.setField(resultSet.getInt(pos + 1), pos);
					break;
				case java.sql.Types.FLOAT:
					reuse.setField(resultSet.getDouble(pos + 1), pos);
					break;
				case java.sql.Types.REAL:
					reuse.setField(resultSet.getFloat(pos + 1), pos);
					break;
				case java.sql.Types.DOUBLE:
					reuse.setField(resultSet.getDouble(pos + 1), pos);
					break;
				case java.sql.Types.DECIMAL:
					reuse.setField(resultSet.getBigDecimal(pos + 1).doubleValue(), pos);
					break;
				case java.sql.Types.NUMERIC:
					reuse.setField(resultSet.getBigDecimal(pos + 1).doubleValue(), pos);
					break;
				case java.sql.Types.DATE:
					reuse.setField(resultSet.getDate(pos + 1).toString(), pos);
					break;
				case java.sql.Types.TIME:
					reuse.setField(resultSet.getTime(pos + 1).getTime(), pos);
					break;
				case java.sql.Types.TIMESTAMP:
					reuse.setField(resultSet.getTimestamp(pos + 1).toString(), pos);
					break;
				case java.sql.Types.SQLXML:
					reuse.setField(resultSet.getSQLXML(pos + 1).toString(), pos);
					break;
				default:
					throw new SQLException("Unsupported sql-type [" + columnTypes[pos] + "] on column [" + pos + "]");

				// case java.sql.Types.BINARY:
				// case java.sql.Types.VARBINARY:
				// case java.sql.Types.LONGVARBINARY:
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
				// case java.sql.Types.STRUCT:
			}
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		GenericInputSplit[] split = {
			new GenericInputSplit(0, 1)
		};
		return split;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}
	

	/**
	 * A builder used to set parameters to the output format's configuration in a fluent way.
	 * @return builder
	 */
	public static JDBCInputFormatBuilder buildJDBCInputFormat() {
		return new JDBCInputFormatBuilder();
	}

	public static class JDBCInputFormatBuilder {
		private final JDBCInputFormat format;

		public JDBCInputFormatBuilder() {
			this.format = new JDBCInputFormat();
			this.format.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
			this.format.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
		}

		public JDBCInputFormatBuilder setUsername(String username) {
			format.username = username;
			return this;
		}

		public JDBCInputFormatBuilder setPassword(String password) {
			format.password = password;
			return this;
		}

		public JDBCInputFormatBuilder setDrivername(String drivername) {
			format.drivername = drivername;
			return this;
		}

		public JDBCInputFormatBuilder setDBUrl(String dbURL) {
			format.dbURL = dbURL;
			return this;
		}

		public JDBCInputFormatBuilder setQuery(String query) {
			format.query = query;
			return this;
		}

		public JDBCInputFormatBuilder setResultSetType(int resultSetType) {
			format.resultSetType = resultSetType;
			return this;
		}

		public JDBCInputFormatBuilder setResultSetConcurrency(int resultSetConcurrency) {
			format.resultSetConcurrency = resultSetConcurrency;
			return this;
		}

		public JDBCInputFormat finish() {
			if (format.username == null) {
				LOG.info("Username was not supplied separately.");
			}
			if (format.password == null) {
				LOG.info("Password was not supplied separately.");
			}
			if (format.dbURL == null) {
				throw new IllegalArgumentException("No database URL supplied");
			}
			if (format.query == null) {
				throw new IllegalArgumentException("No query supplied");
			}
			if (format.drivername == null) {
				throw new IllegalArgumentException("No driver supplied");
			}
			return format;
		}
	}

}
