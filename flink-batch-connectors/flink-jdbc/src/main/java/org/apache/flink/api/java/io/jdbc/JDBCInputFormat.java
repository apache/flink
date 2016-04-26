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

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.io.RangeInputSplit;
import org.apache.flink.api.java.io.GenericRow;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InputFormat to read data from a database and generate tuples.
 * The InputFormat has to be configured using the supplied InputFormatBuilder.
 * 
 * Remark: split (if set) works only if the split column is numeric
 * 
 * @param <OUT>
 * @see Tuple
 * @see DriverManager
 */
public class JDBCInputFormat extends RichInputFormat<GenericRow, InputSplit> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(JDBCInputFormat.class);

	private String username;
	private String password;
	private String drivername;
	private String dbURL;
	private String queryTemplate;
	private int resultSetType;
	private int resultSetConcurrency;

	private transient Connection dbConn;
	private transient Statement statement;
	private transient ResultSet resultSet;
	
	private String splitColumnName;
	private long max;
	private long min;
	private long fetchSize;
	
	private boolean hasNext = true;
	
	private static final String BETWEEN = "(%s BETWEEN %s AND %s)";
	public static final String CONDITIONS = "$CONDITIONS";

	public JDBCInputFormat() {
	}

	@Override
	public void configure(Configuration parameters) {
		//called once per inputFormat (on open)
		try {
			establishConnection();
		}catch (SQLException se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC-Class not found. - " + cnfe.getMessage(), cnfe);
		}
	}

	/**
	 * Connects to the source database and executes the query.
	 *
	 * @param ignored
	 * @throws IOException
	 */
	@Override
	public void open(InputSplit inputSplit) throws IOException {
		hasNext = true;
		try {
			statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
			String query = queryTemplate;
			if(isSplitConfigured()) {
				RangeInputSplit jdbcInputSplit = (RangeInputSplit) inputSplit;
				long start = jdbcInputSplit.getMin();
				long end = jdbcInputSplit.getMax();
				query = queryTemplate.replace(CONDITIONS, String.format(BETWEEN, splitColumnName, start, end));
			}
			LOG.debug(query);
			resultSet = statement.executeQuery(query);
		} catch (SQLException se) {
			close();
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		}
	}

	private boolean isSplitConfigured() {
		return splitColumnName != null;
	}

	/** Perform initialization of the JDBC connection. Done just once per parallel task */
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
			LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
		} catch (NullPointerException npe) {
		}
		try {
			statement.close();
		} catch (SQLException se) {
			LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
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
			if (!hasNext || resultSet.isLast()) {
				close();	
				return true;
			}
			return false;
		} catch (SQLException se) {
			close();	
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
	public GenericRow nextRecord(GenericRow tuple) throws IOException {
		try {
			hasNext = resultSet.next();
			if(!hasNext) {
				return null;
			}
			if (tuple.getFields() == null || tuple.getFields().length == 0 ) {
				ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
				tuple = new GenericRow(resultSetMetaData.getColumnCount());
			}
			for (int pos = 0; pos < tuple.getArity(); pos++) {
				final Object o = resultSet.getObject(pos + 1);
				tuple.setField(o, pos);
			}
			return tuple;
		} catch (SQLException se) {
			close();
			throw new IOException("Couldn't read data - " + se.getMessage(), se);
		} catch (NullPointerException npe) {
			close();
			throw new IOException("Couldn't access resultSet", npe);
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if(!isSplitConfigured()) {
			GenericInputSplit[] split = {
					new GenericInputSplit(0, 1)
				};
			return split;
		}
		
		double maxEelemCount = (max - min) +1;
		int size = new Double(Math.ceil( maxEelemCount / fetchSize)).intValue();
		if(minNumSplits > size) {
			size = minNumSplits;
			fetchSize = new Double(Math.ceil( maxEelemCount / minNumSplits)).intValue();
		}
		RangeInputSplit[] ret = new RangeInputSplit[size];
		int count = 0;
		for (long i = min; i < max; i += fetchSize, count++) {
			long currentLimit = i + fetchSize - 1;
			ret[count] = new RangeInputSplit(count, i, currentLimit);
			if (currentLimit + 1 + fetchSize > max) {
				ret[count+1] = new RangeInputSplit(count, currentLimit + 1, max);
				break;
			}
		}
		return ret;
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
			// The 'isLast' method is only allowed on scroll cursors.
			this.format.resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
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
			format.queryTemplate = query;
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
		public JDBCInputFormatBuilder setSplitConfig(String splitColumnName,long fetchSize, long min, long max) {
			format.splitColumnName = splitColumnName;
			format.fetchSize = fetchSize;
			format.min = min;
			format.max = max;
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
			if (format.queryTemplate == null) {
				throw new IllegalArgumentException("No query supplied");
			}
			if (format.drivername == null) {
				throw new IllegalArgumentException("No driver supplied");
			}
			adjustQueryTemplateIfNecessary();
			return format;
		}

		/** Try to add $CONDITIONS token automatically (at least for straightforward cases) */
		private void adjustQueryTemplateIfNecessary() {
			if(!format.isSplitConfigured()) {
				return;
			}
			if(!format.queryTemplate.toLowerCase().contains("where")) {
				if(format.queryTemplate.contains(";")) {
					format.queryTemplate = format.queryTemplate.replace(";", "");
				}
				format.queryTemplate += " WHERE "+CONDITIONS;
			}else if(!format.queryTemplate.contains(CONDITIONS)) {		
				//if not simple query and no $CONDITIONS avoid dangerous heuristics
				throw new IllegalArgumentException("Usage of splits requires to specify "+CONDITIONS+" in the query for their generation");
			}
		}
	}

}