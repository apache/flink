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
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.io.jdbc.split.ParameterValuesProvider;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InputFormat to read data from a database and generate Rows.
 * The InputFormat has to be configured using the supplied InputFormatBuilder.
 * A valid RowTypeInfo must be properly configured in the builder, e.g.: </br>
 *
 * <pre><code>
 * TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
 *		BasicTypeInfo.INT_TYPE_INFO,
 *		BasicTypeInfo.STRING_TYPE_INFO,
 *		BasicTypeInfo.STRING_TYPE_INFO,
 *		BasicTypeInfo.DOUBLE_TYPE_INFO,
 *		BasicTypeInfo.INT_TYPE_INFO
 *	};
 *
 * RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
 *
 * JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
 *				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
 *				.setDBUrl("jdbc:derby:memory:ebookshop")
 *				.setQuery("select * from books")
 *				.setRowTypeInfo(rowTypeInfo)
 *				.finish();
 * </code></pre>
 *
 * In order to query the JDBC source in parallel, you need to provide a
 * parameterized query template (i.e. a valid {@link PreparedStatement}) and
 * a {@link ParameterValuesProvider} which provides binding values for the
 * query parameters. E.g.:</br>
 *
 * <pre><code>
 *
 * Serializable[][] queryParameters = new String[2][1];
 * queryParameters[0] = new String[]{"Kumar"};
 * queryParameters[1] = new String[]{"Tan Ah Teck"};
 *
 * JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
 *				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
 *				.setDBUrl("jdbc:derby:memory:ebookshop")
 *				.setQuery("select * from books WHERE author = ?")
 *				.setRowTypeInfo(rowTypeInfo)
 *				.setParametersProvider(new GenericParameterValuesProvider(queryParameters))
 *				.finish();
 * </code></pre>
 *
 * @see Row
 * @see ParameterValuesProvider
 * @see PreparedStatement
 * @see DriverManager
 */
public class JDBCInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(JDBCInputFormat.class);

	private String username;
	private String password;
	private String drivername;
	private String dbURL;
	private String queryTemplate;
	private int resultSetType;
	private int resultSetConcurrency;
	private RowTypeInfo rowTypeInfo;

	private transient Connection dbConn;
	private transient PreparedStatement statement;
	private transient ResultSet resultSet;

	private boolean hasNext;
	private Object[][] parameterValues;

	public JDBCInputFormat() {
	}

	@Override
	public RowTypeInfo getProducedType() {
		return rowTypeInfo;
	}

	@Override
	public void configure(Configuration parameters) {
		//do nothing here
	}

	@Override
	public void openInputFormat() {
		//called once per inputFormat (on open)
		try {
			Class.forName(drivername);
			if (username == null) {
				dbConn = DriverManager.getConnection(dbURL);
			} else {
				dbConn = DriverManager.getConnection(dbURL, username, password);
			}
			statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
		} catch (SQLException se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC-Class not found. - " + cnfe.getMessage(), cnfe);
		}
	}

	@Override
	public void closeInputFormat() {
		//called once per inputFormat (on close)
		try {
			if(statement != null) {
				statement.close();
			}
		} catch (SQLException se) {
			LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
		} finally {
			statement = null;
		}

		try {
			if(dbConn != null) {
				dbConn.close();
			}
		} catch (SQLException se) {
			LOG.info("Inputformat couldn't be closed - " + se.getMessage());
		} finally {
			dbConn = null;
		}

		parameterValues = null;
	}

	/**
	 * Connects to the source database and executes the query in a <b>parallel
	 * fashion</b> if
	 * this {@link InputFormat} is built using a parameterized query (i.e. using
	 * a {@link PreparedStatement})
	 * and a proper {@link ParameterValuesProvider}, in a <b>non-parallel
	 * fashion</b> otherwise.
	 *
	 * @param inputSplit which is ignored if this InputFormat is executed as a
	 *        non-parallel source,
	 *        a "hook" to the query parameters otherwise (using its
	 *        <i>splitNumber</i>)
	 * @throws IOException if there's an error during the execution of the query
	 */
	@Override
	public void open(InputSplit inputSplit) throws IOException {
		try {
			if (inputSplit != null && parameterValues != null) {
				for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
					Object param = parameterValues[inputSplit.getSplitNumber()][i];
					if (param instanceof String) {
						statement.setString(i + 1, (String) param);
					} else if (param instanceof Long) {
						statement.setLong(i + 1, (Long) param);
					} else if (param instanceof Integer) {
						statement.setInt(i + 1, (Integer) param);
					} else if (param instanceof Double) {
						statement.setDouble(i + 1, (Double) param);
					} else if (param instanceof Boolean) {
						statement.setBoolean(i + 1, (Boolean) param);
					} else if (param instanceof Float) {
						statement.setFloat(i + 1, (Float) param);
					} else if (param instanceof BigDecimal) {
						statement.setBigDecimal(i + 1, (BigDecimal) param);
					} else if (param instanceof Byte) {
						statement.setByte(i + 1, (Byte) param);
					} else if (param instanceof Short) {
						statement.setShort(i + 1, (Short) param);
					} else if (param instanceof Date) {
						statement.setDate(i + 1, (Date) param);
					} else if (param instanceof Time) {
						statement.setTime(i + 1, (Time) param);
					} else if (param instanceof Timestamp) {
						statement.setTimestamp(i + 1, (Timestamp) param);
					} else if (param instanceof Array) {
						statement.setArray(i + 1, (Array) param);
					} else {
						//extends with other types if needed
						throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet)." );
					}
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Executing '%s' with parameters %s", queryTemplate, Arrays.deepToString(parameterValues[inputSplit.getSplitNumber()])));
				}
			}
			resultSet = statement.executeQuery();
			hasNext = resultSet.next();
		} catch (SQLException se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		}
	}

	/**
	 * Closes all resources used.
	 *
	 * @throws IOException Indicates that a resource could not be closed.
	 */
	@Override
	public void close() throws IOException {
		if(resultSet == null) {
			return;
		}
		try {
			resultSet.close();
		} catch (SQLException se) {
			LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
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
		return !hasNext;
	}

	/**
	 * Stores the next resultSet row in a tuple
	 *
	 * @param row row to be reused.
	 * @return row containing next {@link Row}
	 * @throws java.io.IOException
	 */
	@Override
	public Row nextRecord(Row row) throws IOException {
		try {
			if (!hasNext) {
				return null;
			}
			for (int pos = 0; pos < row.getArity(); pos++) {
				row.setField(pos, resultSet.getObject(pos + 1));
			}
			//update hasNext after we've read the record
			hasNext = resultSet.next();
			return row;
		} catch (SQLException se) {
			throw new IOException("Couldn't read data - " + se.getMessage(), se);
		} catch (NullPointerException npe) {
			throw new IOException("Couldn't access resultSet", npe);
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (parameterValues == null) {
			return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
		}
		GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = new GenericInputSplit(i, ret.length);
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
			//using TYPE_FORWARD_ONLY for high performance reads
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

		public JDBCInputFormatBuilder setParametersProvider(ParameterValuesProvider parameterValuesProvider) {
			format.parameterValues = parameterValuesProvider.getParameterValues();
			return this;
		}

		public JDBCInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
			format.rowTypeInfo = rowTypeInfo;
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
			if (format.rowTypeInfo == null) {
				throw new IllegalArgumentException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
			}
			if (format.parameterValues == null) {
				LOG.debug("No input splitting configured (data will be read with parallelism 1).");
			}
			return format;
		}

	}

}
