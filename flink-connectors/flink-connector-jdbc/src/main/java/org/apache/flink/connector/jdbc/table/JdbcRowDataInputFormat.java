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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * InputFormat for {@link JdbcDynamicTableSource}.
 */
@Internal
public class JdbcRowDataInputFormat extends RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataInputFormat.class);

	private JdbcConnectionOptions connectionOptions;
	private int fetchSize;
	private Boolean autoCommit;
	private Object[][] parameterValues;
	private String queryTemplate;
	private int resultSetType;
	private int resultSetConcurrency;
	private JdbcRowConverter rowConverter;
	private TypeInformation<RowData> rowDataTypeInfo;

	private transient Connection dbConn;
	private transient PreparedStatement statement;
	private transient ResultSet resultSet;
	private transient boolean hasNext;

	private JdbcRowDataInputFormat(
			JdbcConnectionOptions connectionOptions,
			int fetchSize,
			Boolean autoCommit,
			Object[][] parameterValues,
			String queryTemplate,
			int resultSetType,
			int resultSetConcurrency,
			JdbcRowConverter rowConverter,
			TypeInformation<RowData> rowDataTypeInfo) {
		this.connectionOptions = connectionOptions;
		this.fetchSize = fetchSize;
		this.autoCommit = autoCommit;
		this.parameterValues = parameterValues;
		this.queryTemplate = queryTemplate;
		this.resultSetType = resultSetType;
		this.resultSetConcurrency = resultSetConcurrency;
		this.rowConverter = rowConverter;
		this.rowDataTypeInfo = rowDataTypeInfo;
	}

	@Override
	public void configure(Configuration parameters) {
		//do nothing here
	}

	@Override
	public void openInputFormat() {
		//called once per inputFormat (on open)
		try {
			dbConn = new SimpleJdbcConnectionProvider(connectionOptions).getConnection();
			// set autoCommit mode only if it was explicitly configured.
			// keep connection default otherwise.
			if (autoCommit != null) {
				dbConn.setAutoCommit(autoCommit);
			}
			statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
			if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
				statement.setFetchSize(fetchSize);
			}
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
			if (statement != null) {
				statement.close();
			}
		} catch (SQLException se) {
			LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
		} finally {
			statement = null;
		}

		try {
			if (dbConn != null) {
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
	 * and a proper {@link JdbcParameterValuesProvider}, in a <b>non-parallel
	 * fashion</b> otherwise.
	 *
	 * @param inputSplit which is ignored if this InputFormat is executed as a
	 *                   non-parallel source,
	 *                   a "hook" to the query parameters otherwise (using its
	 *                   <i>splitNumber</i>)
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
						throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet).");
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
		if (resultSet == null) {
			return;
		}
		try {
			resultSet.close();
		} catch (SQLException se) {
			LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
		}
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return rowDataTypeInfo;
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
	 * Stores the next resultSet row in a tuple.
	 *
	 * @param reuse row to be reused.
	 * @return row containing next {@link RowData}
	 * @throws IOException
	 */
	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		try {
			if (!hasNext) {
				return null;
			}
			RowData row = rowConverter.toInternal(resultSet);
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
	 *
	 * @return builder
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link JdbcRowDataInputFormat}.
	 */
	public static class Builder {
		private JdbcConnectionOptions.JdbcConnectionOptionsBuilder connOptionsBuilder;
		private int fetchSize;
		private Boolean autoCommit;
		private Object[][] parameterValues;
		private String queryTemplate;
		private JdbcRowConverter rowConverter;
		private TypeInformation<RowData> rowDataTypeInfo;
		private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
		private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;

		public Builder() {
			this.connOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
		}

		public Builder setDrivername(String drivername) {
			this.connOptionsBuilder.withDriverName(drivername);
			return this;
		}

		public Builder setDBUrl(String dbURL) {
			this.connOptionsBuilder.withUrl(dbURL);
			return this;
		}

		public Builder setUsername(String username) {
			this.connOptionsBuilder.withUsername(username);
			return this;
		}

		public Builder setPassword(String password) {
			this.connOptionsBuilder.withPassword(password);
			return this;
		}

		public Builder setQuery(String query) {
			this.queryTemplate = query;
			return this;
		}

		public Builder setParametersProvider(JdbcParameterValuesProvider parameterValuesProvider) {
			this.parameterValues = parameterValuesProvider.getParameterValues();
			return this;
		}

		public Builder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
			this.rowDataTypeInfo = rowDataTypeInfo;
			return this;
		}

		public Builder setRowConverter(JdbcRowConverter rowConverter) {
			this.rowConverter = rowConverter;
			return this;
		}

		public Builder setFetchSize(int fetchSize) {
			Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize > 0,
				"Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
			this.fetchSize = fetchSize;
			return this;
		}

		public Builder setAutoCommit(Boolean autoCommit) {
			this.autoCommit = autoCommit;
			return this;
		}

		public Builder setResultSetType(int resultSetType) {
			this.resultSetType = resultSetType;
			return this;
		}

		public Builder setResultSetConcurrency(int resultSetConcurrency) {
			this.resultSetConcurrency = resultSetConcurrency;
			return this;
		}

		public JdbcRowDataInputFormat build() {
			if (this.queryTemplate == null) {
				throw new IllegalArgumentException("No query supplied");
			}
			if (this.rowConverter == null) {
				throw new IllegalArgumentException("No row converter supplied");
			}
			if (this.parameterValues == null) {
				LOG.debug("No input splitting configured (data will be read with parallelism 1).");
			}
			return new JdbcRowDataInputFormat(
				connOptionsBuilder.build(),
				this.fetchSize,
				this.autoCommit,
				this.parameterValues,
				this.queryTemplate,
				this.resultSetType,
				this.resultSetConcurrency,
				this.rowConverter,
				this.rowDataTypeInfo);
		}
	}
}
