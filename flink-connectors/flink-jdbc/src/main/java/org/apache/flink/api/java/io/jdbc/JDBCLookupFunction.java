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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.java.io.jdbc.JDBCTypeUtil.getFieldFromResultSet;
import static org.apache.flink.api.java.io.jdbc.JDBCTypeUtil.setFieldToStatement;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableFunction} to query fields from JDBC by keys.
 * The query template like:
 * <PRE>
 *   SELECT c, d, e, f from T where a = ? and b = ?
 * </PRE>
 *
 * Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 */
public class JDBCLookupFunction extends TableFunction<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCLookupFunction.class);

	static final int DEFAULT_MAX_RETRY_TIMES = 3;

	private final String query;
	private final String drivername;
	private final String dbURL;
	private final String username;
	private final String password;
	private final TypeInformation[] keyTypes;
	private final int[] keySqlTypes;
	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;
	private final int[] outputSqlTypes;
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;

	private transient Connection dbConn;
	private transient PreparedStatement statement;
	private transient Cache<Row, List<Row>> cache;

	public JDBCLookupFunction(
			String tableName, String username, String password, String drivername, String dbURL,
			String[] fieldNames, TypeInformation[] fieldTypes, String[] keyNames,
			String leftQuote, String rightQuote, long cacheMaxSize, long cacheExpireMs, int maxRetryTimes) {
		this.drivername = drivername;
		this.dbURL = dbURL;
		this.username = username;
		this.password = password;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		List<String> nameList = Arrays.asList(fieldNames);
		this.keyTypes = Arrays.stream(keyNames)
				.map(s -> {
					checkArgument(nameList.contains(s),
							"keyName %s can't find in fieldNames %s.", s, nameList);
					return fieldTypes[nameList.indexOf(s)];
				})
				.toArray(TypeInformation[]::new);
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
		this.keySqlTypes = Arrays.stream(keyTypes).mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();
		this.outputSqlTypes = Arrays.stream(fieldTypes).mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();

		String quoteTableName = leftQuote + tableName + rightQuote;
		String selectFields = StringUtils.join(Arrays.stream(fieldNames)
				.map(name -> leftQuote + name + rightQuote)
				.toArray(String[]::new), ",");
		String conditions = StringUtils.join(Arrays.stream(keyNames)
				.map(name -> leftQuote + name + rightQuote + " = ?")
				.toArray(String[]::new), " AND ");
		this.query = "SELECT " + selectFields + " FROM " + quoteTableName + " WHERE " + conditions;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		try {
			establishConnection();
			statement = dbConn.prepareStatement(query);
			this.cache = cacheMaxSize == -1 ? null : CacheBuilder.newBuilder()
					.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
					.maximumSize(cacheMaxSize)
					.build();
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}
	}

	public void eval(Object... keys) {
		Row keyRow = Row.of(keys);
		if (cache != null) {
			List<Row> cachedRows = cache.getIfPresent(keyRow);
			if (cachedRows != null) {
				for (Row cachedRow : cachedRows) {
					collect(cachedRow);
				}
				return;
			}
		}

		for (int retry = 1; retry <= maxRetryTimes; retry++) {
			try {
				statement.clearParameters();
				for (int i = 0; i < keys.length; i++) {
					setFieldToStatement(i, keySqlTypes[i], keys[i], statement);
				}
				try (ResultSet resultSet = statement.executeQuery()) {
					if (cache == null) {
						while (resultSet.next()) {
							collect(convertToRowFromResultSet(resultSet));
						}
					} else {
						ArrayList<Row> rows = new ArrayList<>();
						while (resultSet.next()) {
							Row row = convertToRowFromResultSet(resultSet);
							rows.add(row);
							collect(row);
						}
						rows.trimToSize();
						cache.put(keyRow, rows);
					}
				}
				break;
			} catch (SQLException e) {
				LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
				if (retry >= maxRetryTimes) {
					throw new RuntimeException("Execution of JDBC statement failed.", e);
				}

				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}
		}
	}

	private Row convertToRowFromResultSet(ResultSet resultSet) throws SQLException {
		Row row = new Row(outputSqlTypes.length);
		for (int i = 0; i < outputSqlTypes.length; i++) {
			row.setField(i, getFieldFromResultSet(i, outputSqlTypes[i], resultSet));
		}
		return row;
	}

	private void establishConnection() throws SQLException, ClassNotFoundException {
		Class.forName(drivername);
		if (username == null) {
			dbConn = DriverManager.getConnection(dbURL);
		} else {
			dbConn = DriverManager.getConnection(dbURL, username, password);
		}
	}

	@Override
	public void close() throws IOException {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				statement = null;
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

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return keyTypes;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for a {@link JDBCLookupFunction}.
	 */
	public static class Builder {
		private String tableName;
		private String username;
		private String password;
		private String drivername;
		private String dbURL;
		private String[] fieldNames;
		private TypeInformation[] fieldTypes;
		private String[] keyNames;
		private String leftQuote = "";
		private String rightQuote = "";
		private long cacheMaxSize = -1;
		private long cacheExpireMs = -1;
		private int maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;

		public Builder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder setUsername(String username) {
			this.username = username;
			return this;
		}

		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		public Builder setDrivername(String drivername) {
			this.drivername = drivername;
			return this;
		}

		public Builder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		public Builder setFieldTypes(TypeInformation[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		public Builder setKeyNames(String[] keyNames) {
			this.keyNames = keyNames;
			return this;
		}

		public Builder setLeftQuote(String leftQuote) {
			this.leftQuote = leftQuote;
			return this;
		}

		public Builder setRightQuote(String rightQuote) {
			this.rightQuote = rightQuote;
			return this;
		}

		public Builder setCacheMaxSize(long cacheMaxSize) {
			this.cacheMaxSize = cacheMaxSize;
			return this;
		}

		public Builder setCacheExpireMs(long cacheExpireMs) {
			this.cacheExpireMs = cacheExpireMs;
			return this;
		}

		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCLookupFunction
		 */
		public JDBCLookupFunction build() {
			checkNotNull(tableName, "No tableName supplied.");
			checkNotNull(drivername, "No driver supplied.");
			checkNotNull(dbURL, "No database URL supplied.");
			checkNotNull(fieldNames, "No fieldNames supplied.");
			checkNotNull(fieldTypes, "No fieldTypes supplied.");
			checkNotNull(keyNames, "No keyNames supplied.");

			return new JDBCLookupFunction(
					tableName, username, password, drivername, dbURL,
					fieldNames, fieldTypes, keyNames,
					leftQuote, rightQuote, cacheMaxSize, cacheExpireMs, maxRetryTimes);
		}
	}
}
