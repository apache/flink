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

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

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

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.getFieldFromResultSet;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableFunction} to query fields from JDBC by keys.
 * The query template like:
 * <PRE>
 * SELECT c, d, e, f from T where a = ? and b = ?
 * </PRE>
 *
 * <p>Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 */
public class JDBCLookupFunction extends TableFunction<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCLookupFunction.class);
	private static final long serialVersionUID = 1L;

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
			JDBCOptions options, JDBCLookupOptions lookupOptions,
			String[] fieldNames, TypeInformation[] fieldTypes, String[] keyNames) {
		this.drivername = options.getDriverName();
		this.dbURL = options.getDbURL();
		this.username = options.getUsername();
		this.password = options.getPassword();
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
		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.cacheExpireMs = lookupOptions.getCacheExpireMs();
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
		this.keySqlTypes = Arrays.stream(keyTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
		this.outputSqlTypes = Arrays.stream(fieldTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
		this.query = options.getDialect().getSelectFromStatement(
				options.getTableName(), fieldNames, keyNames);
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		try {
			establishConnection();
			statement = dbConn.prepareStatement(query);
			this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
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
					JDBCUtils.setField(statement, keySqlTypes[i], keys[i], i);
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
		if (cache != null) {
			cache.cleanUp();
			cache = null;
		}
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

	/**
	 * Builder for a {@link JDBCLookupFunction}.
	 */
	public static class Builder {
		private JDBCOptions options;
		private JDBCLookupOptions lookupOptions;
		private String[] fieldNames;
		private TypeInformation[] fieldTypes;
		private String[] keyNames;

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JDBCOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, lookup related options.
		 */
		public Builder setLookupOptions(JDBCLookupOptions lookupOptions) {
			this.lookupOptions = lookupOptions;
			return this;
		}

		/**
		 * required, field names of this jdbc table.
		 */
		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		/**
		 * required, field types of this jdbc table.
		 */
		public Builder setFieldTypes(TypeInformation[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		/**
		 * required, key names to query this jdbc table.
		 */
		public Builder setKeyNames(String[] keyNames) {
			this.keyNames = keyNames;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCLookupFunction
		 */
		public JDBCLookupFunction build() {
			checkNotNull(options, "No JDBCOptions supplied.");
			if (lookupOptions == null) {
				lookupOptions = JDBCLookupOptions.builder().build();
			}
			checkNotNull(fieldNames, "No fieldNames supplied.");
			checkNotNull(fieldTypes, "No fieldTypes supplied.");
			checkNotNull(keyNames, "No keyNames supplied.");

			return new JDBCLookupFunction(options, lookupOptions, fieldNames, fieldTypes, keyNames);
		}
	}
}
