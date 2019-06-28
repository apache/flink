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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.types.Row;

import static org.apache.flink.api.java.io.jdbc.JDBCLookupFunction.DEFAULT_MAX_RETRY_TIMES;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link LookupableTableSource} for JDBC.
 */
public class JDBCLookupTableSource implements LookupableTableSource<Row> {

	private final String tableName;
	private final String username;
	private final String password;
	private final String drivername;
	private final String dbURL;
	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;
	private final String leftQuote;
	private final String rightQuote;
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;

	public JDBCLookupTableSource(String tableName, String username, String password,
			String drivername, String dbURL, String[] fieldNames, TypeInformation[] fieldTypes,
			String leftQuote, String rightQuote, long cacheMaxSize, long cacheExpireMs,
			int maxRetryTimes) {
		this.tableName = tableName;
		this.username = username;
		this.password = password;
		this.drivername = drivername;
		this.dbURL = dbURL;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.leftQuote = leftQuote;
		this.rightQuote = rightQuote;
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		return JDBCLookupFunction.builder()
				.setTableName(tableName)
				.setUsername(username)
				.setPassword(password)
				.setDrivername(drivername)
				.setDBUrl(dbURL)
				.setFieldNames(fieldNames)
				.setFieldTypes(fieldTypes)
				.setKeyNames(lookupKeys)
				.setLeftQuote(leftQuote)
				.setRightQuote(rightQuote)
				.setCacheMaxSize(cacheMaxSize)
				.setCacheExpireMs(cacheExpireMs)
				.setMaxRetryTimes(maxRetryTimes).build();
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public TableSchema getTableSchema() {
		return new TableSchema(fieldNames, fieldTypes);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for a {@link JDBCLookupTableSource}.
	 */
	public static class Builder {
		private String tableName;
		private String username;
		private String password;
		private String drivername;
		private String dbURL;
		private String[] fieldNames;
		private TypeInformation[] fieldTypes;
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
		 * @return Configured JDBCLookupTableSource
		 */
		public JDBCLookupTableSource build() {
			checkNotNull(tableName, "No tableName supplied.");
			checkNotNull(drivername, "No driver supplied.");
			checkNotNull(dbURL, "No database URL supplied.");
			checkNotNull(fieldNames, "No fieldNames supplied.");
			checkNotNull(fieldTypes, "No fieldTypes supplied.");

			return new JDBCLookupTableSource(
					tableName, username, password, drivername, dbURL,
					fieldNames, fieldTypes, leftQuote, rightQuote,
					cacheMaxSize, cacheExpireMs, maxRetryTimes);
		}
	}
}
