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

import java.io.Serializable;

import static org.apache.flink.api.java.io.jdbc.AbstractJdbcOutputFormat.DEFAULT_CREATE_TABLE_IF_NOT_EXISTS;

/**
 * Options for using JDBC DDL, like create table.
 */
public class JdbcDdlOptions implements Serializable {

	private static final long serialVersionUID = 1L;
	private final JDBCOptions jdbcOptions;
	private final int maxRetry;
	private final boolean createTableIfNotExists;
	private final String createTableStatement;

	private JdbcDdlOptions(
			JDBCOptions jdbcOptions,
			int maxRetry,
			boolean createTableIfNotExists,
			String createTableStatement) {
		this.jdbcOptions = jdbcOptions;
		this.maxRetry = maxRetry;
		this.createTableIfNotExists = createTableIfNotExists;
		this.createTableStatement = createTableStatement;
	}

	public JDBCOptions getJdbcOptions() {
		return jdbcOptions;
	}

	public int getMaxRetry() {
		return maxRetry;
	}

	public boolean createTableIfNotExists() {
		return createTableIfNotExists;
	}

	public String getCreateTableStatement() {
		return createTableStatement;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link JdbcDdlOptions}.
	 */
	public static class Builder {
		private JDBCOptions jdbcOptions;
		private int maxRetry;
		private boolean createTableIfNotExists = DEFAULT_CREATE_TABLE_IF_NOT_EXISTS;
		private String createTableStatement;

		private Builder() {}

		public Builder setJDBCOptions(JDBCOptions jdbcOptions) {
			this.jdbcOptions = jdbcOptions;
			return this;
		}

		public Builder setMaxRetry(int maxRetry) {
			this.maxRetry = maxRetry;
			return this;
		}

		public Builder setCreateTableIfNotExists(boolean createTableIfNotExists) {
			this.createTableIfNotExists = createTableIfNotExists;
			return this;
		}

		public Builder setCreateTableStatement(String createTableStatement) {
			this.createTableStatement = createTableStatement;
			return this;
		}

		public JdbcDdlOptions build() {
			return new JdbcDdlOptions(jdbcOptions, maxRetry, createTableIfNotExists, createTableStatement);
		}
	}
}
