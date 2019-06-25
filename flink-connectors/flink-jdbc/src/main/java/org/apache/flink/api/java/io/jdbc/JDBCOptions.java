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

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects;

import java.util.Optional;

/**
 * Options for the JDBC connector.
 */
public class JDBCOptions {

	private String dbURL;
	private String tableName;
	private String driverName;
	private String username;
	private String password;
	private JDBCDialect dialect;

	public JDBCOptions(String dbURL, String tableName, String driverName, String username,
			String password, JDBCDialect dialect) {
		this.dbURL = dbURL;
		this.tableName = tableName;
		this.driverName = driverName;
		this.username = username;
		this.password = password;
		this.dialect = dialect;
	}

	public String getDbURL() {
		return dbURL;
	}

	public String getTableName() {
		return tableName;
	}

	public String getDriverName() {
		return driverName;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public JDBCDialect getDialect() {
		return dialect;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder of {@link JDBCOptions}.
	 */
	public static class Builder {
		private String dbURL;
		private String tableName;
		private String driverName;
		private String username;
		private String password;
		private JDBCDialect dialect;

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

		public Builder setDriverName(String driverName) {
			this.driverName = driverName;
			return this;
		}

		public Builder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		public Builder setDialect(JDBCDialect dialect) {
			this.dialect = dialect;
			return this;
		}

		public JDBCOptions build() {
			if (this.dbURL == null) {
				throw new IllegalArgumentException("No database URL supplied.");
			}
			if (this.tableName == null) {
				throw new IllegalArgumentException("No tableName supplied.");
			}
			if (this.dialect == null) {
				Optional<JDBCDialect> optional = JDBCDialects.get(dbURL);
				this.dialect = optional.orElseGet(() -> {
					throw new IllegalArgumentException("No dialect supplied.");
				});
			}
			if (this.driverName == null) {
				Optional<String> optional = dialect.defaultDriverName();
				this.driverName = optional.orElseGet(() -> {
					throw new IllegalArgumentException("No driverName supplied.");
				});
			}

			return new JDBCOptions(dbURL, tableName, driverName, username, password, dialect);
		}
	}
}
