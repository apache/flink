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

package org.apache.flink.connector.jdbc.internal.options;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Options for the JDBC connector.
 */
public class JdbcOptions extends JdbcConnectionOptions {

	private static final long serialVersionUID = 1L;

	private String tableName;
	private JdbcDialect dialect;

	private JdbcOptions(String dbURL, String tableName, String driverName, String username,
						String password, JdbcDialect dialect) {
		super(dbURL, driverName, username, password);
		this.tableName = tableName;
		this.dialect = dialect;
	}

	public String getTableName() {
		return tableName;
	}

	public JdbcDialect getDialect() {
		return dialect;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JdbcOptions) {
			JdbcOptions options = (JdbcOptions) o;
			return Objects.equals(url, options.url) &&
				Objects.equals(tableName, options.tableName) &&
				Objects.equals(driverName, options.driverName) &&
				Objects.equals(username, options.username) &&
				Objects.equals(password, options.password) &&
				Objects.equals(dialect.getClass().getName(), options.dialect.getClass().getName());
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link JdbcOptions}.
	 */
	public static class Builder {
		private String dbURL;
		private String tableName;
		private String driverName;
		private String username;
		private String password;
		private JdbcDialect dialect;

		/**
		 * required, table name.
		 */
		public Builder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		/**
		 * optional, user name.
		 */
		public Builder setUsername(String username) {
			this.username = username;
			return this;
		}

		/**
		 * optional, password.
		 */
		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		/**
		 * optional, driver name, dialect has a default driver name,
		 * See {@link JdbcDialect#defaultDriverName}.
		 */
		public Builder setDriverName(String driverName) {
			this.driverName = driverName;
			return this;
		}

		/**
		 * required, JDBC DB url.
		 */
		public Builder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		/**
		 * optional, Handle the SQL dialect of jdbc driver. If not set, it will be infer by
		 * {@link JdbcDialects#get} from DB url.
		 */
		public Builder setDialect(JdbcDialect dialect) {
			this.dialect = dialect;
			return this;
		}

		public JdbcOptions build() {
			checkNotNull(dbURL, "No dbURL supplied.");
			checkNotNull(tableName, "No tableName supplied.");
			if (this.dialect == null) {
				Optional<JdbcDialect> optional = JdbcDialects.get(dbURL);
				this.dialect = optional.orElseGet(() -> {
					throw new NullPointerException("Unknown dbURL,can not find proper dialect.");
				});
			}
			if (this.driverName == null) {
				Optional<String> optional = dialect.defaultDriverName();
				this.driverName = optional.orElseGet(() -> {
					throw new NullPointerException("No driverName supplied.");
				});
			}

			return new JdbcOptions(dbURL, tableName, driverName, username, password, dialect);
		}
	}
}
