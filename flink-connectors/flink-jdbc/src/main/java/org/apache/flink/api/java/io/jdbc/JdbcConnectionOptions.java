/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * JDBC connection options.
 */
@PublicEvolving
public class JdbcConnectionOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	protected final String url;
	protected final String driverName;
	@Nullable
	protected final String username;
	@Nullable
	protected final String password;

	JdbcConnectionOptions(String url, String driverName, String username, String password) {
		this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
		this.driverName = Preconditions.checkNotNull(driverName, "driver name is empty");
		this.username = username;
		this.password = password;
	}

	public String getDbURL() {
		return url;
	}

	public String getDriverName() {
		return driverName;
	}

	public Optional<String> getUsername() {
		return Optional.ofNullable(username);
	}

	public Optional<String> getPassword() {
		return Optional.ofNullable(password);
	}

	/**
	 * Builder for {@link JdbcConnectionOptions}.
	 */
	public static class JdbcConnectionOptionsBuilder {
		private String url;
		private String driverName;
		private String username;
		private String password;

		public JdbcConnectionOptionsBuilder withUrl(String url) {
			this.url = url;
			return this;
		}

		public JdbcConnectionOptionsBuilder withDriverName(String driverName) {
			this.driverName = driverName;
			return this;
		}

		public JdbcConnectionOptionsBuilder withUsername(String username) {
			this.username = username;
			return this;
		}

		public JdbcConnectionOptionsBuilder withPassword(String password) {
			this.password = password;
			return this;
		}

		public JdbcConnectionOptions build() {
			return new JdbcConnectionOptions(url, driverName, username, password);
		}
	}
}
