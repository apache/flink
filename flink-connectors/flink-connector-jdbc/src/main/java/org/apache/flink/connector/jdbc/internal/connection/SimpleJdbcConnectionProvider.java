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

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Simple JDBC connection provider.
 */
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

	private static final long serialVersionUID = 1L;

	private final JdbcConnectionOptions jdbcOptions;

	private transient volatile Connection connection;

	public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
		this.jdbcOptions = jdbcOptions;
	}

	@Override
	public Connection getConnection() throws SQLException, ClassNotFoundException {
		if (connection == null) {
			synchronized (this) {
				if (connection == null) {
					Class.forName(jdbcOptions.getDriverName());
					if (jdbcOptions.getUsername().isPresent()) {
						connection = DriverManager.getConnection(jdbcOptions.getDbURL(), jdbcOptions.getUsername().get(), jdbcOptions.getPassword().orElse(null));
					} else {
						connection = DriverManager.getConnection(jdbcOptions.getDbURL());
					}
				}
			}
		}
		return connection;
	}
}
