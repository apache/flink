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

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * OutputFormat to write Rows into a JDBC database.
 *
 * @see Row
 * @see DriverManager
 */
public abstract class AbstractJDBCOutputFormat<T> extends RichOutputFormat<T> {

	private static final long serialVersionUID = 1L;
	static final int DEFAULT_FLUSH_MAX_SIZE = 5000;
	static final long DEFAULT_FLUSH_INTERVAL_MILLS = 0;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractJDBCOutputFormat.class);

	private final String username;
	private final String password;
	private final String drivername;
	protected final String dbURL;

	protected transient Connection connection;

	public AbstractJDBCOutputFormat(String username, String password, String drivername, String dbURL) {
		this.username = username;
		this.password = password;
		this.drivername = drivername;
		this.dbURL = dbURL;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	protected void establishConnection() throws SQLException, ClassNotFoundException {
		Class.forName(drivername);
		if (username == null) {
			connection = DriverManager.getConnection(dbURL);
		} else {
			connection = DriverManager.getConnection(dbURL, username, password);
		}
	}

	protected void closeDbConnection() throws IOException {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException se) {
				LOG.warn("JDBC connection could not be closed: " + se.getMessage());
			} finally {
				connection = null;
			}
		}
	}
}
