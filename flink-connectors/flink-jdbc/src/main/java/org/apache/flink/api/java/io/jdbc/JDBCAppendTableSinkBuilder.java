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
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.DEFAULT_BATCH_INTERVAL;
import static org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.DEFAULT_IDLE_CONNECTION_CHECK_INTERVAL;
import static org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.DEFAULT_IDLE_CONNECTION_CHECK_TIMEOUT;

/**
 * A builder to configure and build the JDBCAppendTableSink.
 */
public class JDBCAppendTableSinkBuilder {
	private String username;
	private String password;
	private String driverName;
	private String dbURL;
	private String query;
	private int batchSize = DEFAULT_BATCH_INTERVAL;
	private int[] parameterTypes;
	private int idleConnectionCheckInterval = DEFAULT_IDLE_CONNECTION_CHECK_INTERVAL;
	private int idleConnectionCheckTimeout = DEFAULT_IDLE_CONNECTION_CHECK_TIMEOUT;

	/**
	 * Specify the username of the JDBC connection.
	 * @param username the username of the JDBC connection.
	 */
	public JDBCAppendTableSinkBuilder setUsername(String username) {
		this.username = username;
		return this;
	}

	/**
	 * Specify the password of the JDBC connection.
	 * @param password the password of the JDBC connection.
	 */
	public JDBCAppendTableSinkBuilder setPassword(String password) {
		this.password = password;
		return this;
	}

	/**
	 * Specify the name of the JDBC driver that will be used.
	 * @param drivername the name of the JDBC driver.
	 */
	public JDBCAppendTableSinkBuilder setDrivername(String drivername) {
		this.driverName = drivername;
		return this;
	}

	/**
	 * Specify the URL of the JDBC database.
	 * @param dbURL the URL of the database, whose format is specified by the
	 *              corresponding JDBC driver.
	 */
	public JDBCAppendTableSinkBuilder setDBUrl(String dbURL) {
		this.dbURL = dbURL;
		return this;
	}

	/**
	 * Specify the query that the sink will execute. Usually user can specify
	 * INSERT, REPLACE or UPDATE to push the data to the database.
	 * @param query The query to be executed by the sink.
	 * @see org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder#setQuery(String)
	 */
	public JDBCAppendTableSinkBuilder setQuery(String query) {
		this.query = query;
		return this;
	}

	/**
	 * Specify the size of the batch. By default the sink will batch the query
	 * to improve the performance
	 * @param batchSize the size of batch
	 */
	public JDBCAppendTableSinkBuilder setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Specify the type of the rows that the sink will be accepting.
	 * @param types the type of each field
	 */
	public JDBCAppendTableSinkBuilder setParameterTypes(TypeInformation<?>... types) {
		int[] ty = new int[types.length];
		for (int i = 0; i < types.length; ++i) {
			ty[i] = JDBCTypeUtil.typeInformationToSqlType(types[i]);
		}
		this.parameterTypes = ty;
		return this;
	}

	/**
	 * Specify the type of the rows that the sink will be accepting.
	 * @param types the type of each field defined by {@see java.sql.Types}.
	 */
	public JDBCAppendTableSinkBuilder setParameterTypes(int... types) {
		this.parameterTypes = types;
		return this;
	}

	/**
	 * Specify the interval that the idle connection will be checked.
	 * @param idleConnectionCheckInterval the interval in seconds that the
	 *                                    idle connection will be checked.
	 */
	public JDBCAppendTableSinkBuilder setIdleConnectionCheckInterval(int idleConnectionCheckInterval) {
		this.idleConnectionCheckInterval = idleConnectionCheckInterval;
		return this;
	}

	/**
	 * Specify the time in seconds to wait for the database operation used to
	 * validate the connection to complete.
	 * @param idleConnectionCheckTimeout time in seconds to wait while validating
	 *                                   the connection.
	 */
	public JDBCAppendTableSinkBuilder setIdleConnectionCheckTimeout(int idleConnectionCheckTimeout) {
		this.idleConnectionCheckTimeout = idleConnectionCheckTimeout;
		return this;
	}

	/**
	 * Finalizes the configuration and checks validity.
	 *
	 * @return Configured JDBCOutputFormat
	 */
	public JDBCAppendTableSink build() {
		Preconditions.checkNotNull(parameterTypes,
			"Types of the query parameters are not specified." +
			" Please specify types using the setParameterTypes() method.");

		JDBCOutputFormat format = JDBCOutputFormat.buildJDBCOutputFormat()
			.setUsername(username)
			.setPassword(password)
			.setDBUrl(dbURL)
			.setQuery(query)
			.setDrivername(driverName)
			.setBatchInterval(batchSize)
			.setSqlTypes(parameterTypes)
			.setIdleConnectionCheckInterval(idleConnectionCheckInterval)
			.setIdleConnectionCheckTimeout(idleConnectionCheckTimeout)
			.finish();

		return new JDBCAppendTableSink(format);
	}
}
