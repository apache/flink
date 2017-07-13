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

/**
 * A builder to configure and build the JDBCAppendTableSink.
 */
public class JDBCAppendTableSinkBuilder {
	private String username;
	private String password;
	private String driverName;
	private String dbURL;
	private String query;
	private int batchInterval = DEFAULT_BATCH_INTERVAL;
	private TypeInformation<?>[] fieldTypes;

	public JDBCAppendTableSinkBuilder setUsername(String username) {
		this.username = username;
		return this;
	}

	public JDBCAppendTableSinkBuilder setPassword(String password) {
		this.password = password;
		return this;
	}

	public JDBCAppendTableSinkBuilder setDrivername(String drivername) {
		this.driverName = drivername;
		return this;
	}

	public JDBCAppendTableSinkBuilder setDBUrl(String dbURL) {
		this.dbURL = dbURL;
		return this;
	}

	public JDBCAppendTableSinkBuilder setQuery(String query) {
		this.query = query;
		return this;
	}

	public JDBCAppendTableSinkBuilder setBatchInterval(int batchInterval) {
		this.batchInterval = batchInterval;
		return this;
	}

	public JDBCAppendTableSinkBuilder setFieldTypes(TypeInformation<?>[] fieldTypes) {
		this.fieldTypes = fieldTypes;
		return this;
	}

	/**
	 * Finalizes the configuration and checks validity.
	 *
	 * @return Configured JDBCOutputFormat
	 */
	public JDBCAppendTableSink build() {
		Preconditions.checkNotNull(fieldTypes, "Row type is unspecified");
		int[] types = new int[fieldTypes.length];
		for (int i = 0; i < types.length; ++i) {
			types[i] = JDBCTypeUtil.typeInformationToSqlType(fieldTypes[i]);
		}

		JDBCOutputFormat format = JDBCOutputFormat.buildJDBCOutputFormat()
			.setUsername(username)
			.setPassword(password)
			.setDBUrl(dbURL)
			.setQuery(query)
			.setDrivername(driverName)
			.setBatchInterval(batchInterval)
			.setSqlTypes(types)
			.finish();

		return new JDBCAppendTableSink(format);
	}
}
