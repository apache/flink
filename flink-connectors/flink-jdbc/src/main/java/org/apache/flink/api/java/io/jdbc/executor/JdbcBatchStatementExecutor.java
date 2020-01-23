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

package org.apache.flink.api.java.io.jdbc.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JDBCWriter used to execute statements (e.g. INSERT, UPSERT, DELETE).
 */
@Internal
public interface JdbcBatchStatementExecutor<T> {

	/**
	 * Open the writer by JDBC Connection. It can create Statement from Connection.
	 */
	void open(Connection connection) throws SQLException;

	void process(T record) throws SQLException;

	/**
	 * Submits a batch of commands to the database for execution.
	 */
	void executeBatch() throws SQLException;

	/**
	 * Close JDBC related statements and other classes.
	 */
	void close() throws SQLException;

	static JdbcBatchStatementExecutor<Row> upsert(
			JDBCDialect dialect,
			String tableName,
			String[] fieldNames,
			int[] fieldTypes,
			String[] keyFields,
			boolean objectReuse) {

		checkNotNull(keyFields);

		int[] pkFields = Arrays.stream(keyFields).mapToInt(Arrays.asList(fieldNames)::indexOf).toArray();
		int[] pkTypes = fieldTypes == null ? null : Arrays.stream(pkFields).map(f -> fieldTypes[f]).toArray();

		return dialect
				.getUpsertStatement(tableName, fieldNames, keyFields)
				.map(sql -> keyed(pkFields, pkTypes, sql, objectReuse))
				.orElseGet(() ->
						new InsertOrUpdateJdbcExecutor(
								fieldTypes, pkFields, pkTypes,
								dialect.getRowExistsStatement(tableName, keyFields),
								dialect.getInsertIntoStatement(tableName, fieldNames),
								dialect.getUpdateStatement(tableName, fieldNames, keyFields),
								objectReuse));
	}

	static JdbcBatchStatementExecutor<Row> keyed(int[] pkFields, int[] pkTypes, String sql, boolean objectReuse) {
		return new KeyedBatchStatementExecutor(pkFields, pkTypes, sql, objectReuse);
	}

	static JdbcBatchStatementExecutor<Row> simple(String sql, int[] fieldTypes, boolean objectReuse) {
		return new SimpleBatchStatementExecutor(sql, fieldTypes, objectReuse);
	}

}
