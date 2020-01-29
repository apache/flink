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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.io.jdbc.JdbcDmlOptions;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.getPrimaryKey;
import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;
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

	static JdbcBatchStatementExecutor<Row> upsertRow(JdbcDmlOptions opt, RuntimeContext ctx) {
		checkNotNull(opt.getKeyFields());

		int[] pkFields = Arrays.stream(opt.getKeyFields()).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
		int[] pkTypes = opt.getFieldTypes() == null ? null : Arrays.stream(pkFields).map(f -> opt.getFieldTypes()[f]).toArray();

		return opt.getDialect()
				.getUpsertStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields())
				.map(sql -> keyedRow(pkFields, opt.getFieldTypes(), sql))
				.orElseGet(() ->
						new InsertOrUpdateJdbcExecutor<>(
								opt.getDialect().getRowExistsStatement(opt.getTableName(), opt.getKeyFields()),
								opt.getDialect().getInsertIntoStatement(opt.getTableName(), opt.getFieldNames()),
								opt.getDialect().getUpdateStatement(opt.getTableName(), opt.getFieldNames(), opt.getKeyFields()),
								ParameterSetter.forRow(pkTypes),
								ParameterSetter.forRow(opt.getFieldTypes()),
								ParameterSetter.forRow(opt.getFieldTypes()),
								rowKeyExtractor(pkFields),
								ctx.getExecutionConfig().isObjectReuseEnabled() ? Row::copy : Function.identity()));
	}

	static Function<Row, Row> rowKeyExtractor(int[] pkFields) {
		return row -> getPrimaryKey(row, pkFields);
	}

	static JdbcBatchStatementExecutor<Row> keyedRow(int[] pkFields, int[] pkTypes, String sql) {
		return keyed(sql,
				rowKeyExtractor(pkFields),
				(st, record) -> setRecordToStatement(st, pkTypes, rowKeyExtractor(pkFields).apply(record)));
	}

	static <T, K> JdbcBatchStatementExecutor<T> keyed(String sql, Function<T, K> keyExtractor, ParameterSetter<K> parameterSetter) {
		return new KeyedBatchStatementExecutor<>(sql, keyExtractor, parameterSetter);
	}

	static JdbcBatchStatementExecutor<Row> simpleRow(String sql, int[] fieldTypes, boolean objectReuse) {
		return simple(sql, ParameterSetter.forRow(fieldTypes), objectReuse ? Row::copy : Function.identity());
	}

	static <T, V> JdbcBatchStatementExecutor<T> simple(String sql, ParameterSetter<V> paramSetter, Function<T, V> valueTransformer) {
		return new SimpleBatchStatementExecutor<>(sql, paramSetter, valueTransformer);
	}

}
