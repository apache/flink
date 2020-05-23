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

package org.apache.flink.connector.jdbc.internal.executor;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A {@link JdbcBatchStatementExecutor} that executes supplied statement for given the records (without any pre-processing).
 */
class SimpleBatchStatementExecutor<T, V> implements JdbcBatchStatementExecutor<T> {

	private final String sql;
	private final JdbcStatementBuilder<V> parameterSetter;
	private final Function<T, V> valueTransformer;

	private transient PreparedStatement st;
	private transient List<V> batch;

	SimpleBatchStatementExecutor(String sql, JdbcStatementBuilder<V> statementBuilder, Function<T, V> valueTransformer) {
		this.sql = sql;
		this.parameterSetter = statementBuilder;
		this.valueTransformer = valueTransformer;
	}

	@Override
	public void open(Connection connection) throws SQLException {
		this.batch = new ArrayList<>();
		this.st = connection.prepareStatement(sql);
	}

	@Override
	public void addToBatch(T record) {
		batch.add(valueTransformer.apply(record));
	}

	@Override
	public void executeBatch() throws SQLException {
		if (!batch.isEmpty()) {
			for (V r : batch) {
				parameterSetter.accept(st, r);
				st.addBatch();
			}
			st.executeBatch();
			batch.clear();
		}
	}

	@Override
	public void close() throws SQLException {
		if (st != null) {
			st.close();
			st = null;
		}
		if (batch != null) {
			batch.clear();
		}
	}
}
