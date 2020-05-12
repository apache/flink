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
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link JdbcBatchStatementExecutor} that extracts SQL keys from the supplied stream elements and executes a SQL query for them.
 */
class KeyedBatchStatementExecutor<T, K> implements JdbcBatchStatementExecutor<T> {

	private final String sql;
	private final JdbcStatementBuilder<K> parameterSetter;
	private final Function<T, K> keyExtractor;

	private transient Set<K> batch = new HashSet<>();
	private transient PreparedStatement st;

	/**
	 * Keep in mind object reuse: if it's on then key extractor may be required to return new object.
	 */
	KeyedBatchStatementExecutor(String sql, Function<T, K> keyExtractor, JdbcStatementBuilder<K> statementBuilder) {
		this.parameterSetter = statementBuilder;
		this.keyExtractor = keyExtractor;
		this.sql = sql;
	}

	@Override
	public void open(Connection connection) throws SQLException {
		batch = new HashSet<>();
		st = connection.prepareStatement(sql);
	}

	@Override
	public void addToBatch(T record) {
		batch.add(keyExtractor.apply(record));
	}

	@Override
	public void executeBatch() throws SQLException {
		if (!batch.isEmpty()) {
			for (K entry : batch) {
				parameterSetter.accept(st, entry);
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
	}

}
