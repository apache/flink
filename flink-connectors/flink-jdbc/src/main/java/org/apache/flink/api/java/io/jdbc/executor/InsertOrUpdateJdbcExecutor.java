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

package org.apache.flink.api.java.io.jdbc.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

final class InsertOrUpdateJdbcExecutor<R, K, V> implements JdbcBatchStatementExecutor<R> {

	private final String existSQL;
	private final String insertSQL;
	private final String updateSQL;

	private final ParameterSetter<K> existSetter;
	private final ParameterSetter<V> insertSetter;
	private final ParameterSetter<V> updateSetter;

	private final Function<R, K> keyExtractor;
	private final Function<R, V> valueMapper;

	private transient PreparedStatement existStatement;
	private transient PreparedStatement insertStatement;
	private transient PreparedStatement updateStatement;
	private transient Map<K, V> batch = new HashMap<>();

	InsertOrUpdateJdbcExecutor(String existSQL,
								String insertSQL,
								String updateSQL,
								ParameterSetter<K> existSetter,
								ParameterSetter<V> insertSetter,
								ParameterSetter<V> updateSetter,
								Function<R, K> keyExtractor,
								Function<R, V> valueExtractor) {
		this.existSQL = existSQL;
		this.insertSQL = insertSQL;
		this.updateSQL = updateSQL;
		this.existSetter = existSetter;
		this.insertSetter = insertSetter;
		this.updateSetter = updateSetter;
		this.keyExtractor = keyExtractor;
		this.valueMapper = valueExtractor;
	}

	@Override
	public void open(Connection connection) throws SQLException {
		batch = new HashMap<>();
		existStatement = connection.prepareStatement(existSQL);
		insertStatement = connection.prepareStatement(insertSQL);
		updateStatement = connection.prepareStatement(updateSQL);
	}

	@Override
	public void process(R record) {
		batch.put(keyExtractor.apply(record), valueMapper.apply(record));
	}

	@Override
	public void executeBatch() throws SQLException {
		if (!batch.isEmpty()) {
			for (Map.Entry<K, V> entry : batch.entrySet()) {
				processOneRowInBatch(entry.getKey(), entry.getValue());
			}
			updateStatement.executeBatch();
			insertStatement.executeBatch();
			batch.clear();
		}
	}

	private void processOneRowInBatch(K pk, V row) throws SQLException {
		if (exist(pk)) {
			updateSetter.accept(updateStatement, row);
			updateStatement.addBatch();
		} else {
			insertSetter.accept(insertStatement, row);
			insertStatement.addBatch();
		}
	}

	private boolean exist(K pk) throws SQLException {
		existSetter.accept(existStatement, pk);
		try (ResultSet resultSet = existStatement.executeQuery()) {
			return resultSet.next();
		}
	}

	@Override
	public void close() throws SQLException {
		for (PreparedStatement s : Arrays.asList(existStatement, insertStatement, updateStatement)) {
			if (s != null) {
				s.close();
			}
		}
	}
}
