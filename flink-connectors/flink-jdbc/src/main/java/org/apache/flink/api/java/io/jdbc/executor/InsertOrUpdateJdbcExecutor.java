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

import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

final class InsertOrUpdateJdbcExecutor implements JdbcBatchStatementExecutor<Row> {

	private final String existSQL;
	private final String insertSQL;
	private final String updateSQL;
	private final int[] pkTypes;
	private final int[] pkFields;
	private final boolean objectReuse;

	private transient PreparedStatement existStatement;
	private transient PreparedStatement insertStatement;
	private transient PreparedStatement updateStatement;
	private transient Map<Row, Row> keyToRows = new HashMap<>();
	private final int[] fieldTypes;

	InsertOrUpdateJdbcExecutor(
			int[] fieldTypes,
			int[] pkFields, int[] pkTypes,
			String existSQL,
			String insertSQL,
			String updateSQL, boolean objectReuse) {
		this.pkFields = pkFields;
		this.existSQL = existSQL;
		this.insertSQL = insertSQL;
		this.updateSQL = updateSQL;
		this.fieldTypes = fieldTypes;
		this.pkTypes = pkTypes;
		this.objectReuse = objectReuse;
	}

	@Override
	public void open(Connection connection) throws SQLException {
		keyToRows = new HashMap<>();
		existStatement = connection.prepareStatement(existSQL);
		insertStatement = connection.prepareStatement(insertSQL);
		updateStatement = connection.prepareStatement(updateSQL);
	}

	@Override
	public void process(Row record) {
		keyToRows.put(KeyedBatchStatementExecutor.getPrimaryKey(record, pkFields), objectReuse ? Row.copy(record) : record);
	}

	@Override
	public void executeBatch() throws SQLException {
		if (keyToRows.size() > 0) {
			for (Map.Entry<Row, Row> entry : keyToRows.entrySet()) {
				processOneRowInBatch(entry.getKey(), entry.getValue());
			}
			updateStatement.executeBatch();
			insertStatement.executeBatch();
			keyToRows.clear();
		}
	}

	private void processOneRowInBatch(Row pk, Row row) throws SQLException {
		if (exist(pk)) {
			setRecordToStatement(updateStatement, fieldTypes, row);
			updateStatement.addBatch();
		} else {
			setRecordToStatement(insertStatement, fieldTypes, row);
			insertStatement.addBatch();
		}
	}

	private boolean exist(Row pk) throws SQLException {
		setRecordToStatement(existStatement, pkTypes, pk);
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
