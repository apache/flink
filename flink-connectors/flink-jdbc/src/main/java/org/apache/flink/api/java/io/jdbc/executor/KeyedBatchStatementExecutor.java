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

import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

/**
 * Upsert writer to deal with upsert, delete message.
 */
class KeyedBatchStatementExecutor implements JdbcBatchStatementExecutor<Row> {

	private final int[] pkTypes;
	private final int[] pkFields;
	private final String sql;
	private final boolean objectReuse;

	private transient Map<Row, Row> keyToRows = new HashMap<>();
	private transient PreparedStatement st;

	KeyedBatchStatementExecutor(int[] pkFields, int[] pkTypes, String sql, boolean objectReuse) {
		this.pkFields = pkFields;
		this.pkTypes = pkTypes;
		this.sql = sql;
		this.objectReuse = objectReuse;
	}

	@Override
	public void open(Connection connection) throws SQLException {
		keyToRows = new HashMap<>();
		st = connection.prepareStatement(sql);
	}

	@Override
	public void process(Row record) {
		// we don't need perform a deep copy, because jdbc field are immutable object.
		Row row = objectReuse ? Row.copy(record) : record;
		// add records to buffer
		keyToRows.put(getPrimaryKey(row), row);
	}

	@Override
	public void executeBatch() throws SQLException {
		if (keyToRows.size() > 0) {
			for (Map.Entry<Row, Row> entry : keyToRows.entrySet()) {
				setRecordToStatement(st, pkTypes, entry.getKey());
				st.addBatch();
			}
			st.executeBatch();
			keyToRows.clear();
		}
	}

	@Override
	public void close() throws SQLException {
		if (st != null) {
			st.close();
			st = null;
		}
	}

	private Row getPrimaryKey(Row row) {
		int[] pkFields = this.pkFields;
		return getPrimaryKey(row, pkFields);
	}

	static Row getPrimaryKey(Row row, int[] pkFields) {
		Row pkRow = new Row(pkFields.length);
		for (int i = 0; i < pkFields.length; i++) {
			pkRow.setField(i, row.getField(pkFields[i]));
		}
		return pkRow;
	}
}
