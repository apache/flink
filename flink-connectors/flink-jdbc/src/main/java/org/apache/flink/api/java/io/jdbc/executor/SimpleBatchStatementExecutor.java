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
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

class SimpleBatchStatementExecutor implements JdbcBatchStatementExecutor<Row> {

	private final String sql;
	private final int[] paramTypes;

	private transient PreparedStatement st;
	private List<Row> batch;
	private final boolean objectReuse;

	SimpleBatchStatementExecutor(String sql, int[] paramTypes, boolean objectReuse) {
		this.sql = sql;
		this.paramTypes = paramTypes;
		this.objectReuse = objectReuse;
	}

	@Override
	public void open(Connection connection) throws SQLException {
		this.batch = new ArrayList<>();
		this.st = connection.prepareStatement(sql);
	}

	@Override
	public void process(Row record) {
		batch.add(objectReuse ? Row.copy(record) : record);
	}

	@Override
	public void executeBatch() throws SQLException {
		if (!batch.isEmpty()) {
			for (Row r : batch) {
				setRecordToStatement(st, paramTypes, r);
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
