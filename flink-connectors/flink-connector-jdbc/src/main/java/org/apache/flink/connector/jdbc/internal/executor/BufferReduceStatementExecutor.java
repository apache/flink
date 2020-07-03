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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Currently, this statement executor is only used for table/sql to buffer insert/update/delete
 * events, and reduce them in buffer before submit to external database.
 */
public class BufferReduceStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

	private final JdbcBatchStatementExecutor<RowData> upsertExecutor;
	private final JdbcBatchStatementExecutor<RowData> deleteExecutor;
	private final Function<RowData, RowData> keyExtractor;
	private final Function<RowData, RowData> valueTransform;
	// the mapping is [KEY, <+/-, VALUE>]
	private final Map<RowData, Tuple2<Boolean, RowData>> reduceBuffer = new HashMap<>();

	public BufferReduceStatementExecutor(
			JdbcBatchStatementExecutor<RowData> upsertExecutor,
			JdbcBatchStatementExecutor<RowData> deleteExecutor,
			Function<RowData, RowData> keyExtractor,
			Function<RowData, RowData> valueTransform) {
		this.upsertExecutor = upsertExecutor;
		this.deleteExecutor = deleteExecutor;
		this.keyExtractor = keyExtractor;
		this.valueTransform = valueTransform;
	}

	@Override
	public void prepareStatements(Connection connection) throws SQLException {
		upsertExecutor.prepareStatements(connection);
		deleteExecutor.prepareStatements(connection);
	}

	@Override
	public void addToBatch(RowData record) throws SQLException {
		RowData key = keyExtractor.apply(record);
		boolean flag = changeFlag(record.getRowKind());
		RowData value = valueTransform.apply(record); // copy or not
		reduceBuffer.put(key, Tuple2.of(flag, value));
	}

	/**
	 * Returns true if the row kind is INSERT or UPDATE_AFTER,
	 * returns false if the row kind is DELETE or UPDATE_BEFORE.
	 */
	private boolean changeFlag(RowKind rowKind) {
		switch (rowKind) {
			case INSERT:
			case UPDATE_AFTER:
				return true;
			case DELETE:
			case UPDATE_BEFORE:
				return false;
			default:
				throw new UnsupportedOperationException(
					String.format("Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER," +
						" DELETE, but get: %s.", rowKind));
		}
	}

	@Override
	public void executeBatch() throws SQLException {
		// TODO: reuse the extracted key and avoid value copy in the future
		for (Tuple2<Boolean, RowData> tuple : reduceBuffer.values()) {
			if (tuple.f0) {
				upsertExecutor.addToBatch(tuple.f1);
			} else {
				deleteExecutor.addToBatch(tuple.f1);
			}
		}
		upsertExecutor.executeBatch();
		deleteExecutor.executeBatch();
		reduceBuffer.clear();
	}

	@Override
	public void closeStatements() throws SQLException {
		upsertExecutor.closeStatements();
		deleteExecutor.closeStatements();
	}
}
