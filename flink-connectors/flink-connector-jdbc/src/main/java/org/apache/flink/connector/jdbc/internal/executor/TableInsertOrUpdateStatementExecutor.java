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

package org.apache.flink.connector.jdbc.internal.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.statement.StatementFactory;
import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link JdbcBatchStatementExecutor} that provides upsert semantics by updating row
 * if it exists and inserting otherwise. Only used in Table/SQL API.
 */
@Internal
public final class TableInsertOrUpdateStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

	private final StatementFactory existStmtFactory;
	private final StatementFactory insertStmtFactory;
	private final StatementFactory updateStmtFactory;

	private final JdbcRowConverter existSetter;
	private final JdbcRowConverter insertSetter;
	private final JdbcRowConverter updateSetter;

	private final Function<RowData, RowData> keyExtractor;

	private transient FieldNamedPreparedStatement existStatement;
	private transient FieldNamedPreparedStatement insertStatement;
	private transient FieldNamedPreparedStatement updateStatement;

	public TableInsertOrUpdateStatementExecutor(
			StatementFactory existStmtFactory,
			StatementFactory insertStmtFactory,
			StatementFactory updateStmtFactory,
			JdbcRowConverter existSetter,
			JdbcRowConverter insertSetter,
			JdbcRowConverter updateSetter,
			Function<RowData, RowData> keyExtractor) {
		this.existStmtFactory = checkNotNull(existStmtFactory);
		this.insertStmtFactory = checkNotNull(insertStmtFactory);
		this.updateStmtFactory = checkNotNull(updateStmtFactory);
		this.existSetter = checkNotNull(existSetter);
		this.insertSetter = checkNotNull(insertSetter);
		this.updateSetter = checkNotNull(updateSetter);
		this.keyExtractor = keyExtractor;
	}

	@Override
	public void prepareStatements(Connection connection) throws SQLException {
		existStatement = existStmtFactory.createStatement(connection);
		insertStatement = insertStmtFactory.createStatement(connection);
		updateStatement = updateStmtFactory.createStatement(connection);
	}

	@Override
	public void addToBatch(RowData record) throws SQLException {
		processOneRowInBatch(keyExtractor.apply(record), record);
	}

	private void processOneRowInBatch(RowData pk, RowData row) throws SQLException {
		if (exist(pk)) {
			updateSetter.toExternal(row, updateStatement);
			updateStatement.addBatch();
		} else {
			insertSetter.toExternal(row, insertStatement);
			insertStatement.addBatch();
		}
	}

	private boolean exist(RowData pk) throws SQLException {
		existSetter.toExternal(pk, existStatement);
		try (ResultSet resultSet = existStatement.executeQuery()) {
			return resultSet.next();
		}
	}

	@Override
	public void executeBatch() throws SQLException {
		updateStatement.executeBatch();
		insertStatement.executeBatch();
	}

	@Override
	public void closeStatements() throws SQLException {
		for (FieldNamedPreparedStatement s : Arrays.asList(existStatement, insertStatement, updateStatement)) {
			if (s != null) {
				s.close();
			}
		}
	}
}
