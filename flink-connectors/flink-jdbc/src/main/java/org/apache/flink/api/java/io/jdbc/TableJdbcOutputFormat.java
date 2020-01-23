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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.executor.JdbcBatchStatementExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

class TableJdbcOutputFormat extends JdbcBatchingOutputFormat<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> {
	private static final Logger LOG = LoggerFactory.getLogger(TableJdbcOutputFormat.class);

	private JdbcBatchStatementExecutor<Row> deleteExecutor;

	TableJdbcOutputFormat(JdbcConnectionOptions connectionOptions, JdbcDmlOptions dmlOptions, JdbcBatchOptions batchOptions) {
		super(connectionOptions, dmlOptions, batchOptions);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		deleteExecutor = createDeleteExecutor();
		try {
			deleteExecutor.open(connection);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private JdbcBatchStatementExecutor<Row> createDeleteExecutor() {
		int[] pkFields = Arrays.stream(dmlOptions.getFieldNames()).mapToInt(Arrays.asList(dmlOptions.getFieldNames())::indexOf).toArray();
		int[] pkTypes = dmlOptions.getFieldTypes() == null ? null :
				Arrays.stream(pkFields).map(f -> dmlOptions.getFieldTypes()[f]).toArray();
		String deleteSql = dmlOptions.getDialect().getDeleteStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames());
		return JdbcBatchStatementExecutor.keyedRow(pkFields, pkTypes, deleteSql);
	}

	@Override
	JdbcBatchStatementExecutor<Row> createStatementRunner(JDBCDialect dialect) {
		return JdbcBatchStatementExecutor.upsertRow(
				dialect, dmlOptions.getTableName(), dmlOptions.getFieldNames(), dmlOptions.getFieldTypes(), dmlOptions.getKeyFields(),
				getRuntimeContext().getExecutionConfig().isObjectReuseEnabled());
	}

	@Override
	void doWriteRecord(Tuple2<Boolean, Row> record) throws SQLException {
		if (record.f0) {
			super.doWriteRecord(record);
		} else {
			deleteExecutor.process(extractJdbcRecord(record));
		}
	}

	@Override
	Row extractJdbcRecord(Tuple2<Boolean, Row> record) {
		return record.f1;
	}

	@Override
	public synchronized void close() {
		try {
			super.close();
		} finally {
			try {
				deleteExecutor.close();
			} catch (SQLException e) {
				LOG.warn("unable to close delete statement runner", e);
			}
		}
	}

	@Override
	void attemptFlush() throws SQLException {
		super.attemptFlush();
		deleteExecutor.executeBatch();
	}
}
