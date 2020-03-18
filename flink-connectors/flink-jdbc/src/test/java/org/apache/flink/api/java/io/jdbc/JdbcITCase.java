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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.io.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import org.apache.flink.api.java.io.jdbc.JdbcTestFixture.TestEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.java.io.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.api.java.io.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.api.java.io.jdbc.JdbcTestFixture.TEST_DATA;
import static org.junit.Assert.assertEquals;

/**
 * Smoke tests for the {@link JdbcSink} and the underlying classes.
 */
public class JdbcITCase extends JDBCTestBase {

	@Test
	@Ignore
	public void testInsert() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.setParallelism(1);
		env
			.fromElements(TEST_DATA)
			.addSink(JdbcSink.sink(
				String.format(INSERT_TEMPLATE, INPUT_TABLE),
				(ps, t) -> {
					ps.setInt(1, t.id);
					ps.setString(2, t.title);
					ps.setString(3, t.author);
					if (t.price == null) {
						ps.setNull(4, Types.DOUBLE);
					} else {
						ps.setDouble(4, t.price);
					}
					ps.setInt(5, t.qty);
				},
				new JdbcConnectionOptionsBuilder()
					.withUrl(getDbMetadata().getUrl())
					.withDriverName(getDbMetadata().getDriverClass())
					.build()));
		env.execute();

		assertEquals(Arrays.asList(TEST_DATA), selectBooks());
	}

	private List<TestEntry> selectBooks() throws SQLException {
		List<TestEntry> result = new ArrayList<>();
		try (Connection connection = DriverManager.getConnection(getDbMetadata().getUrl())) {
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			connection.setReadOnly(true);
			try (Statement st = connection.createStatement()) {
				try (ResultSet rs = st.executeQuery("select id, title, author, price, qty from " + INPUT_TABLE)) {
					while (rs.next()) {
						result.add(new TestEntry(
								rs.getInt(1),
								rs.getString(2),
								rs.getString(3),
								rs.getDouble(4),
								rs.getInt(5)
						));
					}
				}
			}
		}
		return result;
	}

	@Override
	protected DbMetadata getDbMetadata() {
		return JdbcTestFixture.DERBY_EBOOKSHOP_DB;
	}
}
