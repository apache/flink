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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.io.jdbc.split.NumericBetweenParametersProvider;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.flink.api.java.io.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.api.java.io.jdbc.JdbcTestFixture.ROW_TYPE_INFO;
import static org.hamcrest.core.StringContains.containsString;

/**
 * Tests using both {@link JDBCInputFormat} and {@link JDBCOutputFormat}.
 */
public class JDBCFullTest extends JDBCDataTestBase {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testWithoutParallelism() throws Exception {
		runTest(false);
	}

	@Test
	public void testWithParallelism() throws Exception {
		runTest(true);
	}

	@Test
	public void testEnrichedClassCastException() throws Exception {
		exception.expect(ClassCastException.class);
		exception.expectMessage(containsString("field index: 3, field value: 11.11."));

		JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(getDbMetadata().getDriverClass())
			.setDBUrl(getDbMetadata().getUrl())
			.setQuery("insert into newbooks (id, title, author, price, qty) values (?,?,?,?,?)")
			.setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.INTEGER})
			.finish();

		jdbcOutputFormat.open(1, 1);
		Row inputRow = Row.of(1001, "Java public for dummies", "Tan Ah Teck", "11.11", 11);
		jdbcOutputFormat.writeRecord(inputRow);
		jdbcOutputFormat.close();
	}

	private void runTest(boolean exploitParallelism) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(getDbMetadata().getDriverClass())
				.setDBUrl(getDbMetadata().getUrl())
				.setQuery(JdbcTestFixture.SELECT_ALL_BOOKS)
				.setRowTypeInfo(ROW_TYPE_INFO);

		if (exploitParallelism) {
			final int fetchSize = 1;
			final long min = JdbcTestFixture.TEST_DATA[0].id;
			final long max = JdbcTestFixture.TEST_DATA[JdbcTestFixture.TEST_DATA.length - fetchSize].id;
			//use a "splittable" query to exploit parallelism
			inputBuilder = inputBuilder
					.setQuery(JdbcTestFixture.SELECT_ALL_BOOKS_SPLIT_BY_ID)
					.setParametersProvider(new NumericBetweenParametersProvider(min, max).ofBatchSize(fetchSize));
		}
		DataSet<Row> source = environment.createInput(inputBuilder.finish());

		//NOTE: in this case (with Derby driver) setSqlTypes could be skipped, but
		//some databases don't null values correctly when no column type was specified
		//in PreparedStatement.setObject (see its javadoc for more details)
		source.output(JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(getDbMetadata().getDriverClass())
				.setDBUrl(getDbMetadata().getUrl())
				.setQuery("insert into newbooks (id, title, author, price, qty) values (?,?,?,?,?)")
				.setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.INTEGER})
				.finish());

		environment.execute();

		try (
				Connection dbConn = DriverManager.getConnection(getDbMetadata().getUrl());
				PreparedStatement statement = dbConn.prepareStatement(JdbcTestFixture.SELECT_ALL_NEWBOOKS);
				ResultSet resultSet = statement.executeQuery()
		) {
			int count = 0;
			while (resultSet.next()) {
				count++;
			}
			Assert.assertEquals(JdbcTestFixture.TEST_DATA.length, count);
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(getDbMetadata().getDriverClass());
		try (
				Connection conn = DriverManager.getConnection(getDbMetadata().getUrl());
				Statement stat = conn.createStatement()) {
			stat.execute("DELETE FROM " + OUTPUT_TABLE);

			stat.close();
			conn.close();
		}
	}

}
