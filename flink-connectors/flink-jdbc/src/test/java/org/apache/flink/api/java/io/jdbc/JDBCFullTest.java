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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.apache.flink.api.java.io.jdbc.JDBCUpsertOutputFormatTest.check;
import static org.hamcrest.core.StringContains.containsString;

/**
 * Tests using both {@link JDBCInputFormat} and {@link JDBCOutputFormat}.
 */
public class JDBCFullTest extends JDBCTestBase {

	@Test
	public void testInputOutputFormatWithoutParallelism() throws Exception {
		runInputOutputFormatTest(false);
	}

	@Test
	public void testInputOutputFormatWithParallelism() throws Exception {
		runInputOutputFormatTest(true);
	}

	@Test
	public void testSourceSinkWithoutParallelism() throws Exception {
		runSourceSinkTest(false);
	}

	@Test
	public void testSourceSinkWithParallelism() throws Exception {
		runSourceSinkTest(true);
	}

	@Test
	public void testEnrichedClassCastException() throws Exception {
		exception.expect(ClassCastException.class);
		exception.expectMessage(containsString("field index: 3, field value: 11.11."));

		JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(JDBCTestBase.DRIVER_CLASS)
			.setDBUrl(JDBCTestBase.DB_URL)
			.setQuery("insert into newbooks (" +
				"id, title, author, price, qty, print_date, print_time, print_timestamp) values (?,?,?,?,?,?,?,?)")
			.setSqlTypes(SQL_TYPES)
			.finish();

		jdbcOutputFormat.open(1, 1);
		Row inputRow = Row.of(
			1001, "Java public for dummies", "Tan Ah Teck", "11.11", 11,
			Date.valueOf("2011-01-11"), Time.valueOf("01:11:11"), Timestamp.valueOf("2011-01-11 01:11:11"));
		jdbcOutputFormat.writeRecord(inputRow);
		jdbcOutputFormat.close();
	}

	private void runInputOutputFormatTest(boolean exploitParallelism) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCTestBase.DRIVER_CLASS)
				.setDBUrl(JDBCTestBase.DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS)
				.setRowTypeInfo(ROW_TYPE_INFO);

		if (exploitParallelism) {
			final int fetchSize = 1;
			final long min = JDBCTestBase.TEST_DATA[0].id;
			final long max = JDBCTestBase.TEST_DATA[JDBCTestBase.TEST_DATA.length - fetchSize].id;
			//use a "splittable" query to exploit parallelism
			inputBuilder = inputBuilder
					.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_ID)
					.setParametersProvider(new NumericBetweenParametersProvider(min, max).ofBatchSize(fetchSize));
		}
		DataSet<Row> source = environment.createInput(inputBuilder.finish());

		//NOTE: in this case (with Derby driver) setSqlTypes could be skipped, but
		//some databases don't null values correctly when no column type was specified
		//in PreparedStatement.setObject (see its javadoc for more details)
		source.output(JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(JDBCTestBase.DRIVER_CLASS)
				.setDBUrl(JDBCTestBase.DB_URL)
				.setQuery("insert into newbooks (" +
					"id, title, author, price, qty, print_date, print_time, print_timestamp) values (?,?,?,?,?,?,?,?)")
				.setSqlTypes(SQL_TYPES)
				.finish());

		environment.execute();

		try (
			Connection dbConn = DriverManager.getConnection(JDBCTestBase.DB_URL);
			PreparedStatement statement = dbConn.prepareStatement(JDBCTestBase.SELECT_ALL_NEWBOOKS);
			ResultSet resultSet = statement.executeQuery()
		) {
			int count = 0;
			while (resultSet.next()) {
				count++;
			}
			Assert.assertEquals(JDBCTestBase.TEST_DATA.length, count);
		}
	}

	private void runSourceSinkTest(boolean exploitParallelism) throws Exception {
		TableSchema schema = TableSchema.builder()
			.field("id", DataTypes.INT())
			.field("title", DataTypes.STRING())
			.field("author", DataTypes.STRING())
			.field("price", DataTypes.DOUBLE())
			.field("qty", DataTypes.INT())
			.field("print_date", DataTypes.DATE())
			.field("print_time", DataTypes.TIME())
			.field("print_timestamp", DataTypes.TIMESTAMP())
			.build();

		JDBCTableSource.Builder sourceBuilder = JDBCTableSource.builder()
			.setOptions(JDBCOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(INPUT_TABLE)
				.build())
			.setSchema(schema);
		if (exploitParallelism) {
			sourceBuilder.setReadOptions(JDBCReadOptions.builder()
				.setPartitionColumnName("id")
				.setPartitionLowerBound(TEST_DATA[0].id)
				.setPartitionUpperBound(TEST_DATA[TEST_DATA.length - 1].id)
				.setNumPartitions(3)
				.build());
		}
		JDBCTableSource source = sourceBuilder.build();

		JDBCUpsertTableSink.Builder sinkBuilder = JDBCUpsertTableSink.builder()
			.setOptions(JDBCOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(OUTPUT_TABLE)
				.build())
			.setTableSchema(schema);
		JDBCUpsertTableSink sink = sinkBuilder.build();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		tEnv.registerTableSource(INPUT_TABLE, source);
		tEnv.registerTableSink(OUTPUT_TABLE, sink);
		tEnv.sqlUpdate(
			"INSERT INTO " + OUTPUT_TABLE + " " +
				"SELECT id, title, author, price, qty, " +
				"print_date + interval '1' day, " +
				"print_time + interval '1' hour, " +
				"print_timestamp + interval '1' day + interval '1' hour " +
				"FROM " + INPUT_TABLE);
		env.execute();

		check(
			Arrays.stream(TEST_DATA).map(entry -> {
				Row row = new Row(8);
				row.setField(0, entry.id);
				row.setField(1, entry.title);
				row.setField(2, entry.author);
				row.setField(3, entry.price);
				row.setField(4, entry.qty);
				row.setField(5,
					entry.printDate == null ? null : Date.valueOf(entry.printDate.toLocalDate().plusDays(1)));
				row.setField(6,
					entry.printTime == null ? null : Time.valueOf(entry.printTime.toLocalTime().plusHours(1)));
				row.setField(7,
					entry.printTimestamp == null ?
						null : Timestamp.valueOf(entry.printTimestamp.toLocalDateTime().plusDays(1).plusHours(1)));
				return row;
			}).toArray(Row[]::new),
			DB_URL, OUTPUT_TABLE, FIELD_NAMES);
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL);
			Statement stat = conn.createStatement()) {
			stat.execute("DELETE FROM " + OUTPUT_TABLE);

			stat.close();
			conn.close();
		}
	}

}
