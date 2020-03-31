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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.java.io.jdbc.JDBCTestBase.DRIVER_CLASS;
import static org.apache.flink.api.java.io.jdbc.JDBCUpsertOutputFormatTest.check;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;

/**
 * IT case for {@link JDBCUpsertTableSink}.
 */
public class JDBCUpsertTableSinkITCase extends AbstractTestBase {

	public static final String DB_URL = "jdbc:derby:memory:upsert";
	public static final String OUTPUT_TABLE1 = "upsertSink";
	public static final String OUTPUT_TABLE2 = "appendSink";

	@Before
	public void before() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", JDBCTestBase.class.getCanonicalName() + ".DEV_NULL");

		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement()) {
			stat.executeUpdate("CREATE TABLE " + OUTPUT_TABLE1 + " (" +
					"cnt BIGINT NOT NULL DEFAULT 0," +
					"lencnt BIGINT NOT NULL DEFAULT 0," +
					"cTag INT NOT NULL DEFAULT 0," +
					"PRIMARY KEY (cnt, cTag))");

			stat.executeUpdate("CREATE TABLE " + OUTPUT_TABLE2 + " (" +
					"id INT NOT NULL DEFAULT 0," +
					"num BIGINT NOT NULL DEFAULT 0)");
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL);
			Statement stat = conn.createStatement()) {
			stat.execute("DROP TABLE " + OUTPUT_TABLE1);
			stat.execute("DROP TABLE " + OUTPUT_TABLE2);
		}
	}

	public static DataStream<Tuple3<Integer, Long, String>> get3TupleDataStream(StreamExecutionEnvironment env) {
		List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
		data.add(new Tuple3<>(1, 1L, "Hi"));
		data.add(new Tuple3<>(2, 2L, "Hello"));
		data.add(new Tuple3<>(3, 2L, "Hello world"));
		data.add(new Tuple3<>(4, 3L, "Hello world, how are you?"));
		data.add(new Tuple3<>(5, 3L, "I am fine."));
		data.add(new Tuple3<>(6, 3L, "Luke Skywalker"));
		data.add(new Tuple3<>(7, 4L, "Comment#1"));
		data.add(new Tuple3<>(8, 4L, "Comment#2"));
		data.add(new Tuple3<>(9, 4L, "Comment#3"));
		data.add(new Tuple3<>(10, 4L, "Comment#4"));
		data.add(new Tuple3<>(11, 5L, "Comment#5"));
		data.add(new Tuple3<>(12, 5L, "Comment#6"));
		data.add(new Tuple3<>(13, 5L, "Comment#7"));
		data.add(new Tuple3<>(14, 5L, "Comment#8"));
		data.add(new Tuple3<>(15, 5L, "Comment#9"));
		data.add(new Tuple3<>(16, 6L, "Comment#10"));
		data.add(new Tuple3<>(17, 6L, "Comment#11"));
		data.add(new Tuple3<>(18, 6L, "Comment#12"));
		data.add(new Tuple3<>(19, 6L, "Comment#13"));
		data.add(new Tuple3<>(20, 6L, "Comment#14"));
		data.add(new Tuple3<>(21, 6L, "Comment#15"));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}

	@Test
	public void testUpsert() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		Table t = tEnv.fromDataStream(get3TupleDataStream(env).assignTimestampsAndWatermarks(
				new AscendingTimestampExtractor<Tuple3<Integer, Long, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple3<Integer, Long, String> element) {
						return element.f0;
					}}), "id, num, text");

		tEnv.registerTable("T", t);

		String[] fields = {"cnt", "lencnt", "cTag"};
		tEnv.registerTableSink("upsertSink", JDBCUpsertTableSink.builder()
				.setOptions(JDBCOptions.builder()
						.setDBUrl(DB_URL)
						.setTableName(OUTPUT_TABLE1)
						.build())
				.setTableSchema(TableSchema.builder().fields(
						fields, new DataType[] {BIGINT(), BIGINT(), INT()}).build())
				.build());

		tEnv.sqlUpdate("INSERT INTO upsertSink SELECT cnt, COUNT(len) AS lencnt, cTag FROM" +
				" (SELECT len, COUNT(id) as cnt, cTag FROM" +
				" (SELECT id, CHAR_LENGTH(text) AS len, (CASE WHEN id > 0 THEN 1 ELSE 0 END) cTag FROM T)" +
				" GROUP BY len, cTag)" +
				" GROUP BY cnt, cTag");
		env.execute();
		check(new Row[] {
				Row.of(1, 5, 1),
				Row.of(7, 1, 1),
				Row.of(9, 1, 1)
		}, DB_URL, OUTPUT_TABLE1, fields);
	}

	@Test
	public void testAppend() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		Table t = tEnv.fromDataStream(get3TupleDataStream(env), "id, num, text");

		tEnv.registerTable("T", t);

		String[] fields = {"id", "num"};
		tEnv.registerTableSink("upsertSink", JDBCUpsertTableSink.builder()
				.setOptions(JDBCOptions.builder()
						.setDBUrl(DB_URL)
						.setTableName(OUTPUT_TABLE2)
						.build())
				.setTableSchema(TableSchema.builder().fields(
						fields, new DataType[] {INT(), BIGINT()}).build())
				.build());

		tEnv.sqlUpdate("INSERT INTO upsertSink SELECT id, num FROM T WHERE id IN (2, 10, 20)");
		env.execute();
		check(new Row[] {
				Row.of(2, 2),
				Row.of(10, 4),
				Row.of(20, 6)
		}, DB_URL, OUTPUT_TABLE2, fields);
	}
}
