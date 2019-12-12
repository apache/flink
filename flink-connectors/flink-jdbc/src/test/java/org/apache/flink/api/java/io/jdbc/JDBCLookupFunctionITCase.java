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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.api.java.io.jdbc.JDBCTestBase.DRIVER_CLASS;

/**
 * IT case for {@link JDBCLookupFunction}.
 */
@RunWith(Parameterized.class)
public class JDBCLookupFunctionITCase extends AbstractTestBase {

	public static final String DB_URL = "jdbc:derby:memory:lookup";
	public static final String LOOKUP_TABLE = "lookup_table";

	private final boolean useCache;

	public JDBCLookupFunctionITCase(boolean useCache) {
		this.useCache = useCache;
	}

	@Parameterized.Parameters(name = "Table config = {0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@Before
	public void before() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", JDBCTestBase.class.getCanonicalName() + ".DEV_NULL");

		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement()) {
			stat.executeUpdate("CREATE TABLE " + LOOKUP_TABLE + " (" +
					"id1 INT NOT NULL DEFAULT 0," +
					"id2 INT NOT NULL DEFAULT 0," +
					"comment1 VARCHAR(1000)," +
					"comment2 VARCHAR(1000)," +
					"my_date DATE," +
					"my_time TIME," +
					"my_timestamp TIMESTAMP)");

			Object[][] data = new Object[][] {
					new Object[] {
						1, 1, "11-c1-v1", "11-c2-v1",
						null, Time.valueOf("01:11:11"), Timestamp.valueOf("2011-01-11 01:11:11")},
					new Object[] {
						1, 1, "11-c1-v2", "11-c2-v2",
						Date.valueOf("2012-02-12"), null, Timestamp.valueOf("2012-02-12 02:12:12")},
					new Object[] {
						2, 3, null, "23-c2",
						Date.valueOf("2013-03-13"), Time.valueOf("03:13:13"), Timestamp.valueOf("2013-03-13 03:13:13")},
					new Object[] {
						2, 5, "25-c1", "25-c2",
						Date.valueOf("2014-04-14"), Time.valueOf("04:14:14"), Timestamp.valueOf("2014-04-14 04:14:14")},
					new Object[] {
						3, 8, "38-c1", "38-c2",
						Date.valueOf("2015-05-15"), Time.valueOf("05:15:15"), Timestamp.valueOf("2015-05-15 05:15:15")}
			};
			boolean[] surroundedByQuotes = new boolean[] {
				false, false, true, true, true, true, true
			};

			StringBuilder sqlQueryBuilder = new StringBuilder(
					"INSERT INTO " + LOOKUP_TABLE +
						" (id1, id2, comment1, comment2, my_date, my_time, my_timestamp) VALUES ");
			for (int i = 0; i < data.length; i++) {
				sqlQueryBuilder.append("(");
				for (int j = 0; j < data[i].length; j++) {
					if (data[i][j] == null) {
						sqlQueryBuilder.append("null");
					} else {
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
						sqlQueryBuilder.append(data[i][j]);
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
					}
					if (j < data[i].length - 1) {
						sqlQueryBuilder.append(", ");
					}
				}
				sqlQueryBuilder.append(")");
				if (i < data.length - 1) {
					sqlQueryBuilder.append(", ");
				}
			}
			stat.execute(sqlQueryBuilder.toString());
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
				Connection conn = DriverManager.getConnection(DB_URL);
				Statement stat = conn.createStatement()) {
			stat.execute("DROP TABLE " + LOOKUP_TABLE);
		}
	}

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		StreamITCase.clear();

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
					new Tuple2<>(1, 1),
					new Tuple2<>(1, 1),
					new Tuple2<>(2, 3),
					new Tuple2<>(2, 5),
					new Tuple2<>(3, 5),
					new Tuple2<>(3, 8)
				)), "id1, id2");

		tEnv.registerTable("T", t);

		JDBCTableSource.Builder builder = JDBCTableSource.builder()
				.setOptions(JDBCOptions.builder()
						.setDBUrl(DB_URL)
						.setTableName(LOOKUP_TABLE)
						.build())
				.setSchema(TableSchema.builder().fields(
						new String[]{"id1", "id2", "comment1", "comment2", "my_date", "my_time", "my_timestamp"},
						new DataType[]{
							DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(),
							DataTypes.DATE(), DataTypes.TIME(), DataTypes.TIMESTAMP()})
						.build());
		if (useCache) {
			builder.setLookupOptions(JDBCLookupOptions.builder()
					.setCacheMaxSize(1000).setCacheExpireMs(1000 * 1000).build());
		}
		tEnv.registerFunction("jdbcLookup",
				builder.build().getLookupFunction(t.getSchema().getFieldNames()));

		String sqlQuery = "SELECT id1, id2, comment1, comment2, my_date, my_time, my_timestamp FROM T, " +
				"LATERAL TABLE(jdbcLookup(id1, id2)) AS " +
				"S(l_id1, l_id2, comment1, comment2, my_date, my_time, my_timestamp)";
		Table result = tEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1,null,01:11:11,2011-01-11 01:11:11.0");
		expected.add("1,1,11-c1-v1,11-c2-v1,null,01:11:11,2011-01-11 01:11:11.0");
		expected.add("1,1,11-c1-v2,11-c2-v2,2012-02-12,null,2012-02-12 02:12:12.0");
		expected.add("1,1,11-c1-v2,11-c2-v2,2012-02-12,null,2012-02-12 02:12:12.0");
		expected.add("2,3,null,23-c2,2013-03-13,03:13:13,2013-03-13 03:13:13.0");
		expected.add("2,5,25-c1,25-c2,2014-04-14,04:14:14,2014-04-14 04:14:14.0");
		expected.add("3,8,38-c1,38-c2,2015-05-15,05:15:15,2015-05-15 05:15:15.0");

		StreamITCase.compareWithList(expected);
	}
}
