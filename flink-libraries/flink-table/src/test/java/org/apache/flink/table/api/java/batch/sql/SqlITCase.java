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

package org.apache.flink.table.api.java.batch.sql;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
<<<<<<< HEAD
<<<<<<< HEAD
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
=======
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
>>>>>>> add the rand functions to FunctionGenerator class, and init random field in constructor
=======
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
>>>>>>> support non-constant field arguments for rand and rand_integer
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.types.Row;
<<<<<<< HEAD

=======
>>>>>>> add the rand functions to FunctionGenerator class, and init random field in constructor
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
<<<<<<< HEAD
import java.util.Map;
=======
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
>>>>>>> add the rand functions to FunctionGenerator class, and init random field in constructor

/**
 * Integration tests for batch SQL.
 */
@RunWith(Parameterized.class)
public class SqlITCase extends TableProgramsCollectionTestBase {

	public SqlITCase(TableConfigMode configMode) {
		super(configMode);
	}

	@Test
	public void testValues() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		String sqlQuery = "VALUES (1, 'Test', TRUE, DATE '1944-02-24', 12.4444444444444445)," +
			"(2, 'Hello', TRUE, DATE '1944-02-24', 12.666666665)," +
			"(3, 'World', FALSE, DATE '1944-12-24', 12.54444445)";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);

		List<Row> results = resultSet.collect();
		String expected = "3,World,false,1944-12-24,12.5444444500000000\n" +
			"2,Hello,true,1944-02-24,12.6666666650000000\n" +
			// Calcite converts to decimals and strings with equal length
			"1,Test ,true,1944-02-24,12.4444444444444445\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testSelectFromTable() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		Table in = tableEnv.fromDataSet(ds, "a,b,c");
		tableEnv.registerTable("T", in);

		String sqlQuery = "SELECT a, c FROM T";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "1,Hi\n" + "2,Hello\n" + "3,Hello world\n" +
			"4,Hello world, how are you?\n" + "5,I am fine.\n" + "6,Luke Skywalker\n" +
			"7,Comment#1\n" + "8,Comment#2\n" + "9,Comment#3\n" + "10,Comment#4\n" +
			"11,Comment#5\n" + "12,Comment#6\n" + "13,Comment#7\n" +
			"14,Comment#8\n" + "15,Comment#9\n" + "16,Comment#10\n" +
			"17,Comment#11\n" + "18,Comment#12\n" + "19,Comment#13\n" +
			"20,Comment#14\n" + "21,Comment#15\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testFilterFromDataSet() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet("DataSetTable", ds, "x, y, z");

		String sqlQuery = "SELECT x FROM DataSetTable WHERE z LIKE '%Hello%'";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "2\n" + "3\n" + "4";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet("AggTable", ds, "x, y, z");

		String sqlQuery = "SELECT sum(x), min(x), max(x), count(y), avg(x) FROM AggTable";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "231,1,21,21,11";
		compareResultAsText(results, expected);
	}

	@Test
	public void testJoin() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		tableEnv.registerDataSet("t1", ds1, "a, b, c");
		tableEnv.registerDataSet("t2", ds2, "d, e, f, g, h");

		String sqlQuery = "SELECT c, g FROM t1, t2 WHERE b = e";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n";
		compareResultAsText(results, expected);
	}

	@Test
<<<<<<< HEAD
	public void testMap() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		List<Tuple2<Integer, Map<String, String>>> rows = new ArrayList<>();
		rows.add(new Tuple2<>(1, Collections.singletonMap("foo", "bar")));
		rows.add(new Tuple2<>(2, Collections.singletonMap("foo", "spam")));

		TypeInformation<Tuple2<Integer, Map<String, String>>> ty = new TupleTypeInfo<>(
			BasicTypeInfo.INT_TYPE_INFO,
			new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

		DataSet<Tuple2<Integer, Map<String, String>>> ds1 = env.fromCollection(rows, ty);
		tableEnv.registerDataSet("t1", ds1, "a, b");

		String sqlQuery = "SELECT b['foo'] FROM t1";
=======
	public void testRandAndRandInteger() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		String sqlQuery = "SELECT i, RAND() AS r1, RAND(1) AS r2, " +
			"RAND_INTEGER(10) AS r3, RAND_INTEGER(3, 10) AS r4 " +
			"FROM (VALUES 1, 2, 3, 4, 5) AS t(i)";
>>>>>>> add the rand functions to FunctionGenerator class, and init random field in constructor
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
<<<<<<< HEAD
		String expected = "bar\n" + "spam\n";
		compareResultAsText(results, expected);
=======
		Random expectedRandom1 = new Random(1);
		Random expectedRandom2 = new Random(3);
		assertEquals(5, results.size());
		for (int i = 0; i < 5; ++i) {
			Row row = results.get(i);
			assertEquals(i + 1, row.getField(0));
			double r1 = (double) row.getField(1);
			double r2 = (double) row.getField(2);
			int r3 = (int) row.getField(3);
			int r4 = (int) row.getField(4);

			assertTrue(0 <= r1 && r1 < 1);
			assertEquals("" + expectedRandom1.nextDouble(), "" + r2);
			assertTrue(0 <= r3 && r3 < 10);
			assertEquals("" + expectedRandom2.nextInt(10), "" + r4);
		}
<<<<<<< HEAD
>>>>>>> add the rand functions to FunctionGenerator class, and init random field in constructor
=======
	}

	@Test
	public void testRandAndRandIntegerWithField() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
		tableEnv.registerDataSet("t", ds, "a, b, c");

		String sqlQuery = "SELECT a, b, RAND(a) AS r1, " +
			"RAND_INTEGER(a) AS r2, RAND_INTEGER(a, 10) AS r3, RAND_INTEGER(4, a) AS r4, " +
			"RAND_INTEGER(a, CAST(b AS INT)) AS r5 FROM t ORDER BY a";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		List<Integer> aValues = Lists.newArrayList(1, 2, 3);
		List<Long> bValues = Lists.newArrayList(1L, 2L, 2L);
		Random expectedRandom4 = new Random(4);
		assertEquals(3, results.size());
		for (int i = 0; i < 3; ++i) {
			Row row = results.get(i);
			int a = aValues.get(i);
			Long b = bValues.get(i);
			assertEquals(a, row.getField(0));
			assertEquals(b, row.getField(1));
			double expectedR1 = new Random(a).nextDouble();
			double r1 = (double) row.getField(2);
			int r2 = (int) row.getField(3);
			int expectedR3 = new Random(a).nextInt(10);
			int r3 = (int) row.getField(4);
			int expectedR4 = expectedRandom4.nextInt(a);
			int r4 = (int) row.getField(5);
			int expectedR5 = new Random(a).nextInt(b.intValue());
			int r5 = (int) row.getField(6);

			assertEquals("" + expectedR1, "" + r1);
			assertTrue(0 <= r2 && r2 < a);
			assertEquals(expectedR3, r3);
			assertEquals(expectedR4, r4);
			assertEquals(expectedR5, r5);
		}
	}

	@Test
	public void testMap() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		List<Tuple2<Integer, Map<String, String>>> rows = new ArrayList<>();
		rows.add(new Tuple2<>(1, Collections.singletonMap("foo", "bar")));
		rows.add(new Tuple2<>(2, Collections.singletonMap("foo", "spam")));

		TypeInformation<Tuple2<Integer, Map<String, String>>> ty = new TupleTypeInfo<>(
			BasicTypeInfo.INT_TYPE_INFO,
			new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

		DataSet<Tuple2<Integer, Map<String, String>>> ds1 = env.fromCollection(rows, ty);
		tableEnv.registerDataSet("t1", ds1, "a, b");

		String sqlQuery = "SELECT b['foo'] FROM t1";
		Table result = tableEnv.sql(sqlQuery);

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "bar\n" + "spam\n";
		compareResultAsText(results, expected);
>>>>>>> support non-constant field arguments for rand and rand_integer
	}
}
