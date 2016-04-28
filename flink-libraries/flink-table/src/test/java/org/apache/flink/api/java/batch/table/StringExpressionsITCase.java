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

package org.apache.flink.api.java.batch.table;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.codegen.CodeGenException;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class StringExpressionsITCase extends MultipleProgramsTestBase {

	public StringExpressionsITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testSubstring() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<String, Integer>> ds = env.fromElements(
				new Tuple2<>("AAAA", 2),
				new Tuple2<>("BBBB", 1));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
				.select("a.substring(1, b)");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "AA\nB";
		compareResultAsText(results, expected);
	}

	@Test
	public void testSubstringWithMaxEnd() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<String, Integer>> ds = env.fromElements(
				new Tuple2<>("ABCD", 3),
				new Tuple2<>("ABCD", 2));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
				.select("a.substring(b)");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "CD\nBCD";
		compareResultAsText(results, expected);
	}

	@Test(expected = CodeGenException.class)
	public void testNonWorkingSubstring1() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<String, Float>> ds = env.fromElements(
				new Tuple2<>("ABCD", 2.0f),
				new Tuple2<>("ABCD", 1.0f));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
			// Must fail. Second parameter of substring must be an Integer not a Double.
			.select("a.substring(0, b)");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		resultSet.collect();
	}

	@Test(expected = CodeGenException.class)
	public void testNonWorkingSubstring2() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<String, String>> ds = env.fromElements(
				new Tuple2<>("ABCD", "a"),
				new Tuple2<>("ABCD", "b"));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
			// Must fail. First parameter of substring must be an Integer not a String.
			.select("a.substring(b, 15)");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		resultSet.collect();
	}

	@Test(expected = CodeGenException.class)
	public void testGeneratedCodeForStringComparison() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		DataSet<Tuple3<Integer, Long, String>> tupleDataSet = CollectionDataSets.get3TupleDataSet(env);
		Table in = tableEnv.fromDataSet(tupleDataSet, "a, b, c");
		// Must fail because the comparison here is between Integer(column 'a') and (String 'Fred')
		Table res = in.filter("a = 'Fred'" );
		DataSet<Row> resultSet = tableEnv.toDataSet(res, Row.class);
	}

	@Test(expected = CodeGenException.class)
	public void testGeneratedCodeForIntegerEqualsComparison() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		DataSet<Tuple3<Integer, Long, String>> tupleDataSet = CollectionDataSets.get3TupleDataSet(env);
		Table in = tableEnv.fromDataSet(tupleDataSet, "a, b, c");
		// Must fail because the comparison here is between String(column 'c') and (Integer 10)
		Table res = in.filter("c = 10" );
		DataSet<Row> resultSet = tableEnv.toDataSet(res, Row.class);
	}

	@Test(expected = CodeGenException.class)
	public void testGeneratedCodeForIntegerGreaterComparison() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		DataSet<Tuple3<Integer, Long, String>> tupleDataSet = CollectionDataSets.get3TupleDataSet(env);
		Table in = tableEnv.fromDataSet(tupleDataSet, "a, b, c");
		// Must fail because the comparison here is between String(column 'c') and (Integer 10)
		Table res = in.filter("c > 10");
		DataSet<Row> resultSet = tableEnv.toDataSet(res, Row.class);
	}

	@Test
	public void testStringConcat() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<String, Integer>> ds = env.fromElements(
			new Tuple2<>("ABCD", 3),
			new Tuple2<>("ABCD", 2));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
			.select("a + b + 42");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "ABCD342\nABCD242";
		compareResultAsText(results, expected);
	}

	@Test
	public void testStringConcat1() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<String, Integer>> ds = env.fromElements(
			new Tuple2<>("ABCD", 3),
			new Tuple2<>("ABCD", 2));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
			.select("42 + b + a");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "44ABCD\n45ABCD";
		compareResultAsText(results, expected);
	}
}
