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

package org.apache.flink.api.java.table.test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;

import java.io.File;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;

public class SqlExplainTest extends MultipleProgramsTestBase {

	public SqlExplainTest() {
		super(TestExecutionMode.CLUSTER);
	}

	private static String testFilePath = SqlExplainTest.class.getResource("/").getFile();

	@Test
	public void testFilterWithoutExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<Integer, String>> input = env.fromElements(new Tuple2<>(1,"d"));
		Table table = tableEnv
			.fromDataSet(input, "a, b")
			.filter("a % 2 = 0");

		String result = tableEnv.explain(table);
		try (Scanner scanner = new Scanner(new File(testFilePath +
			"../../src/test/scala/resources/testFilter0.out"))){
			String source = scanner.useDelimiter("\\A").next();
			assertEquals(source, result);
		}
	}

	@Test
	public void testFilterWithExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<Integer, String>> input = env.fromElements(new Tuple2<>(1,"d"));
		Table table = tableEnv
			.fromDataSet(input, "a, b")
			.filter("a % 2 = 0");

		String result = tableEnv.explain(table, true);
		try (Scanner scanner = new Scanner(new File(testFilePath +
			"../../src/test/scala/resources/testFilter1.out"))){
			String source = scanner.useDelimiter("\\A").next();
			assertEquals(source, result);
		}
	}

	@Test
	public void testJoinWithoutExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<Integer, String>> input1 = env.fromElements(new Tuple2<>(1,"d"));
		DataSet<Tuple2<Integer, String>> input2 = env.fromElements(new Tuple2<>(1,"d"));
		Table table1 = tableEnv.fromDataSet(input1, "a, b");
		Table table2 = tableEnv.fromDataSet(input2, "c, d");
		Table table = table1
			.join(table2)
			.where("b = d")
			.select("a, c");

		String result = tableEnv.explain(table);
		try (Scanner scanner = new Scanner(new File(testFilePath +
			"../../src/test/scala/resources/testJoin0.out"))){
			String source = scanner.useDelimiter("\\A").next();
			assertEquals(source, result);
		}
	}

	@Test
	public void testJoinWithExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<Integer, String>> input1 = env.fromElements(new Tuple2<>(1,"d"));
		DataSet<Tuple2<Integer, String>> input2 = env.fromElements(new Tuple2<>(1,"d"));
		Table table1 = tableEnv.fromDataSet(input1, "a, b");
		Table table2 = tableEnv.fromDataSet(input2, "c, d");
		Table table = table1
			.join(table2)
			.where("b = d")
			.select("a, c");

		String result = tableEnv.explain(table, true);
		try (Scanner scanner = new Scanner(new File(testFilePath +
			"../../src/test/scala/resources/testJoin1.out"))){
			String source = scanner.useDelimiter("\\A").next();
			assertEquals(source, result);
		}
	}

	@Test
	public void testUnionWithoutExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<Integer, String>> input1 = env.fromElements(new Tuple2<>(1,"d"));
		DataSet<Tuple2<Integer, String>> input2 = env.fromElements(new Tuple2<>(1,"d"));
		Table table1 = tableEnv.fromDataSet(input1, "count, word");
		Table table2 = tableEnv.fromDataSet(input2, "count, word");
		Table table = table1.unionAll(table2);

		String result = tableEnv.explain(table);
		try (Scanner scanner = new Scanner(new File(testFilePath +
			"../../src/test/scala/resources/testUnion0.out"))){
			String source = scanner.useDelimiter("\\A").next();
			assertEquals(source, result);
		}
	}

	@Test
	public void testUnionWithExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple2<Integer, String>> input1 = env.fromElements(new Tuple2<>(1,"d"));
		DataSet<Tuple2<Integer, String>> input2 = env.fromElements(new Tuple2<>(1,"d"));
		Table table1 = tableEnv.fromDataSet(input1, "count, word");
		Table table2 = tableEnv.fromDataSet(input2, "count, word");
		Table table = table1.unionAll(table2);

		String result = tableEnv.explain(table, true);
		try (Scanner scanner = new Scanner(new File(testFilePath +
			"../../src/test/scala/resources/testUnion1.out"))){
			String source = scanner.useDelimiter("\\A").next();
			assertEquals(source, result);
		}
	}
}
