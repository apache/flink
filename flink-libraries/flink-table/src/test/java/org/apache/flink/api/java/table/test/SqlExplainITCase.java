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
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.plan.TranslationContext;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;

public class SqlExplainITCase extends MultipleProgramsTestBase {

	public SqlExplainITCase() {
		super(TestExecutionMode.CLUSTER);
	}

	private static String testFilePath = SqlExplainITCase.class.getResource("/").getFile();

	public static class WC {
		public String word;
		public int count;

		// Public constructor to make it a Flink POJO
		public WC() {}

		public WC(int count, String word) {
			this.word = word;
			this.count = count;
		}
	}

	@Before
	public void resetContext() {
		TranslationContext.reset();
	}

	@Test
	public void testFilterWithoutExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<WC> input = env.fromElements(
				new WC(1,"d"),
				new WC(2,"d"),
				new WC(3,"d"));

		Table table = tableEnv.fromDataSet(input, "count as a, word as b");

		String result = table
				.filter("a % 2 = 0")
				.explain();
		String source = new Scanner(new File(testFilePath +
				"../../src/test/scala/resources/testFilter0.out"))
				.useDelimiter("\\A").next();
		assertEquals(source, result);
	}

	@Test
	public void testFilterWithExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<WC> input = env.fromElements(
				new WC(1, "d"),
				new WC(2, "d"),
				new WC(3, "d"));

		Table table = tableEnv.fromDataSet(input, "count as a, word as b");

		String result = table
				.filter("a % 2 = 0")
				.explain(true);
		String source = new Scanner(new File(testFilePath +
				"../../src/test/scala/resources/testFilter1.out"))
				.useDelimiter("\\A").next();
		assertEquals(source, result);
	}

	@Test
	public void testJoinWithoutExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<WC> input1 = env.fromElements(
				new WC(1, "d"),
				new WC(1, "d"),
				new WC(1, "d"));

		Table table1 = tableEnv.fromDataSet(input1, "count as a, word as b");

		DataSet<WC> input2 = env.fromElements(
				new WC(1,"d"),
				new WC(1,"d"),
				new WC(1,"d"));

		Table table2 = tableEnv.fromDataSet(input2, "count as c, word as d");

		String result = table1
				.join(table2)
				.where("b = d")
				.select("a, c")
				.explain();
		String source = new Scanner(new File(testFilePath +
				"../../src/test/scala/resources/testJoin0.out"))
				.useDelimiter("\\A").next();
		assertEquals(source, result);
	}

	@Test
	public void testJoinWithExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<WC> input1 = env.fromElements(
				new WC(1, "d"),
				new WC(1, "d"),
				new WC(1, "d"));

		Table table1 = tableEnv.fromDataSet(input1, "count as a, word as b");

		DataSet<WC> input2 = env.fromElements(
				new WC(1, "d"),
				new WC(1, "d"),
				new WC(1, "d"));

		Table table2 = tableEnv.fromDataSet(input2, "count as c, word as d");

		String result = table1
				.join(table2)
				.where("b = d")
				.select("a, c")
				.explain(true);
		String source = new Scanner(new File(testFilePath +
				"../../src/test/scala/resources/testJoin1.out"))
				.useDelimiter("\\A").next();
		assertEquals(source, result);
	}

	@Test
	public void testUnionWithoutExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<WC> input1 = env.fromElements(
				new WC(1, "d"),
				new WC(1, "d"),
				new WC(1, "d"));

		Table table1 = tableEnv.fromDataSet(input1);

		DataSet<WC> input2 = env.fromElements(
				new WC(1, "d"),
				new WC(1, "d"),
				new WC(1, "d"));

		Table table2 = tableEnv.fromDataSet(input2);

		String result = table1
				.unionAll(table2)
				.explain();
		String source = new Scanner(new File(testFilePath +
				"../../src/test/scala/resources/testUnion0.out"))
				.useDelimiter("\\A").next();
		assertEquals(source, result);
	}

	@Test
	public void testUnionWithExtended() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<WC> input1 = env.fromElements(
				new WC(1, "d"),
				new WC(1, "d"),
				new WC(1, "d"));

		Table table1 = tableEnv.fromDataSet(input1);

		DataSet<WC> input2 = env.fromElements(
				new WC(1, "d"),
				new WC(1, "d"),
				new WC(1, "d"));

		Table table2 = tableEnv.fromDataSet(input2);

		String result = table1
				.unionAll(table2)
				.explain(true);
		String source = new Scanner(new File(testFilePath +
				"../../src/test/scala/resources/testUnion1.out"))
				.useDelimiter("\\A").next();
		assertEquals(source, result);
	}
}
