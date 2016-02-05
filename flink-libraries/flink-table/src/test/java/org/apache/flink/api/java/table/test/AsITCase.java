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

import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.codegen.CodeGenException;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class AsITCase extends MultipleProgramsTestBase {

	public AsITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testAsFromTuple() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		Table table =
				tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b, c");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
			"4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
			"7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
			"11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" +
			"14,5,Comment#8\n" + "15,5,Comment#9\n" + "16,6,Comment#10\n" +
			"17,6,Comment#11\n" + "18,6,Comment#12\n" + "19,6,Comment#13\n" +
			"20,6,Comment#14\n" + "21,6,Comment#15\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = CodeGenException.class)
	public void testAsFromPojo() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		List<SmallPojo> data = new ArrayList<>();
		data.add(new SmallPojo("Peter", 28, 4000.00, "Sales"));
		data.add(new SmallPojo("Anna", 56, 10000.00, "Engineering"));
		data.add(new SmallPojo("Lucy", 42, 6000.00, "HR"));

		Table table =
			tableEnv.fromDataSet(env.fromCollection(data),
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected =
			"Sales,28,4000.0,Peter\n" +
			"Engineering,56,10000.0,Anna\n" +
			"HR,42,6000.0,Lucy\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAsWithToFewFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		Table table =
				tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAsWithToManyFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		Table table =
				tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b, c, d");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAsWithAmbiguousFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		Table table =
				tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b, b");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAsWithNonFieldReference1() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		Table table =
				tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a + 1, b, c");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAsWithNonFieldReference2() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		Table table =
				tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a as foo, b," +
						" c");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	public static class SmallPojo {

		public SmallPojo() { }

		public SmallPojo(String name, int age, double salary, String department) {
			this.name = name;
			this.age = age;
			this.salary = salary;
			this.department = department;
		}

		public String name;
		public int age;
		public double salary;
		public String department;
	}

}

