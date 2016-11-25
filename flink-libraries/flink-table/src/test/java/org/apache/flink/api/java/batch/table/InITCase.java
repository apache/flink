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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.ValidationException;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class InITCase extends TableProgramsTestBase {

	public InITCase(TestExecutionMode mode, TableConfigMode configMode) {
		super(mode, configMode);
	}

	@Test
	public void testInWithNumericLiterals() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> in1 = CollectionDataSets.get3TupleDataSet(env);
		Table ds1 = tableEnv.fromDataSet(in1, "a, b, c");
		Table simpleIn = ds1.select("a, b, c").where("a.in(1, 3, 7)");
		DataSet<Row> ds = tableEnv.toDataSet(simpleIn, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1,Hi\n" + "3,2,Hello world\n" + "7,4,Comment#1\n";
		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testInWithManyLiterals() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> in1 = CollectionDataSets.get3TupleDataSet(env);
		Table ds1 = tableEnv.fromDataSet(in1, "a, b, c");
		Table simpleIn = ds1.select("a, b, c").where("c.in(\"Hi\", \"Hello\", \"Hello world\", \"Hello world, how are you?\", \"I am fine.\", \"Luke Skywalker\", \"Comment#1\", \"Comment#2\", \"Comment#3\", \"Comment#4\", \"Comment#5\", \"Comment#6\", \"Comment#7\", \"Comment#8\", \"Comment#9\", \"Comment#10\", \"Comment#11\", \"Comment#12\", \"Comment#13\", \"Comment#14\")");
		DataSet<Row> ds = tableEnv.toDataSet(simpleIn, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1,Hi\n" +
			"2,2,Hello\n" +
			"3,2,Hello world\n" +
			"4,3,Hello world, how are you?\n" +
			"5,3,I am fine.\n" +
			"6,3,Luke Skywalker\n" +
			"7,4,Comment#1\n" +
			"8,4,Comment#2\n" +
			"9,4,Comment#3\n" +
			"10,4,Comment#4\n" +
			"11,5,Comment#5\n" +
			"12,5,Comment#6\n" +
			"13,5,Comment#7\n" +
			"14,5,Comment#8\n" +
			"15,5,Comment#9\n" +
			"16,6,Comment#10\n" +
			"17,6,Comment#11\n" +
			"18,6,Comment#12\n" +
			"19,6,Comment#13\n" +
			"20,6,Comment#14\n";
		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testInWithStringLiterals() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		DataSet<Tuple3<Integer, Long, String>> in1 = CollectionDataSets.get3TupleDataSet(env);

		Table ds1 = tableEnv.fromDataSet(in1, "a, b, c");

		Table simpleIn = ds1.select("a, b, c").where("c.in(\"Hi\", \"Hello world\", \"Comment#1\")");
		DataSet<Row> ds = tableEnv.toDataSet(simpleIn, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1,Hi\n" + "3,2,Hello world\n" + "7,4,Comment#1\n";
		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Ignore
	@Test
	public void testInWithBigDecimalLiterals() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		DataSet<Tuple3<Integer, Long, String>> in1 = CollectionDataSets.get3TupleDataSet(env);

		Table ds1 = tableEnv.fromDataSet(in1, "a, b, c");

		Table simpleIn = ds1.select("a, b, c").where("a.in(BigDecimal(1), BigDecimal(2), BigDecimal(3.01))");
		DataSet<Row> ds = tableEnv.toDataSet(simpleIn, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1,Hi\n" + "2,2,Hello\n";
		TestBaseUtils.compareResultAsText(results, expected);
  	}
	
	@Ignore
	@Test(expected = ValidationException.class)
	public void testInWithNullOperand() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		DataSet<Tuple3<Integer, Long, String>> in1 = CollectionDataSets.get3TupleDataSet(env);
		Table ds1 = tableEnv.fromDataSet(in1, "a, b, c");

		Table simpleIn = ds1.select("a, b, c").where("c.in(\"Hi\", \"Hello world\", \"Comment#1\", \"null\")");
		DataSet<Row> ds = tableEnv.toDataSet(simpleIn, Row.class);
		ds.collect();
  	}

	@Test(expected = ValidationException.class)
	public void testInWithDifferentTypeRightOperands() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		DataSet<Tuple3<Integer, Long, String>> in1 = CollectionDataSets.get3TupleDataSet(env);
		Table ds1 = tableEnv.fromDataSet(in1, "a, b, c");

		Table simpleIn = ds1.select("a, b, c").where("c.in(\"Hi\", \"Hello world\", \"Comment#1\", 1)");
		DataSet<Row> ds = tableEnv.toDataSet(simpleIn, Row.class);
		ds.collect();
  	}

	@Test(expected = ValidationException.class)
	public void testInWithDifferentTypeOperands() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		DataSet<Tuple3<Integer, Long, String>> in1 = CollectionDataSets.get3TupleDataSet(env);
		Table ds1 = tableEnv.fromDataSet(in1, "a, b, c");

		Table simpleIn = ds1.select("a, b, c").where("a.in(\"Hi\", \"Hello world\", \"Comment#1\")");
		DataSet<Row> ds = tableEnv.toDataSet(simpleIn, Row.class);
		ds.collect();
  	}
}
