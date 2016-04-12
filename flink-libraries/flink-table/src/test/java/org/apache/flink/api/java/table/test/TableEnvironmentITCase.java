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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.TableException;
import org.apache.flink.api.table.test.utils.TableProgramsTestBase;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class TableEnvironmentITCase extends TableProgramsTestBase {

	public TableEnvironmentITCase(TestExecutionMode mode, TableConfigMode configMode) {
		super(mode, configMode);
	}

	@Test
	public void testSimpleRegister() throws Exception {
		final String tableName = "MyTable";
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet(tableName, ds);
		Table t = tableEnv.scan(tableName);

		Table result = t.select("f0, f1");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
				"8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
				"16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testRegisterWithFields() throws Exception {
		final String tableName = "MyTable";
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet(tableName, ds, "a, b, c");
		Table t = tableEnv.scan(tableName);

		Table result = t.select("a, b, c");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
				"4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
				"7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
				"11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" +
				"14,5,Comment#8\n" + "15,5,Comment#9\n" + "16,6,Comment#10\n" +
				"17,6,Comment#11\n" + "18,6,Comment#12\n" + "19,6,Comment#13\n" +
				"20,6,Comment#14\n" + "21,6,Comment#15\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = TableException.class)
	public void testRegisterExistingDatasetTable() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet("MyTable", ds);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
				CollectionDataSets.getSmall5TupleDataSet(env);
		// Must fail. Name is already used for different table.
		tableEnv.registerDataSet("MyTable", ds2);
	}

	@Test(expected = TableException.class)
	public void testScanUnregisteredTable() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail. No table registered under that name.
		tableEnv.scan("nonRegisteredTable");
	}

	@Test
	public void testTableRegister() throws Exception {
		final String tableName = "MyTable";
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		Table t = tableEnv.fromDataSet(ds);
		tableEnv.registerTable(tableName, t);
		Table result = tableEnv.scan(tableName).select("f0, f1").filter("f0 > 7");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" +
				"13,5\n" + "14,5\n" + "15,5\n" +
				"16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = TableException.class)
	public void testIllegalName() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		Table t = tableEnv.fromDataSet(ds);
		// Must fail. Table name matches internal name pattern.
		tableEnv.registerTable("_DataSetTable_42", t);
	}

	@Test(expected = TableException.class)
	public void testRegisterTableFromOtherEnv() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv1 = TableEnvironment.getTableEnvironment(env, config());
		BatchTableEnvironment tableEnv2 = TableEnvironment.getTableEnvironment(env, config());

		Table t = tableEnv1.fromDataSet(CollectionDataSets.get3TupleDataSet(env));
		// Must fail. Table is bound to different TableEnvironment.
		tableEnv2.registerTable("MyTable", t);
	}
}
