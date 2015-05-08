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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.table.JavaBatchTranslator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class FilterITCase extends MultipleProgramsTestBase {


	public FilterITCase(TestExecutionMode mode){
		super(mode);
	}

	private String resultPath;
	private String expected = "";

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testAllRejectingFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		Table table =
				tableEnv.toTable(input, "a, b, c");

		Table result = table
				.filter("false");

		DataSet<Row> ds = tableEnv.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "\n";
	}

	@Test
	public void testAllPassingFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		Table table =
				tableEnv.toTable(input, "a, b, c");

		Table result = table
				.filter("true");

		DataSet<Row> ds = tableEnv.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
				"how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
				"Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
				"Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
				"Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
				"6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n";
	}

	@Test
	public void testFilterOnIntegerTupleField() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		Table table =
				tableEnv.toTable(input, "a, b, c");

		Table result = table
				.filter(" a % 2 = 0 ");

		DataSet<Row> ds = tableEnv.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "2,2,Hello\n" + "4,3,Hello world, how are you?\n" + "6,3,Luke Skywalker\n" + "8,4," +
				"Comment#2\n" + "10,4,Comment#4\n" + "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
				"Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n";
	}

	@Test
	public void testNotEquals() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		Table table =
				tableEnv.toTable(input, "a, b, c");

		Table result = table
				.filter("!( a % 2 <> 0 ) ");

		DataSet<Row> ds = tableEnv.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "2,2,Hello\n" + "4,3,Hello world, how are you?\n" + "6,3,Luke Skywalker\n" + "8,4," +
				"Comment#2\n" + "10,4,Comment#4\n" + "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
				"Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n";
	}

	@Test
	public void testIntegerBiggerThan128() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input = env.fromElements(new Tuple3<Integer, Long, String>(300, 1L, "Hello"));

		Table table = tableEnv.toTable(input, "a, b, c");

		Table result = table.filter("a = 300 ");

		DataSet<Row> ds = tableEnv.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "300,1,Hello\n";
	}
}

