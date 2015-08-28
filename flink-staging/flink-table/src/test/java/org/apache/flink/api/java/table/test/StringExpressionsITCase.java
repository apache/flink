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

import org.apache.flink.api.table.ExpressionException;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StringExpressionsITCase extends MultipleProgramsTestBase {


	public StringExpressionsITCase(TestExecutionMode mode) {
		super(mode);
	}

	private String resultPath;
	private String expected = "";

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testSubstring() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple2<String, Integer>> ds = env.fromElements(
				new Tuple2<String, Integer>("AAAA", 2),
				new Tuple2<String, Integer>("BBBB", 1));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
				.select("a.substring(0, b)");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		resultSet.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "AA\nB";
	}

	@Test
	public void testSubstringWithMaxEnd() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple2<String, Integer>> ds = env.fromElements(
				new Tuple2<String, Integer>("ABCD", 2),
				new Tuple2<String, Integer>("ABCD", 1));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
				.select("a.substring(b)");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		resultSet.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "CD\nBCD";
	}

	@Test(expected = ExpressionException.class)
	public void testNonWorkingSubstring1() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple2<String, Float>> ds = env.fromElements(
				new Tuple2<String, Float>("ABCD", 2.0f),
				new Tuple2<String, Float>("ABCD", 1.0f));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
				.select("a.substring(0, b)");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		resultSet.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "";
	}

	@Test(expected = ExpressionException.class)
	public void testNonWorkingSubstring2() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple2<String, String>> ds = env.fromElements(
				new Tuple2<String, String>("ABCD", "a"),
				new Tuple2<String, String>("ABCD", "b"));

		Table in = tableEnv.fromDataSet(ds, "a, b");

		Table result = in
				.select("a.substring(b, 15)");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		resultSet.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "";
	}
}
