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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple7;
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
public class CastingITCase extends MultipleProgramsTestBase {


	public CastingITCase(TestExecutionMode mode){
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
	public void testAutoCastToString() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple7<Byte, Short, Integer, Long, Float, Double, String>> input =
				env.fromElements(new Tuple7<Byte, Short, Integer, Long, Float, Double, String>(
						(byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d, "Hello"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result = table.select(
				"f0 + 'b', f1 + 's', f2 + 'i', f3 + 'L', f4 + 'f', f5 + \"d\"");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "1b,1s,1i,1L,1.0f,1.0d";
	}

	@Test
	public void testNumericAutocastInArithmetic() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple7<Byte, Short, Integer, Long, Float, Double, String>> input =
				env.fromElements(new Tuple7<Byte, Short, Integer, Long, Float, Double, String>(
						(byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d, "Hello"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result = table.select("f0 + 1, f1 +" +
				" 1, f2 + 1L, f3 + 1.0f, f4 + 1.0d, f5 + 1");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "2,2,2,2.0,2.0,2.0";
	}

	@Test
	public void testNumericAutocastInComparison() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple7<Byte, Short, Integer, Long, Float, Double, String>> input =
				env.fromElements(
						new Tuple7<Byte, Short, Integer, Long, Float, Double, String>((byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d, "Hello"),
						new Tuple7<Byte, Short, Integer, Long, Float, Double, String>((byte) 2, (short) 2, 2, 2L, 2.0f, 2.0d, "Hello"));

		Table table =
				tableEnv.fromDataSet(input, "a,b,c,d,e,f,g");

		Table result = table
				.filter("a > 1 && b > 1 && c > 1L && d > 1.0f && e > 1.0d && f > 1");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "2,2,2,2,2.0,2.0,Hello";
	}

	@Test
	public void testCastFromString() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple3<String, String, String>> input =
				env.fromElements(new Tuple3<String, String, String>("1", "true", "2.0"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result = table.select(
				"f0.cast(BYTE), f0.cast(SHORT), f0.cast(INT), f0.cast(LONG), f2.cast(DOUBLE), f2.cast(FLOAT), f1.cast(BOOL)");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "1,1,1,1,2.0,2.0,true\n";
	}
}

