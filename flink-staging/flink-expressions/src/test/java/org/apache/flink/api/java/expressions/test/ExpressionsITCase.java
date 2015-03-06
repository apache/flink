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

package org.apache.flink.api.java.expressions.test;

import org.apache.flink.api.expressions.ExpressionException;
import org.apache.flink.api.expressions.ExpressionOperation;
import org.apache.flink.api.expressions.Row;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.expressions.ExpressionUtil;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.scala.expressions.JavaBatchTranslator;
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
public class ExpressionsITCase extends MultipleProgramsTestBase {


	public ExpressionsITCase(TestExecutionMode mode){
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
	public void testArithmetic() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<Integer, Integer>> input =
				env.fromElements(new Tuple2<Integer, Integer>(5, 10));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input, "a, b");

		ExpressionOperation<JavaBatchTranslator> result = expressionOperation.select(
				"a - 5, a + 5, a / 2, a * 2, a % 2, -a");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "0,10,2,10,1,-5";
	}

	@Test
	public void testLogic() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<Integer, Boolean>> input =
				env.fromElements(new Tuple2<Integer, Boolean>(5, true));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input, "a, b");

		ExpressionOperation<JavaBatchTranslator> result = expressionOperation.select(
				"b && true, b && false, b || false, !b");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "true,false,true,false";
	}

	@Test
	public void testComparisons() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple3<Integer, Integer, Integer>> input =
				env.fromElements(new Tuple3<Integer, Integer, Integer>(5, 5, 4));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input, "a, b, c");

		ExpressionOperation<JavaBatchTranslator> result = expressionOperation.select(
				"a > c, a >= b, a < c, a.isNull, a.isNotNull");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "true,true,false,false,true";
	}

	@Test
	public void testBitwiseOperation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<Byte, Byte>> input =
				env.fromElements(new Tuple2<Byte, Byte>((byte) 3, (byte) 5));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input, "a, b");

		ExpressionOperation<JavaBatchTranslator> result = expressionOperation.select(
				"a & b, a | b, a ^ b, ~a");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "1,7,6,-4";
	}

	@Test
	public void testBitwiseWithAutocast() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<Integer, Byte>> input =
				env.fromElements(new Tuple2<Integer, Byte>(3, (byte) 5));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input, "a, b");

		ExpressionOperation<JavaBatchTranslator> result = expressionOperation.select(
				"a & b, a | b, a ^ b, ~a");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "1,7,6,-4";
	}

	@Test(expected = ExpressionException.class)
	public void testBitwiseWithNonWorkingAutocast() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<Float, Byte>> input =
				env.fromElements(new Tuple2<Float, Byte>(3.0f, (byte) 5));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input, "a, b");

		ExpressionOperation<JavaBatchTranslator> result =
				expressionOperation.select("a & b, a | b, a ^ b, ~a");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "";
	}
}

