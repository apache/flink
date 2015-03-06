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

import org.apache.flink.api.expressions.ExpressionException;
import org.apache.flink.api.expressions.ExpressionOperation;
import org.apache.flink.api.expressions.Row;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.expressions.ExpressionUtil;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.scala.expressions.JavaBatchTranslator;
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
public class AggregationsITCase extends MultipleProgramsTestBase {


	public AggregationsITCase(TestExecutionMode mode){
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
	public void testAggregationTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(CollectionDataSets.get3TupleDataSet(env));

		ExpressionOperation<JavaBatchTranslator> result =
				expressionOperation.select("f0.sum, f0.min, f0.max, f0.count, f0.avg");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "231,1,21,21,11";
	}

	@Test(expected = ExpressionException.class)
	public void testAggregationOnNonExistingField() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(CollectionDataSets.get3TupleDataSet(env));

		ExpressionOperation<JavaBatchTranslator> result =
				expressionOperation.select("'foo.avg");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "";
	}

	@Test
	public void testWorkingAggregationDataTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple7<Byte, Short, Integer, Long, Float, Double, String>> input =
				env.fromElements(
						new Tuple7<Byte, Short, Integer, Long, Float, Double, String>((byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d, "Hello"),
						new Tuple7<Byte, Short, Integer, Long, Float, Double, String>((byte) 2, (short) 2, 2, 2L, 2.0f, 2.0d, "Ciao"));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input);

		ExpressionOperation<JavaBatchTranslator> result =
				expressionOperation.select("f0.avg, f1.avg, f2.avg, f3.avg, f4.avg, f5.avg, f6.count");

		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "1,1,1,1,1.5,1.5,2";
	}

	@Test
	public void testAggregationWithArithmetic() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<Float, String>> input =
				env.fromElements(
						new Tuple2<Float, String>(1f, "Hello"),
						new Tuple2<Float, String>(2f, "Ciao"));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input);

		ExpressionOperation<JavaBatchTranslator> result =
				expressionOperation.select("(f0 + 2).avg + 2, f1.count + \" THE COUNT\"");


		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "5.5,2 THE COUNT";
	}

	@Test(expected = ExpressionException.class)
	public void testNonWorkingDataTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<Float, String>> input = env.fromElements(new Tuple2<Float, String>(1f,
				"Hello"));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input);

		ExpressionOperation<JavaBatchTranslator> result =
				expressionOperation.select("f1.sum");


		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "";
	}

	@Test(expected = ExpressionException.class)
	public void testNoNestedAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple2<Float, String>> input = env.fromElements(new Tuple2<Float, String>(1f, "Hello"));

		ExpressionOperation<JavaBatchTranslator> expressionOperation =
				ExpressionUtil.from(input);

		ExpressionOperation<JavaBatchTranslator> result =
				expressionOperation.select("f0.sum.sum");


		DataSet<Row> ds = ExpressionUtil.toSet(result, Row.class);
		ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();

		expected = "";
	}

}

