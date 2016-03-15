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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.table.ExpressionParserException;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.table.plan.PlanGenException;
import org.apache.flink.examples.java.JavaTableExample;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.flink.examples.java.JavaTableExample.WC;

import java.util.List;

@RunWith(Parameterized.class)
public class AggregationsITCase extends MultipleProgramsTestBase {

	public AggregationsITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testAggregationTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		Table table = tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env));

		Table result = table.select("f0.sum, f0.min, f0.max, f0.count, f0.avg");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "231,1,21,21,11";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAggregationOnNonExistingField() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		Table table =
				tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env));

		Table result =
				table.select("foo.avg");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test
	public void testWorkingAggregationDataTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple7<Byte, Short, Integer, Long, Float, Double, String>> input =
				env.fromElements(
						new Tuple7<>((byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d, "Hello"),
						new Tuple7<>((byte) 2, (short) 2, 2, 2L, 2.0f, 2.0d, "Ciao"));

		Table table = tableEnv.fromDataSet(input);

		Table result =
				table.select("f0.avg, f1.avg, f2.avg, f3.avg, f4.avg, f5.avg, f6.count");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1,1,1,1.5,1.5,2";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAggregationWithArithmetic() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple2<Float, String>> input =
				env.fromElements(
						new Tuple2<>(1f, "Hello"),
						new Tuple2<>(2f, "Ciao"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result =
				table.select("(f0 + 2).avg + 2, f1.count + 5");


		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "5.5,7";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAggregationWithTwoCount() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple2<Float, String>> input =
			env.fromElements(
				new Tuple2<>(1f, "Hello"),
				new Tuple2<>(2f, "Ciao"));

		Table table =
			tableEnv.fromDataSet(input);

		Table result =
			table.select("f0.count, f1.count");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "2,2";
		compareResultAsText(results, expected);
	}

	@Test(expected = PlanGenException.class)
	public void testNonWorkingDataTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple2<Float, String>> input = env.fromElements(new Tuple2<>(1f, "Hello"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result =
				table.select("f1.sum");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = ExpressionParserException.class)
	public void testNoNestedAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple2<Float, String>> input = env.fromElements(new Tuple2<>(1f, "Hello"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result =
				table.select("f0.sum.sum");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test
	public void testPojoAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();
		DataSet<WC> input = env.fromElements(
				new WC("Hello", 1),
				new WC("Ciao", 1),
				new WC("Hello", 1),
				new WC("Hola", 1),
				new WC("Hola", 1));

		Table table = tableEnv.fromDataSet(input);

		Table filtered = table
				.groupBy("word")
				.select("word.count as count, word")
				.filter("count = 2");

		List<String> result = tableEnv.toDataSet(filtered, WC.class)
				.map(new MapFunction<WC, String>() {
					public String map(WC value) throws Exception {
						return value.word;
					}
				}).collect();
		String expected = "Hello\n" + "Hola";
		compareResultAsText(result, expected);
	}
}

