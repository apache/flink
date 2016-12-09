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
package org.apache.flink.table.api.java.batch.table;

import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.examples.java.WordCountTable.WC;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AggregationsITCase extends TableProgramsTestBase {

	public AggregationsITCase(TestExecutionMode mode, TableConfigMode configMode){
		super(mode, configMode);
	}

	@Test(expected = ValidationException.class)
	public void testAggregationOnNonExistingField() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		Table table =
				tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env));

		Table result =
				table.select("foo.avg");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = ValidationException.class)
	public void testNonWorkingDataTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSource<Tuple2<Float, String>> input = env.fromElements(new Tuple2<>(1f, "Hello"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result =
				// Must fail. Cannot compute SUM aggregate on String field.
				table.select("f1.sum");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = ValidationException.class)
	public void testNoNestedAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSource<Tuple2<Float, String>> input = env.fromElements(new Tuple2<>(1f, "Hello"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result =
				// Must fail. Aggregation on aggregation not allowed.
				table.select("f0.sum.sum");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = ValidationException.class)
	public void testGroupingOnNonExistentField() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		tableEnv
			.fromDataSet(input, "a, b, c")
			// must fail. Field foo is not in input
			.groupBy("foo")
			.select("a.avg");
	}

	@Test(expected = ValidationException.class)
	public void testGroupingInvalidSelection() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		tableEnv
			.fromDataSet(input, "a, b, c")
			.groupBy("a, b")
			// must fail. Field c is not a grouping key or aggregation
			.select("c");
	}

	@Test
	public void testGroupNoAggregation() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);
		Table table = tableEnv.fromDataSet(input, "a, b, c");

		Table result = table
			.groupBy("b").select("a.sum as d, b").groupBy("b, d").select("b");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		String expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n";
		List<Row> results = ds.collect();
		compareResultAsText(results, expected);
	}

	@Test
	public void testPojoAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());
		DataSet<WC> input = env.fromElements(
				new WC("Hello", 1),
				new WC("Ciao", 1),
				new WC("Hello", 1),
				new WC("Hola", 1),
				new WC("Hola", 1));

		Table table = tableEnv.fromDataSet(input);

		Table filtered = table
				.groupBy("word")
				.select("word.count as frequency, word")
				.filter("frequency = 2");

		List<String> result = tableEnv.toDataSet(filtered, WC.class)
				.map(new MapFunction<WC, String>() {
					public String map(WC value) throws Exception {
						return value.word;
					}
				}).collect();
		String expected = "Hello\n" + "Hola";
		compareResultAsText(result, expected);
	}

	@Test
	public void testPojoGrouping() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<String, Double, String>> data = env.fromElements(
			new Tuple3<>("A", 23.0, "Z"),
			new Tuple3<>("A", 24.0, "Y"),
			new Tuple3<>("B", 1.0, "Z"));

		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		Table table = tableEnv
			.fromDataSet(data, "groupMe, value, name")
			.select("groupMe, value, name")
			.where("groupMe != 'B'");

		DataSet<MyPojo> myPojos = tableEnv.toDataSet(table, MyPojo.class);

		DataSet<MyPojo> result = myPojos.groupBy("groupMe")
			.sortGroup("value", Order.DESCENDING)
			.first(1);

		List<MyPojo> resultList = result.collect();
		compareResultAsText(resultList, "A,24.0,Y");
	}

	// --------------------------------------------------------------------------------------------

	public static class MyPojo implements Serializable {
		private static final long serialVersionUID = 8741918940120107213L;

		public String groupMe;
		public double value;
		public String name;

		public MyPojo() {
			// for serialization
		}

		public MyPojo(String groupMe, double value, String name) {
			this.groupMe = groupMe;
			this.value = value;
			this.name = name;
		}

		@Override
		public String toString() {
			return groupMe + "," + value + "," + name;
		}
	}
}

