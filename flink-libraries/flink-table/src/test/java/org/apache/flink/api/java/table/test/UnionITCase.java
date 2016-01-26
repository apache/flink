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
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.table.ExpressionException;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.NotImplementedError;

import java.util.List;

@RunWith(Parameterized.class)
public class UnionITCase extends MultipleProgramsTestBase {


	public UnionITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test(expected = NotImplementedError.class)
	public void testUnion() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "a, b, c");

		Table selected = in1.unionAll(in2).select("c");

		DataSet<Row> ds = tableEnv.toDataSet(selected, Row.class);
		List<Row> results = ds.collect();
		String expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hi\n" + "Hello\n" + "Hello world\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = NotImplementedError.class)
	public void testUnionWithFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "a, b, d, c, e").select("a, b, c");

		Table selected = in1.unionAll(in2).where("b < 2").select("c");

		DataSet<Row> ds = tableEnv.toDataSet(selected, Row.class);
		List<Row> results = ds.collect();
		String expected = "Hi\n" + "Hallo\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnionFieldsNameNotOverlap1() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table selected = in1.unionAll(in2);

		DataSet<Row> ds = tableEnv.toDataSet(selected, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnionFieldsNameNotOverlap2() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "a, b, c, d, e").select("a, b, c");

		Table selected = in1.unionAll(in2);

		DataSet<Row> ds = tableEnv.toDataSet(selected, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = NotImplementedError.class)
	public void testUnionWithAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "a, b, d, c, e").select("a, b, c");

		Table selected = in1.unionAll(in2).select("c.count");

		DataSet<Row> ds = tableEnv.toDataSet(selected, Row.class);
		List<Row> results = ds.collect();
		String expected = "18";
		compareResultAsText(results, expected);
	}

}
