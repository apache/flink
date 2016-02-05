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

import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.NotImplementedError;

import java.util.List;


@RunWith(Parameterized.class)
public class JoinITCase extends MultipleProgramsTestBase {

	public JoinITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test(expected = NotImplementedError.class)
	public void testJoin() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table result = in1.join(in2).where("b === e").select("c, g");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = NotImplementedError.class)
	public void testJoinWithFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table result = in1.join(in2).where("b === e && b < 2").select("c, g");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "Hi,Hallo\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = NotImplementedError.class)
	public void testJoinWithMultipleKeys() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table result = in1.join(in2).where("a === d && b === h").select("c, g");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
				"Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoinNonExistingKey() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table result = in1.join(in2).where("foo === e").select("c, g");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	// Calcite does not eagerly check the compatibility of compared types
	@Ignore
	@Test(expected = IllegalArgumentException.class)
	public void testJoinWithNonMatchingKeyTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table result = in1
				.join(in2).where("a === g").select("c, g");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoinWithAmbiguousFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, c");

		Table result = in1
				.join(in2).where("a === d").select("c, g");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "";
		compareResultAsText(results, expected);
	}

	@Test(expected = NotImplementedError.class)
	public void testJoinWithAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table result = in1
				.join(in2).where("a === d").select("g.count");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "6";
		compareResultAsText(results, expected);
	}

}
