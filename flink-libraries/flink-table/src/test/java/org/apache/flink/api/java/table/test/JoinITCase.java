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
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.TableException;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;


@RunWith(Parameterized.class)
public class JoinITCase extends MultipleProgramsTestBase {

	public JoinITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testJoin() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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

	@Test
	public void testJoinWithFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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

	@Test
	public void testJoinWithJoinFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table result = in1.join(in2).where("b === e && a < 6 && h < b").select("c, g");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "Hello world, how are you?,Hallo Welt wie\n" +
				"I am fine.,Hallo Welt wie\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testJoinWithMultipleKeys() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		// Must fail. Field foo does not exist.
		in1.join(in2).where("foo === e").select("c, g");
	}

	@Test(expected = TableException.class)
	public void testJoinWithNonMatchingKeyTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		Table result = in1.join(in2)
			// Must fail. Types of join fields are not compatible (Integer and String)
			.where("a === g").select("c, g");

		tableEnv.toDataSet(result, Row.class).collect();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoinWithAmbiguousFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, c");

		// Must fail. Join input have overlapping field names.
		in1.join(in2).where("a === d").select("c, g");
	}

	@Test
	public void testJoinWithAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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

	@Test(expected = TableException.class)
	public void testJoinTablesFromDifferentEnvs() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv1 = TableEnvironment.getTableEnvironment(env);
		BatchTableEnvironment tEnv2 = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tEnv1.fromDataSet(ds1, "a, b, c");
		Table in2 = tEnv2.fromDataSet(ds2, "d, e, f, g, h");

		// Must fail. Tables are bound to different TableEnvironments.
		in1.join(in2).where("a === d").select("g.count");
	}

}
