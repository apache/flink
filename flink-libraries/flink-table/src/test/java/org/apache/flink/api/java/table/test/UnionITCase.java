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
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.TableException;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class UnionITCase extends MultipleProgramsTestBase {

	public UnionITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testUnion() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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

	@Test
	public void testUnionWithFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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
	public void testUnionIncompatibleNumberOfFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "d, e, f, g, h");

		// Must fail. Number of fields of union inputs do not match
		in1.unionAll(in2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnionIncompatibleFieldsName() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "a, b, d");

		// Must fail. Field names of union inputs do not match
		in1.unionAll(in2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnionIncompatibleFieldTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "a, b, c, d, e").select("a, b, c");

		// Must fail. Field types of union inputs do not match
		in1.unionAll(in2);
	}

	@Test
	public void testUnionWithAggregation() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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

	@Test
	public void testUnionWithJoin() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds3 = CollectionDataSets.getSmall5TupleDataSet(env);

		Table in1 = tableEnv.fromDataSet(ds1, "a, b, c");
		Table in2 = tableEnv.fromDataSet(ds2, "a, b, d, c, e").select("a, b, c");
		Table in3 = tableEnv.fromDataSet(ds3, "a2, b2, d2, c2, e2").select("a2, b2, c2");

	    Table joinDs = in1.unionAll(in2).join(in3).where("a === a2").select("c, c2");
	    DataSet<Row> ds = tableEnv.toDataSet(joinDs, Row.class);
	    List<Row> results = ds.collect();

	    String expected = "Hi,Hallo\n" + "Hallo,Hallo\n" +
	      "Hello,Hallo Welt\n" + "Hello,Hallo Welt wie\n" +
	      "Hallo Welt,Hallo Welt\n" + "Hallo Welt wie,Hallo Welt\n" +
	      "Hallo Welt,Hallo Welt wie\n" + "Hallo Welt wie,Hallo Welt wie\n";
	    compareResultAsText(results, expected);
	}

	@Test(expected = TableException.class)
	public void testUnionTablesFromDifferentEnvs() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv1 = TableEnvironment.getTableEnvironment(env);
		BatchTableEnvironment tEnv2 = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);

		Table in1 = tEnv1.fromDataSet(ds1, "a, b, c");
		Table in2 = tEnv2.fromDataSet(ds2, "a, b, c");

		// Must fail. Tables are bound to different TableEnvironments.
		in1.unionAll(in2);
	}
}
