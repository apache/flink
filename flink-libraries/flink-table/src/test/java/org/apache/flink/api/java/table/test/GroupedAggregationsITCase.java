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

import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class GroupedAggregationsITCase extends MultipleProgramsTestBase {

	public GroupedAggregationsITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGroupingOnNonExistentField() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		tableEnv
			.fromDataSet(input, "a, b, c")
			// must fail. Field foo is not in input
			.groupBy("foo")
			.select("a.avg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGroupingInvalidSelection() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		tableEnv
			.fromDataSet(input, "a, b, c")
			.groupBy("a, b")
			// must fail. Field c is not a grouping key or aggregation
			.select("c");
	}

	@Test
	public void testGroupedAggregate() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);
		Table table = tableEnv.fromDataSet(input, "a, b, c");

		Table result = table
				.groupBy("b").select("b, a.sum");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1\n" + "2,5\n" + "3,15\n" + "4,34\n" + "5,65\n" + "6,111\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testGroupingKeyForwardIfNotUsed() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);
		Table table = tableEnv.fromDataSet(input, "a, b, c");

		Table result = table
				.groupBy("b").select("a.sum");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "1\n" + "5\n" + "15\n" + "34\n" + "65\n" + "111\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testGroupNoAggregation() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);
		Table table = tableEnv.fromDataSet(input, "a, b, c");

		Table result = table
			.groupBy("b").select("a.sum as d, b").groupBy("b, d").select("b");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		String expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n";
		List<Row> results = ds.collect();
		compareResultAsText(results, expected);
	}
}

