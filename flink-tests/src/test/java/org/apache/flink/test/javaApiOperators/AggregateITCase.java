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

package org.apache.flink.test.javaApiOperators;

import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

@RunWith(Parameterized.class)
public class AggregateITCase extends MultipleProgramsTestBase {


	public AggregateITCase(TestExecutionMode mode){
		super(mode);
	}

	private String resultPath;
	private String expected;

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
	public void testFullAggregate() throws Exception {
		/*
				 * Full Aggregate
				 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple2<Integer, Long>> aggregateDs = ds
				.aggregate(Aggregations.SUM, 0)
				.and(Aggregations.MAX, 1)
						.project(0, 1);

		aggregateDs.writeAsCsv(resultPath);
		env.execute();

		expected = "231,6\n";
	}

	@Test
	public void testGroupedAggregate() throws Exception {
		/*
				 * Grouped Aggregate
				 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple2<Long, Integer>> aggregateDs = ds.groupBy(1)
				.aggregate(Aggregations.SUM, 0)
						.project(1, 0);

		aggregateDs.writeAsCsv(resultPath);
		env.execute();

		expected = "1,1\n" +
				"2,5\n" +
				"3,15\n" +
				"4,34\n" +
				"5,65\n" +
				"6,111\n";
	}

	@Test
	public void testNestedAggregate() throws Exception {
		/*
		 * Nested Aggregate
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple1<Integer>> aggregateDs = ds.groupBy(1)
				.aggregate(Aggregations.MIN, 0)
				.aggregate(Aggregations.MIN, 0)
						.project(0);

		aggregateDs.writeAsCsv(resultPath);
		env.execute();

		expected = "1\n";
	}
}
