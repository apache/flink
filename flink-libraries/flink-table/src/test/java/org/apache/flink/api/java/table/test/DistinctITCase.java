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

import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DistinctITCase extends MultipleProgramsTestBase {

	public DistinctITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testDistinct() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple3<Integer, Long, String>> input = CollectionDataSets.get3TupleDataSet(env);

		Table table = tableEnv.fromDataSet(input, "a, b, c");

		Table distinct = table.select("b").distinct();

		DataSet<Row> ds = tableEnv.toDataSet(distinct, Row.class);
		List<Row> results = ds.collect();
		String expected = "1\n" + "2\n" + "3\n"+ "4\n"+ "5\n"+ "6\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testDistinctAfterAggregate() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> input = CollectionDataSets.get5TupleDataSet(env);

		Table table = tableEnv.fromDataSet(input, "a, b, c, d, e");

		Table distinct = table.groupBy("a, e").select("e").distinct();

		DataSet<Row> ds = tableEnv.toDataSet(distinct, Row.class);
		List<Row> results = ds.collect();
		String expected = "1\n" + "2\n" + "3\n";
		compareResultAsText(results, expected);
	}
}
