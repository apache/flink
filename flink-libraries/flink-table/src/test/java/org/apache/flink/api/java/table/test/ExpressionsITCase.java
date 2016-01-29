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
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.NotImplementedError;

import java.util.List;

@RunWith(Parameterized.class)
public class ExpressionsITCase extends MultipleProgramsTestBase {


	public ExpressionsITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test(expected = NotImplementedError.class)
	public void testArithmetic() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple2<Integer, Integer>> input =
				env.fromElements(new Tuple2<>(5, 10));

		Table table =
				tableEnv.fromDataSet(input, "a, b");

		Table result = table.select(
				"a - 5, a + 5, a / 2, a * 2, a % 2, -a");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "0,10,2,10,1,-5";
		compareResultAsText(results, expected);
	}

	@Test(expected = NotImplementedError.class)
	public void testLogic() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple2<Integer, Boolean>> input =
				env.fromElements(new Tuple2<>(5, true));

		Table table =
				tableEnv.fromDataSet(input, "a, b");

		Table result = table.select(
				"b && true, b && false, b || false, !b");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "true,false,true,false";
		compareResultAsText(results, expected);
	}

	@Test(expected = NotImplementedError.class)
	public void testComparisons() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		DataSource<Tuple3<Integer, Integer, Integer>> input =
				env.fromElements(new Tuple3<>(5, 5, 4));

		Table table =
				tableEnv.fromDataSet(input, "a, b, c");

		Table result = table.select(
				"a > c, a >= b, a < c, a.isNull, a.isNotNull");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "true,true,false,false,true";
		compareResultAsText(results, expected);
	}

}

