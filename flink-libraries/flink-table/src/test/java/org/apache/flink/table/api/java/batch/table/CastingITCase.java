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

import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CastingITCase extends TableProgramsTestBase {

	public CastingITCase(TestExecutionMode mode, TableConfigMode configMode){
		super(mode, configMode);
	}

	@Test
	public void testNumericAutocastInArithmetic() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSource<Tuple8<Byte, Short, Integer, Long, Float, Double, Long, Double>> input =
				env.fromElements(new Tuple8<>((byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d, 1L, 1001.1));

		Table table =
				tableEnv.fromDataSet(input);

		Table result = table.select("f0 + 1, f1 +" +
				" 1, f2 + 1L, f3 + 1.0f, f4 + 1.0d, f5 + 1, f6 + 1.0d, f7 + f0");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "2,2,2,2.0,2.0,2.0,2.0,1002.1";
		compareResultAsText(results, expected);
	}

	@Test
	public void testNumericAutocastInComparison() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSource<Tuple6<Byte, Short, Integer, Long, Float, Double>> input =
				env.fromElements(
						new Tuple6<>((byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d),
						new Tuple6<>((byte) 2, (short) 2, 2, 2L, 2.0f, 2.0d));

		Table table =
				tableEnv.fromDataSet(input, "a,b,c,d,e,f");

		Table result = table
				.filter("a > 1 && b > 1 && c > 1L && d > 1.0f && e > 1.0d && f > 1");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "2,2,2,2,2.0,2.0";
		compareResultAsText(results, expected);
	}

	@Test
	public void testCasting() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSource<Tuple4<Integer, Double, Long, Boolean>> input =
				env.fromElements(new Tuple4<>(1, 0.0, 1L, true));

		Table table =
				tableEnv.fromDataSet(input);

		Table result = table.select(
				// * -> String
				"f0.cast(STRING), f1.cast(STRING), f2.cast(STRING), f3.cast(STRING)," +
				// NUMERIC TYPE -> Boolean
				"f0.cast(BOOL), f1.cast(BOOL), f2.cast(BOOL)," +
				// NUMERIC TYPE -> NUMERIC TYPE
				"f0.cast(DOUBLE), f1.cast(INT), f2.cast(SHORT)," +
				// Boolean -> NUMERIC TYPE
				"f3.cast(DOUBLE)," +
				// identity casting
				"f0.cast(INT), f1.cast(DOUBLE), f2.cast(LONG), f3.cast(BOOL)");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,0.0,1,true," +
			"true,false,true," +
			"1.0,0,1," +
			"1.0," +
			"1,0.0,1,true\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testCastFromString() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSource<Tuple3<String, String, String>> input =
				env.fromElements(new Tuple3<>("1", "true", "2.0"));

		Table table =
				tableEnv.fromDataSet(input);

		Table result = table.select(
				"f0.cast(BYTE), f0.cast(SHORT), f0.cast(INT), f0.cast(LONG), f2.cast(DOUBLE), f2.cast(FLOAT), f1.cast(BOOL)");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1,1,1,2.0,2.0,true\n";
		compareResultAsText(results, expected);
	}
}

