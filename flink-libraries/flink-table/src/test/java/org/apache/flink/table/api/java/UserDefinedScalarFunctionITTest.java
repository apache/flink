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

package org.apache.flink.table.api.java;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UDFContext;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Test;

import java.util.List;

/**
 * This integration-test cases test java user scalar function
 */
public class UserDefinedScalarFunctionITTest {

	@Test
	public void testUDFWithoutOpenClose() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		tEnv.registerFunction("JavaFunc0", new JavaFunc0());

		String sqlQuery = "SELECT c FROM t1 where JavaFunc0(a)=4";

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
		tEnv.registerDataSet("t1", ds, "a,b,c");

		Table result = tEnv.sql(sqlQuery);

		String expected = "Hello world";
		List<String> results = tEnv.toDataSet(result, String.class).collect();
		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testUDFWithOpenClose() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		tEnv.registerFunction("JavaFunc1", new JavaFunc1());

		String sqlQuery = "SELECT c FROM t1 where JavaFunc1(a)=13";

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
		tEnv.registerDataSet("t1", ds, "a,b,c");

		Table result = tEnv.sql(sqlQuery);

		String expected = "Hello world";
		List<String> results = tEnv.toDataSet(result, String.class).collect();
		TestBaseUtils.compareResultAsText(results, expected);
	}

	public static class JavaFunc0 extends ScalarFunction {
		public int eval(int i) {
			return i + 1;
		}
	}

	public static class JavaFunc1 extends ScalarFunction {
		private int base = 0;

		@Override
		public void open(UDFContext context) throws Exception {
			base = 10;
		}

		@Override
		public void close() throws Exception {
			base = 0;
		}

		public int eval(int i) {
			return i + base;
		}
	}
}
