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

import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.RowFunction;
import org.apache.flink.api.table.Table;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CustomRowFunctionsITCase extends MultipleProgramsTestBase {

	public CustomRowFunctionsITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testOneArgFunction() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		RowFunction<String> rf = new RowFunction<String>() {
			@Override
			public String call(Object[] args) {
				return ((String) args[0]).trim();
			}
		};

		tableEnv.getConfig().registerRowFunction("TRIM", rf, BasicTypeInfo.STRING_TYPE_INFO);

		DataSource<Tuple1<String>> input = env.fromElements(
				new Tuple1<>(" 1 "),
				new Tuple1<>(" 	2 "),
				new Tuple1<>(" 		3 "));

		Table table = tableEnv.fromDataSet(input);

		Table result = table.select("TRIM(f0)");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "1\n2\n3";
		compareResultAsText(results, expected);
	}

	@Test
	public void testZeroArgFunction() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		RowFunction<Long> rf = new RowFunction<Long>() {
			@Override
			public Long call(Object[] args) {
				return 654654654L;
			}
		};

		tableEnv.getConfig().registerRowFunction("SYSTEM_TIME", rf);

		DataSource<Tuple1<String>> input = env.fromElements(new Tuple1<>(" 1 "));

		Table table = tableEnv.fromDataSet(input);

		Table result = table.select("SYSTEM_TIME(), SYSTEM_TIME(), SYSTEM_TIME()");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "654654654,654654654,654654654";
		compareResultAsText(results, expected);
	}

	@Test
	public void testMultiArgFunction() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		RowFunction<String> rf = new RowFunction<String>() {
			@Override
			public String call(Object[] args) {
				return ((String) args[0]).substring((Integer) args[1], (Integer) args[2]);
			}
		};

		tableEnv.getConfig().registerRowFunction(
				"SUBSTRING",
				rf,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO);

		DataSource<Tuple1<String>> input = env.fromElements(new Tuple1<>("Hello World"));

		Table table = tableEnv.fromDataSet(input);

		Table result = table.select("SUBSTRING(SUBSTRING(f0, 2, 5), 0, 1)");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "l";
		compareResultAsText(results, expected);
	}

	@Test
	public void testFunctionWithCasting() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

		RowFunction<Date> rf = new RowFunction<Date>() {
			@Override
			public Date call(Object[] args) {
				return new Date((Long) args[0]);
			}
		};

		tableEnv.getConfig().registerRowFunction("LONG2DATE", rf, BasicTypeInfo.LONG_TYPE_INFO);

		DataSource<Tuple1<Integer>> input = env.fromElements(new Tuple1<>(1000));

		Table table = tableEnv.fromDataSet(input);

		Table result = table.select("LONG2DATE(f0)");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "Thu Jan 01 00:00:01 UTC 1970";
		compareResultAsText(results, expected);
	}

	@Test
	public void testFunctionOverloading() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		RowFunction<String> rf = new RowFunction<String>() {
			@Override
			public String call(Object[] args) {
				if (args.length == 2) {
					return ((String) args[0]).substring((Integer) args[1]);
				}
				return ((String) args[0]).substring((Integer) args[1], (Integer) args[2]);
			}
		};

		tableEnv.getConfig().registerRowFunction(
				"SUBSTRING",
				rf,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO);

		tableEnv.getConfig().registerRowFunction(
				"SUBSTRING",
				rf,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO);

		DataSource<Tuple1<String>> input = env.fromElements(new Tuple1<>("Hello World"));

		Table table = tableEnv.fromDataSet(input);

		Table result = table.select("SUBSTRING(f0, 1), SUBSTRING(f0, 2, 5)");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "ello World,llo";
		compareResultAsText(results, expected);
	}

	@Test
	public void testFunctionWithNull() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();

		RowFunction<String> rf = new RowFunction<String>() {
			@Override
			public String call(Object[] args) {
				return ((String) args[0]).trim();
			}
		};

		tableEnv.getConfig().registerRowFunction("TRIM", rf, BasicTypeInfo.STRING_TYPE_INFO);

		DataSource<Tuple1<String>> input = env.fromElements(
				new Tuple1<>(" 1 "),
				new Tuple1<>(" 	2 "),
				new Tuple1<>(" 		3 "));

		Table table = tableEnv.fromDataSet(input);

		Table result = table.select("TRIM(f0)");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "1\n2\n3";
		compareResultAsText(results, expected);
	}

}
