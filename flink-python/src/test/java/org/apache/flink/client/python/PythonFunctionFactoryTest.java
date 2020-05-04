/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.python;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.python.PythonOptions.PYTHON_FILES;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Tests for PythonFunctionFactory. This test will be executed from Python side. Because the maven test environment
 * may not has a python 3.5+ installed.
 */
public class PythonFunctionFactoryTest {

	private static String tmpdir = "";
	private static BatchTableEnvironment flinkTableEnv;
	private static StreamTableEnvironment blinkTableEnv;
	private static Table flinkSourceTable;
	private static Table blinkSourceTable;

	public static void main(String[] args) throws Exception {
		prepareEnvironment();
		testPythonFunctionFactory();
		cleanEnvironment();
	}

	public static void prepareEnvironment() throws Exception {
		tmpdir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString()).getAbsolutePath();
		new File(tmpdir).mkdir();
		File pyFilePath = new File(tmpdir, "test1.py");
		try (OutputStream out = new FileOutputStream(pyFilePath)) {
			String code = ""
				+ "from pyflink.table.udf import udf\n"
				+ "from pyflink.table import DataTypes\n"
				+ "@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())\n"
				+ "def func1(str):\n"
				+ "    return str + str\n";
			out.write(code.getBytes());
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		flinkTableEnv = BatchTableEnvironment.create(env);
		flinkTableEnv.getConfig().getConfiguration().set(PYTHON_FILES, pyFilePath.getAbsolutePath());
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		blinkTableEnv = StreamTableEnvironment.create(
			sEnv, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
		blinkTableEnv.getConfig().getConfiguration().set(PYTHON_FILES, pyFilePath.getAbsolutePath());
		flinkSourceTable = flinkTableEnv.fromDataSet(env.fromElements("1", "2", "3")).as("str");
		blinkSourceTable = blinkTableEnv.fromDataStream(sEnv.fromElements("1", "2", "3")).as("str");
	}

	public static void cleanEnvironment() throws Exception {
		closeStartedPythonProcess();
		FileUtils.deleteDirectory(new File(tmpdir));
	}

	public static void testPythonFunctionFactory() {
		// flink catalog
		flinkTableEnv.sqlUpdate("create function func1 as 'test1.func1' language python");
		verifyPlan(flinkSourceTable.select(call("func1", $("str"))), flinkTableEnv);

		// flink catalog
		flinkTableEnv.sqlUpdate("alter function func1 as 'test1.func1' language python");
		verifyPlan(flinkSourceTable.select(call("func1", $("str"))), flinkTableEnv);

		// flink temporary catalog
		flinkTableEnv.sqlUpdate("create temporary function func1 as 'test1.func1' language python");
		verifyPlan(flinkSourceTable.select(call("func1", $("str"))), flinkTableEnv);

		// flink temporary system
		flinkTableEnv.sqlUpdate("create temporary system function func1 as 'test1.func1' language python");
		verifyPlan(flinkSourceTable.select(call("func1", $("str"))), flinkTableEnv);

		// blink catalog
		blinkTableEnv.sqlUpdate("create function func1 as 'test1.func1' language python");
		verifyPlan(blinkSourceTable.select(call("func1", $("str"))), blinkTableEnv);

		// blink catalog
		blinkTableEnv.sqlUpdate("alter function func1 as 'test1.func1' language python");
		verifyPlan(blinkSourceTable.select(call("func1", $("str"))), blinkTableEnv);

		// blink temporary catalog
		blinkTableEnv.sqlUpdate("create temporary function func1 as 'test1.func1' language python");
		verifyPlan(blinkSourceTable.select(call("func1", $("str"))), blinkTableEnv);

		// blink temporary system
		blinkTableEnv.sqlUpdate("create temporary system function func1 as 'test1.func1' language python");
		verifyPlan(blinkSourceTable.select(call("func1", $("str"))), blinkTableEnv);
	}

	private static void verifyPlan(Table table, TableEnvironment tableEnvironment) {
		String plan = tableEnvironment.explain(table);
		String expected = "PythonCalc(select=[func1(f0) AS _c0])";
		if (!plan.contains(expected)) {
			throw new AssertionError(String.format("This plan does not contains \"%s\":\n%s", expected, plan));
		}
	}

	private static void closeStartedPythonProcess()
			throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
		Class clazz = Class.forName("java.lang.ApplicationShutdownHooks");
		Field field = clazz.getDeclaredField("hooks");
		field.setAccessible(true);
		Map<Thread, Thread> hooks = (Map<Thread, Thread>) field.get(null);
		PythonFunctionFactory.PythonProcessShutdownHook shutdownHook = null;
		for (Thread t : hooks.keySet()) {
			if (t instanceof PythonFunctionFactory.PythonProcessShutdownHook) {
				shutdownHook = (PythonFunctionFactory.PythonProcessShutdownHook) t;
				break;
			}
		}
		if (shutdownHook != null) {
			shutdownHook.run();
			hooks.remove(shutdownHook);
		}
	}
}
