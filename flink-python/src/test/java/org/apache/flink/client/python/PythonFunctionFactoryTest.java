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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY;
import static org.apache.flink.python.PythonOptions.PYTHON_FILES;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Tests for PythonFunctionFactory. This test will be executed from Python side. Because the maven
 * test environment may not has a python 3.5+ installed.
 */
public class PythonFunctionFactoryTest {

    private static String tmpdir = "";
    private static StreamTableEnvironment tableEnv;
    private static Table sourceTable;

    public static void main(String[] args) throws Exception {
        prepareEnvironment();
        testPythonFunctionFactory();
        cleanEnvironment();
    }

    public static void prepareEnvironment() throws Exception {
        tmpdir =
                new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString())
                        .getAbsolutePath();
        new File(tmpdir).mkdir();
        File pyFilePath = new File(tmpdir, "test1.py");
        try (OutputStream out = new FileOutputStream(pyFilePath)) {
            String code =
                    ""
                            + "from pyflink.table.udf import udf\n"
                            + "from pyflink.table import DataTypes\n"
                            + "@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())\n"
                            + "def func1(str):\n"
                            + "    return str + str\n";
            out.write(code.getBytes());
        }
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = StreamTableEnvironment.create(sEnv);
        tableEnv.getConfig().getConfiguration().set(PYTHON_FILES, pyFilePath.getAbsolutePath());
        tableEnv.getConfig().getConfiguration().setString(TASK_OFF_HEAP_MEMORY.key(), "80mb");
        sourceTable = tableEnv.fromDataStream(sEnv.fromElements("1", "2", "3")).as("str");
    }

    public static void cleanEnvironment() throws Exception {
        closeStartedPythonProcess();
        FileUtils.deleteDirectory(new File(tmpdir));
    }

    public static void testPythonFunctionFactory() {
        // catalog
        tableEnv.executeSql("create function func1 as 'test1.func1' language python");
        verifyPlan(sourceTable.select(call("func1", $("str"))), tableEnv);

        // catalog
        tableEnv.executeSql("alter function func1 as 'test1.func1' language python");
        verifyPlan(sourceTable.select(call("func1", $("str"))), tableEnv);

        // temporary catalog
        tableEnv.executeSql("create temporary function func1 as 'test1.func1' language python");
        verifyPlan(sourceTable.select(call("func1", $("str"))), tableEnv);

        // temporary system
        tableEnv.executeSql(
                "create temporary system function func1 as 'test1.func1' language python");
        verifyPlan(sourceTable.select(call("func1", $("str"))), tableEnv);
    }

    private static void verifyPlan(Table table, TableEnvironment tableEnvironment) {
        String plan = table.explain();
        String expected = "PythonCalc(select=[func1(f0) AS _c0])";
        if (!plan.contains(expected)) {
            throw new AssertionError(
                    String.format("This plan does not contains \"%s\":\n%s", expected, plan));
        }
    }

    private static void closeStartedPythonProcess()
            throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        Class clazz = Class.forName("java.lang.ApplicationShutdownHooks");
        Field field = clazz.getDeclaredField("hooks");
        field.setAccessible(true);
        Map<Thread, Thread> hooks = (Map<Thread, Thread>) field.get(null);
        PythonEnvUtils.PythonProcessShutdownHook shutdownHook = null;
        for (Thread t : hooks.keySet()) {
            if (t instanceof PythonEnvUtils.PythonProcessShutdownHook) {
                shutdownHook = (PythonEnvUtils.PythonProcessShutdownHook) t;
                break;
            }
        }
        if (shutdownHook != null) {
            shutdownHook.run();
            hooks.remove(shutdownHook);
        }
    }
}
