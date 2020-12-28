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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.utils.DummyStreamExecutionEnvironment;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A utility class used in PyFlink. */
public class CommonPythonUtil {
    private static final Method convertLiteralToPython;

    private static final String PYTHON_DEPENDENCY_UTILS_CLASS =
            "org.apache.flink.python.util.PythonDependencyUtils";

    static {
        convertLiteralToPython = loadConvertLiteralToPythonMethod();
    }

    private CommonPythonUtil() {}

    public static Class loadClass(String className) {
        try {
            return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new TableException(
                    "The dependency of 'flink-python' is not present on the classpath.", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Configuration getConfig(StreamExecutionEnvironment env, TableConfig tableConfig) {
        Class clazz = loadClass(PYTHON_DEPENDENCY_UTILS_CLASS);
        try {
            StreamExecutionEnvironment readEnv = getRealEnvironment(env);
            Method method =
                    clazz.getDeclaredMethod(
                            "configurePythonDependencies", List.class, Configuration.class);
            Configuration config =
                    (Configuration)
                            method.invoke(
                                    null,
                                    readEnv.getCachedFiles(),
                                    getMergedConfiguration(readEnv, tableConfig));
            config.setString("table.exec.timezone", tableConfig.getLocalTimeZone().getId());
            return config;
        } catch (NoSuchFieldException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            throw new TableException("Method configurePythonDependencies accessed failed.", e);
        }
    }

    public static PythonFunctionInfo createPythonFunctionInfo(
            RexCall pythonRexCall, Map<RexNode, Integer> inputNodes) {
        SqlOperator operator = pythonRexCall.getOperator();
        try {
            if (operator instanceof ScalarSqlFunction) {
                return createPythonFunctionInfo(
                        pythonRexCall, inputNodes, ((ScalarSqlFunction) operator).scalarFunction());
            } else if (operator instanceof TableSqlFunction) {
                return createPythonFunctionInfo(
                        pythonRexCall, inputNodes, ((TableSqlFunction) operator).udtf());
            } else if (operator instanceof BridgingSqlFunction) {
                return createPythonFunctionInfo(
                        pythonRexCall,
                        inputNodes,
                        ((BridgingSqlFunction) operator).getDefinition());
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new TableException("Method convertLiteralToPython accessed failed. ", e);
        }
        throw new TableException(String.format("Unsupported Python SqlFunction %s.", operator));
    }

    @SuppressWarnings("unchecked")
    public static boolean isPythonWorkerUsingManagedMemory(Configuration config) {
        Class clazz = loadClass("org.apache.flink.python.PythonOptions");
        try {
            return config.getBoolean(
                    (ConfigOption<Boolean>) (clazz.getField("USE_MANAGED_MEMORY").get(null)));
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new TableException("Field USE_MANAGED_MEMORY accessed failed.", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Method loadConvertLiteralToPythonMethod() {
        Class clazz = loadClass("org.apache.flink.api.common.python.PythonBridgeUtils");
        try {
            return clazz.getMethod("convertLiteralToPython", RexLiteral.class, SqlTypeName.class);
        } catch (NoSuchMethodException e) {
            throw new TableException("Method convertLiteralToPython loaded failed.", e);
        }
    }

    private static PythonFunctionInfo createPythonFunctionInfo(
            RexCall pythonRexCall,
            Map<RexNode, Integer> inputNodes,
            FunctionDefinition functionDefinition)
            throws InvocationTargetException, IllegalAccessException {
        ArrayList<Object> inputs = new ArrayList<>();
        for (RexNode operand : pythonRexCall.getOperands()) {
            if (operand instanceof RexCall) {
                RexCall childPythonRexCall = (RexCall) operand;
                PythonFunctionInfo argPythonInfo =
                        createPythonFunctionInfo(childPythonRexCall, inputNodes);
                inputs.add(argPythonInfo);
            } else if (operand instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) operand;
                inputs.add(
                        convertLiteralToPython.invoke(
                                null, literal, literal.getType().getSqlTypeName()));
            } else {
                if (inputNodes.containsKey(operand)) {
                    inputs.add(inputNodes.get(operand));
                } else {
                    Integer inputOffset = inputNodes.size();
                    inputs.add(inputOffset);
                    inputNodes.put(operand, inputOffset);
                }
            }
        }
        return new PythonFunctionInfo((PythonFunction) functionDefinition, inputs.toArray());
    }

    private static StreamExecutionEnvironment getRealEnvironment(StreamExecutionEnvironment env)
            throws NoSuchFieldException, IllegalAccessException {
        Field realExecEnvField =
                DummyStreamExecutionEnvironment.class.getDeclaredField("realExecEnv");
        realExecEnvField.setAccessible(true);
        while (env instanceof DummyStreamExecutionEnvironment) {
            env = (StreamExecutionEnvironment) realExecEnvField.get(env);
        }
        return env;
    }

    private static Configuration getMergedConfiguration(
            StreamExecutionEnvironment env, TableConfig tableConfig)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = StreamExecutionEnvironment.class.getDeclaredMethod("getConfiguration");
        method.setAccessible(true);
        Configuration config = new Configuration((Configuration) method.invoke(env));
        config.addAll(tableConfig.getConfiguration());
        return config;
    }
}
