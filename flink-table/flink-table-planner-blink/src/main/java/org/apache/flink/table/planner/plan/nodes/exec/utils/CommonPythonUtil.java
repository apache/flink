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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.dataview.DataView;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.python.BuiltInPythonAggregateFunction;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.ListAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.ListAggWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.ListAggWsWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.SumWithRetractAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.planner.utils.DummyStreamExecutionEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/** A utility class used in PyFlink. */
public class CommonPythonUtil {
    private static Method convertLiteralToPython = null;

    private static final String PYTHON_CONFIG_UTILS_CLASS =
            "org.apache.flink.python.util.PythonConfigUtil";

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
    public static Configuration getMergedConfig(
            StreamExecutionEnvironment env, TableConfig tableConfig) {
        Class clazz = loadClass(PYTHON_CONFIG_UTILS_CLASS);
        try {
            StreamExecutionEnvironment realEnv = getRealEnvironment(env);
            Method method =
                    clazz.getDeclaredMethod(
                            "getMergedConfig", StreamExecutionEnvironment.class, TableConfig.class);
            return (Configuration) method.invoke(null, realEnv, tableConfig);
        } catch (NoSuchFieldException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            throw new TableException("Method getMergedConfig accessed failed.", e);
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

    public static Tuple2<PythonAggregateFunctionInfo[], DataViewUtils.DataViewSpec[][]>
            extractPythonAggregateFunctionInfos(
                    AggregateInfoList pythonAggregateInfoList, AggregateCall[] aggCalls) {
        List<PythonAggregateFunctionInfo> pythonAggregateFunctionInfoList = new ArrayList<>();
        List<DataViewUtils.DataViewSpec[]> dataViewSpecList = new ArrayList<>();
        AggregateInfo[] aggInfos = pythonAggregateInfoList.aggInfos();
        for (int i = 0; i < aggInfos.length; i++) {
            AggregateInfo aggInfo = aggInfos[i];
            UserDefinedFunction function = aggInfo.function();
            if (function instanceof PythonFunction) {
                pythonAggregateFunctionInfoList.add(
                        new PythonAggregateFunctionInfo(
                                (PythonFunction) function,
                                Arrays.stream(aggInfo.argIndexes()).boxed().toArray(),
                                aggCalls[i].filterArg,
                                aggCalls[i].isDistinct()));
                TypeInference typeInference = function.getTypeInference(null);
                dataViewSpecList.add(
                        extractDataViewSpecs(
                                i,
                                typeInference
                                        .getAccumulatorTypeStrategy()
                                        .get()
                                        .inferType(null)
                                        .get()));
            } else {
                int filterArg = -1;
                boolean distinct = false;
                if (i < aggCalls.length) {
                    filterArg = aggCalls[i].filterArg;
                    distinct = aggCalls[i].isDistinct();
                }
                pythonAggregateFunctionInfoList.add(
                        new PythonAggregateFunctionInfo(
                                getBuiltInPythonAggregateFunction(function),
                                Arrays.stream(aggInfo.argIndexes()).boxed().toArray(),
                                filterArg,
                                distinct));
                // The data views of the built in Python Aggregate Function are different from Java
                // side, we will create the spec at Python side.
                dataViewSpecList.add(new DataViewUtils.DataViewSpec[0]);
            }
        }
        return Tuple2.of(
                pythonAggregateFunctionInfoList.toArray(new PythonAggregateFunctionInfo[0]),
                dataViewSpecList.toArray(new DataViewUtils.DataViewSpec[0][0]));
    }

    public static Tuple2<int[], PythonFunctionInfo[]>
            extractPythonAggregateFunctionInfosFromAggregateCall(AggregateCall[] aggCalls) {
        Map<Integer, Integer> inputNodes = new LinkedHashMap<>();
        List<PythonFunctionInfo> pythonFunctionInfos = new ArrayList<>();
        for (AggregateCall aggregateCall : aggCalls) {
            List<Integer> inputs = new ArrayList<>();
            List<Integer> argList = aggregateCall.getArgList();
            for (Integer arg : argList) {
                if (inputNodes.containsKey(arg)) {
                    inputs.add(inputNodes.get(arg));
                } else {
                    Integer inputOffset = inputNodes.size();
                    inputs.add(inputOffset);
                    inputNodes.put(arg, inputOffset);
                }
            }
            PythonFunction pythonFunction = null;
            SqlAggFunction aggregateFunction = aggregateCall.getAggregation();
            if (aggregateFunction instanceof AggSqlFunction) {
                pythonFunction =
                        (PythonFunction) ((AggSqlFunction) aggregateFunction).aggregateFunction();
            } else if (aggregateFunction instanceof BridgingSqlAggFunction) {
                pythonFunction =
                        (PythonFunction)
                                ((BridgingSqlAggFunction) aggregateFunction).getDefinition();
            }
            PythonFunctionInfo pythonFunctionInfo =
                    new PythonAggregateFunctionInfo(
                            pythonFunction,
                            inputs.toArray(),
                            aggregateCall.filterArg,
                            aggregateCall.isDistinct());
            pythonFunctionInfos.add(pythonFunctionInfo);
        }
        int[] udafInputOffsets = inputNodes.keySet().stream().mapToInt(i -> i).toArray();
        return Tuple2.of(udafInputOffsets, pythonFunctionInfos.toArray(new PythonFunctionInfo[0]));
    }

    public static DataViewUtils.DataViewSpec[] extractDataViewSpecs(int index, DataType accType) {
        if (!(accType instanceof FieldsDataType)) {
            return new DataViewUtils.DataViewSpec[0];
        }
        FieldsDataType compositeAccType = (FieldsDataType) accType;
        if (includesDataView(compositeAccType)) {
            LogicalType logicalType = compositeAccType.getLogicalType();
            if (logicalType instanceof RowType) {
                List<DataType> childrenDataTypes = compositeAccType.getChildren();
                return IntStream.range(0, childrenDataTypes.size())
                        .mapToObj(
                                i -> {
                                    DataType childDataType = childrenDataTypes.get(i);
                                    LogicalType childLogicalType = childDataType.getLogicalType();
                                    if ((childLogicalType instanceof RowType)
                                            && includesDataView((FieldsDataType) childDataType)) {
                                        throw new TableException(
                                                "For Python AggregateFunction, DataView cannot be used in the"
                                                        + " nested columns of the accumulator. ");
                                    } else if ((childLogicalType instanceof StructuredType)
                                            && ListView.class.isAssignableFrom(
                                                    ((StructuredType) childLogicalType)
                                                            .getImplementationClass()
                                                            .get())) {
                                        return new DataViewUtils.ListViewSpec(
                                                "agg"
                                                        + index
                                                        + "$"
                                                        + ((RowType) logicalType)
                                                                .getFieldNames()
                                                                .get(i),
                                                i,
                                                childDataType.getChildren().get(0));
                                    } else if ((childLogicalType instanceof StructuredType)
                                            && MapView.class.isAssignableFrom(
                                                    ((StructuredType) childLogicalType)
                                                            .getImplementationClass()
                                                            .get())) {
                                        return new DataViewUtils.MapViewSpec(
                                                "agg"
                                                        + index
                                                        + "$"
                                                        + ((RowType) logicalType)
                                                                .getFieldNames()
                                                                .get(i),
                                                i,
                                                childDataType.getChildren().get(0),
                                                false);
                                    }
                                    return null;
                                })
                        .filter(Objects::nonNull)
                        .toArray(DataViewUtils.DataViewSpec[]::new);
            } else {
                throw new TableException(
                        "For Python AggregateFunction you can only use DataView in " + "Row type.");
            }
        } else {
            return new DataViewUtils.DataViewSpec[0];
        }
    }

    private static boolean includesDataView(FieldsDataType fdt) {
        return fdt.getChildren().stream()
                .anyMatch(
                        childrenFieldsDataType -> {
                            LogicalType logicalType = childrenFieldsDataType.getLogicalType();
                            if (logicalType instanceof RowType) {
                                return includesDataView((FieldsDataType) childrenFieldsDataType);
                            } else if (logicalType instanceof StructuredType) {
                                return DataView.class.isAssignableFrom(
                                        ((StructuredType) logicalType)
                                                .getImplementationClass()
                                                .get());
                            } else {
                                return false;
                            }
                        });
    }

    @SuppressWarnings("unchecked")
    private static void loadConvertLiteralToPythonMethod() {
        if (convertLiteralToPython == null) {
            synchronized (CommonPythonUtil.class) {
                if (convertLiteralToPython == null) {
                    Class clazz = loadClass("org.apache.flink.api.common.python.PythonBridgeUtils");
                    try {
                        convertLiteralToPython =
                                clazz.getMethod(
                                        "convertLiteralToPython",
                                        RexLiteral.class,
                                        SqlTypeName.class);
                    } catch (NoSuchMethodException e) {
                        throw new TableException("Method convertLiteralToPython loaded failed.", e);
                    }
                }
            }
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
                loadConvertLiteralToPythonMethod();
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

    private static BuiltInPythonAggregateFunction getBuiltInPythonAggregateFunction(
            UserDefinedFunction javaBuiltInAggregateFunction) {
        if (javaBuiltInAggregateFunction instanceof AvgAggFunction) {
            return BuiltInPythonAggregateFunction.AVG;
        }
        if (javaBuiltInAggregateFunction instanceof Count1AggFunction) {
            return BuiltInPythonAggregateFunction.COUNT1;
        }
        if (javaBuiltInAggregateFunction instanceof CountAggFunction) {
            return BuiltInPythonAggregateFunction.COUNT;
        }
        if (javaBuiltInAggregateFunction instanceof FirstValueAggFunction) {
            return BuiltInPythonAggregateFunction.FIRST_VALUE;
        }
        if (javaBuiltInAggregateFunction instanceof FirstValueWithRetractAggFunction) {
            return BuiltInPythonAggregateFunction.FIRST_VALUE_RETRACT;
        }
        if (javaBuiltInAggregateFunction instanceof LastValueAggFunction) {
            return BuiltInPythonAggregateFunction.LAST_VALUE;
        }
        if (javaBuiltInAggregateFunction instanceof LastValueWithRetractAggFunction) {
            return BuiltInPythonAggregateFunction.LAST_VALUE_RETRACT;
        }
        if (javaBuiltInAggregateFunction instanceof ListAggFunction) {
            return BuiltInPythonAggregateFunction.LIST_AGG;
        }
        if (javaBuiltInAggregateFunction instanceof ListAggWithRetractAggFunction) {
            return BuiltInPythonAggregateFunction.LIST_AGG_RETRACT;
        }
        if (javaBuiltInAggregateFunction instanceof ListAggWsWithRetractAggFunction) {
            return BuiltInPythonAggregateFunction.LIST_AGG_WS_RETRACT;
        }
        if (javaBuiltInAggregateFunction instanceof MaxAggFunction) {
            return BuiltInPythonAggregateFunction.MAX;
        }
        if (javaBuiltInAggregateFunction instanceof MaxWithRetractAggFunction) {
            return BuiltInPythonAggregateFunction.MAX_RETRACT;
        }
        if (javaBuiltInAggregateFunction instanceof MinAggFunction) {
            return BuiltInPythonAggregateFunction.MIN;
        }
        if (javaBuiltInAggregateFunction instanceof MinWithRetractAggFunction) {
            return BuiltInPythonAggregateFunction.MIN_RETRACT;
        }
        if (javaBuiltInAggregateFunction instanceof SumAggFunction) {
            return BuiltInPythonAggregateFunction.SUM;
        }
        if (javaBuiltInAggregateFunction instanceof Sum0AggFunction.IntSum0AggFunction) {
            return BuiltInPythonAggregateFunction.INT_SUM0;
        }
        if (javaBuiltInAggregateFunction instanceof Sum0AggFunction.ByteSum0AggFunction) {
            return BuiltInPythonAggregateFunction.INT_SUM0;
        }
        if (javaBuiltInAggregateFunction instanceof Sum0AggFunction.ShortSum0AggFunction) {
            return BuiltInPythonAggregateFunction.INT_SUM0;
        }
        if (javaBuiltInAggregateFunction instanceof Sum0AggFunction.LongSum0AggFunction) {
            return BuiltInPythonAggregateFunction.INT_SUM0;
        }
        if (javaBuiltInAggregateFunction instanceof Sum0AggFunction.FloatSum0AggFunction) {
            return BuiltInPythonAggregateFunction.FLOAT_SUM0;
        }
        if (javaBuiltInAggregateFunction instanceof Sum0AggFunction.DoubleSum0AggFunction) {
            return BuiltInPythonAggregateFunction.FLOAT_SUM0;
        }
        if (javaBuiltInAggregateFunction instanceof Sum0AggFunction.DecimalSum0AggFunction) {
            return BuiltInPythonAggregateFunction.DECIMAL_SUM0;
        }
        if (javaBuiltInAggregateFunction instanceof SumWithRetractAggFunction) {
            return BuiltInPythonAggregateFunction.SUM_RETRACT;
        }
        throw new TableException(
                "Aggregate function "
                        + javaBuiltInAggregateFunction
                        + " is still not supported to be mixed with Python UDAF.");
    }
}
