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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/** Base class for exec Python Calc. */
public abstract class CommonExecPythonCalc extends ExecNodeBase<RowData> {

    private static final String PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.scalar."
                    + "RowDataPythonScalarFunctionOperator";

    private static final String ARROW_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.scalar.arrow."
                    + "RowDataArrowPythonScalarFunctionOperator";

    private final RexProgram calcProgram;

    public CommonExecPythonCalc(
            RexProgram calcProgram, ExecEdge inputEdge, RowType outputType, String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.calcProgram = calcProgram;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);
        OneInputTransformation<RowData, RowData> ret =
                createPythonOneInputTransformation(
                        inputTransform,
                        calcProgram,
                        getDesc(),
                        CommonPythonUtil.getConfig(planner.getExecEnv(), planner.getTableConfig()));
        if (inputsContainSingleton()) {
            ret.setParallelism(1);
            ret.setMaxParallelism(1);
        }
        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(
                planner.getTableConfig().getConfiguration())) {
            ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }
        return ret;
    }

    private OneInputTransformation<RowData, RowData> createPythonOneInputTransformation(
            Transformation<RowData> inputTransform,
            RexProgram calcProgram,
            String name,
            Configuration config) {
        List<RexCall> pythonRexCalls =
                calcProgram.getProjectList().stream()
                        .map(calcProgram::expandLocalRef)
                        .filter(x -> x instanceof RexCall)
                        .map(x -> (RexCall) x)
                        .collect(Collectors.toList());

        List<Integer> forwardedFields =
                calcProgram.getProjectList().stream()
                        .map(calcProgram::expandLocalRef)
                        .filter(x -> x instanceof RexInputRef)
                        .map(x -> ((RexInputRef) x).getIndex())
                        .collect(Collectors.toList());

        Tuple2<int[], PythonFunctionInfo[]> extractResult =
                extractPythonScalarFunctionInfos(pythonRexCalls);
        int[] pythonUdfInputOffsets = extractResult.f0;
        PythonFunctionInfo[] pythonFunctionInfos = extractResult.f1;
        LogicalType[] inputLogicalTypes =
                ((InternalTypeInfo<RowData>) inputTransform.getOutputType()).toRowFieldTypes();
        InternalTypeInfo<RowData> pythonOperatorInputTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();

        List<LogicalType> forwardedFieldsLogicalTypes =
                forwardedFields.stream()
                        .map(i -> inputLogicalTypes[i])
                        .collect(Collectors.toList());
        List<LogicalType> pythonCallLogicalTypes =
                pythonRexCalls.stream()
                        .map(node -> FlinkTypeFactory.toLogicalType(node.getType()))
                        .collect(Collectors.toList());
        List<LogicalType> fieldsLogicalTypes = new ArrayList<>();
        fieldsLogicalTypes.addAll(forwardedFieldsLogicalTypes);
        fieldsLogicalTypes.addAll(pythonCallLogicalTypes);
        InternalTypeInfo<RowData> pythonOperatorResultTyeInfo =
                InternalTypeInfo.ofFields(fieldsLogicalTypes.toArray(new LogicalType[0]));
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonScalarFunctionOperator(
                        config,
                        pythonOperatorInputTypeInfo,
                        pythonOperatorResultTyeInfo,
                        pythonUdfInputOffsets,
                        pythonFunctionInfos,
                        forwardedFields.stream().mapToInt(x -> x).toArray(),
                        calcProgram.getExprList().stream()
                                .anyMatch(
                                        x ->
                                                PythonUtil.containsPythonCall(
                                                        x, PythonFunctionKind.PANDAS)));

        return new OneInputTransformation<>(
                inputTransform,
                name,
                pythonOperator,
                pythonOperatorResultTyeInfo,
                inputTransform.getParallelism());
    }

    private Tuple2<int[], PythonFunctionInfo[]> extractPythonScalarFunctionInfos(
            List<RexCall> rexCalls) {
        LinkedHashMap<RexNode, Integer> inputNodes = new LinkedHashMap<>();
        PythonFunctionInfo[] pythonFunctionInfos =
                rexCalls.stream()
                        .map(x -> CommonPythonUtil.createPythonFunctionInfo(x, inputNodes))
                        .collect(Collectors.toList())
                        .toArray(new PythonFunctionInfo[rexCalls.size()]);

        int[] udfInputOffsets =
                inputNodes.keySet().stream()
                        .map(
                                x -> {
                                    if (x instanceof RexInputRef) {
                                        return ((RexInputRef) x).getIndex();
                                    } else if (x instanceof RexFieldAccess) {
                                        return ((RexFieldAccess) x).getField().getIndex();
                                    }
                                    return null;
                                })
                        .mapToInt(i -> i)
                        .toArray();
        return Tuple2.of(udfInputOffsets, pythonFunctionInfos);
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonScalarFunctionOperator(
            Configuration config,
            InternalTypeInfo<RowData> inputRowTypeInfo,
            InternalTypeInfo<RowData> outputRowTypeInfo,
            int[] udfInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos,
            int[] forwardedFields,
            boolean isArrow) {
        Class clazz;
        if (isArrow) {
            clazz = CommonPythonUtil.loadClass(ARROW_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME);
        } else {
            clazz = CommonPythonUtil.loadClass(PYTHON_SCALAR_FUNCTION_OPERATOR_NAME);
        }

        try {
            Constructor ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            PythonFunctionInfo[].class,
                            RowType.class,
                            RowType.class,
                            int[].class,
                            int[].class);
            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            config,
                            pythonFunctionInfos,
                            inputRowTypeInfo.toRowType(),
                            outputRowTypeInfo.toRowType(),
                            udfInputOffsets,
                            forwardedFields);
        } catch (Exception e) {
            throw new TableException("Python Scalar Function Operator constructed failed.", e);
        }
    }
}
