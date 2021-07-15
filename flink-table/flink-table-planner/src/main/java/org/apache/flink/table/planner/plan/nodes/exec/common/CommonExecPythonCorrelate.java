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
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Base {@link ExecNode} which matches along with join a Python user defined table function. */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class CommonExecPythonCorrelate extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_FUNCTION_CALL = "functionCall";

    private static final String PYTHON_TABLE_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.table.PythonTableFunctionOperator";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    @JsonProperty(FIELD_NAME_FUNCTION_CALL)
    private final RexCall invocation;

    public CommonExecPythonCorrelate(
            FlinkJoinType joinType,
            RexCall invocation,
            int id,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.joinType = joinType;
        this.invocation = invocation;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final Configuration config =
                CommonPythonUtil.getMergedConfig(planner.getExecEnv(), planner.getTableConfig());
        OneInputTransformation<RowData, RowData> transform =
                createPythonOneInputTransformation(inputTransform, config);
        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(config)) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }
        return transform;
    }

    private OneInputTransformation<RowData, RowData> createPythonOneInputTransformation(
            Transformation<RowData> inputTransform, Configuration config) {
        Tuple2<int[], PythonFunctionInfo> extractResult = extractPythonTableFunctionInfo();
        int[] pythonUdtfInputOffsets = extractResult.f0;
        PythonFunctionInfo pythonFunctionInfo = extractResult.f1;
        InternalTypeInfo<RowData> pythonOperatorInputRowType =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        InternalTypeInfo<RowData> pythonOperatorOutputRowType =
                InternalTypeInfo.of((RowType) getOutputType());
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonTableFunctionOperator(
                        config,
                        pythonOperatorInputRowType,
                        pythonOperatorOutputRowType,
                        pythonFunctionInfo,
                        pythonUdtfInputOffsets);
        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                pythonOperator,
                pythonOperatorOutputRowType,
                inputTransform.getParallelism());
    }

    private Tuple2<int[], PythonFunctionInfo> extractPythonTableFunctionInfo() {
        LinkedHashMap<RexNode, Integer> inputNodes = new LinkedHashMap<>();
        PythonFunctionInfo pythonTableFunctionInfo =
                CommonPythonUtil.createPythonFunctionInfo(invocation, inputNodes);
        int[] udtfInputOffsets =
                inputNodes.keySet().stream()
                        .filter(x -> x instanceof RexInputRef)
                        .map(x -> ((RexInputRef) x).getIndex())
                        .mapToInt(i -> i)
                        .toArray();
        return Tuple2.of(udtfInputOffsets, pythonTableFunctionInfo);
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonTableFunctionOperator(
            Configuration config,
            InternalTypeInfo<RowData> inputRowType,
            InternalTypeInfo<RowData> outputRowType,
            PythonFunctionInfo pythonFunctionInfo,
            int[] udtfInputOffsets) {
        Class clazz = CommonPythonUtil.loadClass(PYTHON_TABLE_FUNCTION_OPERATOR_NAME);
        try {
            Constructor ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            PythonFunctionInfo.class,
                            RowType.class,
                            RowType.class,
                            int[].class,
                            FlinkJoinType.class);
            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            config,
                            pythonFunctionInfo,
                            inputRowType.toRowType(),
                            outputRowType.toRowType(),
                            udtfInputOffsets,
                            joinType);
        } catch (Exception e) {
            throw new TableException("Python Table Function Operator constructed failed.", e);
        }
    }
}
