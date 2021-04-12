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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.LinkedHashMap;

/** Base {@link ExecNode} which matches along with join a Python user defined table function. */
public abstract class CommonExecPythonCorrelate extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {
    private static final String PYTHON_TABLE_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.table.RowDataPythonTableFunctionOperator";

    private final FlinkJoinType joinType;
    private final RexCall invocation;

    public CommonExecPythonCorrelate(
            FlinkJoinType joinType,
            RexCall invocation,
            RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.joinType = joinType;
        this.invocation = invocation;
        if (joinType == FlinkJoinType.LEFT && condition != null) {
            throw new TableException(
                    "Currently Python correlate does not support conditions in left join.");
        }
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
