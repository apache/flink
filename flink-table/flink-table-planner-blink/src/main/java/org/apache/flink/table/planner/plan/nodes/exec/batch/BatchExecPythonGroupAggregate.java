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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

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
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/** Batch [[ExecNode]] for Python unbounded group aggregate. */
public class BatchExecPythonGroupAggregate extends CommonExecPythonAggregate
        implements BatchExecNode<RowData> {

    private static final String ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch."
                    + "BatchArrowPythonGroupAggregateFunctionOperator";

    private final int[] grouping;
    private final int[] groupingSet;
    private final AggregateCall[] aggCalls;

    public BatchExecPythonGroupAggregate(
            int[] grouping,
            int[] groupingSet,
            AggregateCall[] aggCalls,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(inputEdge, outputType, description);
        this.grouping = grouping;
        this.groupingSet = groupingSet;
        this.aggCalls = aggCalls;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputNode.getOutputType();
        final RowType outputRowType = InternalTypeInfo.of(getOutputType()).toRowType();
        Configuration config =
                CommonPythonUtil.getMergedConfig(planner.getExecEnv(), planner.getTableConfig());
        OneInputTransformation<RowData, RowData> transform =
                createPythonOneInputTransformation(
                        inputTransform, inputRowType, outputRowType, config);

        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(config)) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }
        return transform;
    }

    @SuppressWarnings("unchecked")
    private OneInputTransformation<RowData, RowData> createPythonOneInputTransformation(
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            RowType outputRowType,
            Configuration config) {
        final Tuple2<int[], PythonFunctionInfo[]> aggInfos =
                extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls);
        int[] pythonUdafInputOffsets = aggInfos.f0;
        PythonFunctionInfo[] pythonFunctionInfos = aggInfos.f1;
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonAggregateFunctionOperator(
                        config,
                        inputRowType,
                        outputRowType,
                        pythonUdafInputOffsets,
                        pythonFunctionInfos);
        return new OneInputTransformation(
                inputTransform,
                getDesc(),
                pythonOperator,
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism());
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonAggregateFunctionOperator(
            Configuration config,
            RowType inputRowType,
            RowType outputRowType,
            int[] udafInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos) {
        final Class clazz =
                CommonPythonUtil.loadClass(ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME);
        try {
            Constructor ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            PythonFunctionInfo[].class,
                            RowType.class,
                            RowType.class,
                            int[].class,
                            int[].class,
                            int[].class);
            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            config,
                            pythonFunctionInfos,
                            inputRowType,
                            outputRowType,
                            grouping,
                            groupingSet,
                            udafInputOffsets);
        } catch (NoSuchMethodException
                | IllegalAccessException
                | InstantiationException
                | InvocationTargetException e) {
            throw new TableException(
                    "Python BatchArrowPythonGroupAggregateFunctionOperator constructed failed.", e);
        }
    }
}
