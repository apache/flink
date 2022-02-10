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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

/** Batch {@link ExecNode} for Python unbounded group aggregate. */
public class BatchExecPythonGroupAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final String ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch."
                    + "BatchArrowPythonGroupAggregateFunctionOperator";

    private final int[] grouping;
    private final int[] auxGrouping;
    private final AggregateCall[] aggCalls;

    public BatchExecPythonGroupAggregate(
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecPythonGroupAggregate.class),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final RowType outputRowType = InternalTypeInfo.of(getOutputType()).toRowType();
        Configuration config =
                CommonPythonUtil.getMergedConfig(planner.getExecEnv(), planner.getTableConfig());
        OneInputTransformation<RowData, RowData> transform =
                createPythonOneInputTransformation(
                        inputTransform,
                        inputRowType,
                        outputRowType,
                        config,
                        planner.getTableConfig());

        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(config)) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }
        return transform;
    }

    private OneInputTransformation<RowData, RowData> createPythonOneInputTransformation(
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            RowType outputRowType,
            Configuration mergedConfig,
            TableConfig tableConfig) {
        final Tuple2<int[], PythonFunctionInfo[]> aggInfos =
                CommonPythonUtil.extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls);
        int[] pythonUdafInputOffsets = aggInfos.f0;
        PythonFunctionInfo[] pythonFunctionInfos = aggInfos.f1;
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonAggregateFunctionOperator(
                        tableConfig,
                        mergedConfig,
                        inputRowType,
                        outputRowType,
                        pythonUdafInputOffsets,
                        pythonFunctionInfos);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationName(mergedConfig),
                createTransformationDescription(mergedConfig),
                pythonOperator,
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism());
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonAggregateFunctionOperator(
            TableConfig tableConfig,
            Configuration mergedConfig,
            RowType inputRowType,
            RowType outputRowType,
            int[] udafInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos) {
        final Class<?> clazz =
                CommonPythonUtil.loadClass(ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME);

        RowType udfInputType = (RowType) Projection.of(udafInputOffsets).project(inputRowType);
        RowType udfOutputType =
                (RowType)
                        Projection.range(auxGrouping.length, outputRowType.getFieldCount())
                                .project(outputRowType);

        try {
            Constructor<?> ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            PythonFunctionInfo[].class,
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            GeneratedProjection.class,
                            GeneratedProjection.class,
                            GeneratedProjection.class);
            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            mergedConfig,
                            pythonFunctionInfos,
                            inputRowType,
                            udfInputType,
                            udfOutputType,
                            ProjectionCodeGenerator.generateProjection(
                                    CodeGeneratorContext.apply(tableConfig),
                                    "UdafInputProjection",
                                    inputRowType,
                                    udfInputType,
                                    udafInputOffsets),
                            ProjectionCodeGenerator.generateProjection(
                                    CodeGeneratorContext.apply(tableConfig),
                                    "GroupKey",
                                    inputRowType,
                                    (RowType) Projection.of(grouping).project(inputRowType),
                                    grouping),
                            ProjectionCodeGenerator.generateProjection(
                                    CodeGeneratorContext.apply(tableConfig),
                                    "GroupSet",
                                    inputRowType,
                                    (RowType) Projection.of(auxGrouping).project(inputRowType),
                                    auxGrouping));
        } catch (NoSuchMethodException
                | IllegalAccessException
                | InstantiationException
                | InvocationTargetException e) {
            throw new TableException(
                    "Python BatchArrowPythonGroupAggregateFunctionOperator constructed failed.", e);
        }
    }
}
