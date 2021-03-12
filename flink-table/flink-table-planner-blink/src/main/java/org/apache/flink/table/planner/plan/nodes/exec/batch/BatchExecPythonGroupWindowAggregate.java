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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.agg.batch.WindowCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty;
import org.apache.flink.table.planner.expressions.PlannerRowtimeAttribute;
import org.apache.flink.table.planner.expressions.PlannerWindowEnd;
import org.apache.flink.table.planner.expressions.PlannerWindowProperty;
import org.apache.flink.table.planner.expressions.PlannerWindowStart;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;

/** Batch {@link ExecNode} for group widow aggregate (Python user defined aggregate function). */
public class BatchExecPythonGroupWindowAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final String ARROW_PYTHON_GROUP_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch."
                    + "BatchArrowPythonGroupWindowAggregateFunctionOperator";

    private final int[] grouping;
    private final int[] auxGrouping;
    private final AggregateCall[] aggCalls;
    private final LogicalWindow window;
    private final int inputTimeFieldIndex;
    private final PlannerNamedWindowProperty[] namedWindowProperties;

    public BatchExecPythonGroupWindowAggregate(
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            LogicalWindow window,
            int inputTimeFieldIndex,
            PlannerNamedWindowProperty[] namedWindowProperties,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.window = window;
        this.inputTimeFieldIndex = inputTimeFieldIndex;
        this.namedWindowProperties = namedWindowProperties;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final RowType outputRowType = InternalTypeInfo.of(getOutputType()).toRowType();

        final Tuple2<Long, Long> windowSizeAndSlideSize = WindowCodeGenerator.getWindowDef(window);
        final TableConfig tableConfig = planner.getTableConfig();
        final Configuration config =
                CommonPythonUtil.getMergedConfig(planner.getExecEnv(), tableConfig);
        int groupBufferLimitSize =
                config.getInteger(ExecutionConfigOptions.TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT);

        OneInputTransformation<RowData, RowData> transform =
                createPythonOneInputTransformation(
                        inputTransform,
                        inputRowType,
                        outputRowType,
                        groupBufferLimitSize,
                        windowSizeAndSlideSize.f0,
                        windowSizeAndSlideSize.f1,
                        config);
        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(config)) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }
        return transform;
    }

    private OneInputTransformation<RowData, RowData> createPythonOneInputTransformation(
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            RowType outputRowType,
            int maxLimitSize,
            long windowSize,
            long slideSize,
            Configuration config) {
        int[] namePropertyTypeArray =
                Arrays.stream(namedWindowProperties)
                        .mapToInt(
                                p -> {
                                    PlannerWindowProperty property = p.getProperty();
                                    if (property instanceof PlannerWindowStart) {
                                        return 0;
                                    }
                                    if (property instanceof PlannerWindowEnd) {
                                        return 1;
                                    }
                                    if (property instanceof PlannerRowtimeAttribute) {
                                        return 2;
                                    }
                                    throw new TableException("Unexpected property " + property);
                                })
                        .toArray();
        Tuple2<int[], PythonFunctionInfo[]> aggInfos =
                CommonPythonUtil.extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls);
        int[] pythonUdafInputOffsets = aggInfos.f0;
        PythonFunctionInfo[] pythonFunctionInfos = aggInfos.f1;
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonGroupWindowAggregateFunctionOperator(
                        config,
                        inputRowType,
                        outputRowType,
                        maxLimitSize,
                        windowSize,
                        slideSize,
                        namePropertyTypeArray,
                        pythonUdafInputOffsets,
                        pythonFunctionInfos);
        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                pythonOperator,
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism());
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonGroupWindowAggregateFunctionOperator(
            Configuration config,
            RowType inputRowType,
            RowType outputRowType,
            int maxLimitSize,
            long windowSize,
            long slideSize,
            int[] namePropertyTypeArray,
            int[] udafInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos) {
        Class<?> clazz =
                CommonPythonUtil.loadClass(
                        ARROW_PYTHON_GROUP_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME);
        try {
            Constructor<?> ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            PythonFunctionInfo[].class,
                            RowType.class,
                            RowType.class,
                            int.class,
                            int.class,
                            long.class,
                            long.class,
                            int[].class,
                            int[].class,
                            int[].class,
                            int[].class);
            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            config,
                            pythonFunctionInfos,
                            inputRowType,
                            outputRowType,
                            inputTimeFieldIndex,
                            maxLimitSize,
                            windowSize,
                            slideSize,
                            namePropertyTypeArray,
                            grouping,
                            auxGrouping,
                            udafInputOffsets);
        } catch (NoSuchMethodException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new TableException(
                    "Python BatchArrowPythonGroupWindowAggregateFunctionOperator constructed failed.",
                    e);
        }
    }
}
