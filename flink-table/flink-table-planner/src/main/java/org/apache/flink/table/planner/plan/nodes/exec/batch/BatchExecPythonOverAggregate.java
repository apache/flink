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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexWindowBound;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Batch {@link ExecNode} for sort-based over window aggregate (Python user defined aggregate
 * function).
 */
public class BatchExecPythonOverAggregate extends BatchExecOverAggregateBase {

    private static final String ARROW_PYTHON_OVER_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch."
                    + "BatchArrowPythonOverWindowAggregateFunctionOperator";

    private final List<Long> lowerBoundary;
    private final List<Long> upperBoundary;
    private final List<AggregateCall> aggCalls;
    private final List<Integer> aggWindowIndex;

    public BatchExecPythonOverAggregate(
            ReadableConfig tableConfig,
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecPythonOverAggregate.class),
                ExecNodeContext.newPersistedConfig(BatchExecPythonOverAggregate.class, tableConfig),
                overSpec,
                inputProperty,
                outputType,
                description);
        lowerBoundary = new ArrayList<>();
        upperBoundary = new ArrayList<>();
        aggCalls = new ArrayList<>();
        aggWindowIndex = new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputType = (RowType) inputEdge.getOutputType();
        final FlinkRelBuilder relBuilder = planner.createRelBuilder();
        final ClassLoader classLoader = planner.getFlinkContext().getClassLoader();

        List<OverSpec.GroupSpec> groups = overSpec.getGroups();
        SortSpec sortSpec = groups.get(groups.size() - 1).getSort();
        boolean[] isRangeWindows = new boolean[groups.size()];
        GeneratedRecordComparator[] lowerBoundComparators =
                new GeneratedRecordComparator[groups.size()];
        GeneratedRecordComparator[] upperBoundComparators =
                new GeneratedRecordComparator[groups.size()];
        for (int i = 0; i < groups.size(); i++) {
            OverSpec.GroupSpec group = groups.get(i);
            List<AggregateCall> groupAggCalls = group.getAggCalls();
            aggCalls.addAll(groupAggCalls);
            for (int j = 0; j < groupAggCalls.size(); j++) {
                aggWindowIndex.add(i);
            }
            OverWindowMode mode = inferGroupMode(group);
            if (mode == OverWindowMode.ROW) {
                isRangeWindows[i] = false;
                if (isUnboundedWindow(group)) {
                    lowerBoundary.add(Long.MIN_VALUE);
                    upperBoundary.add(Long.MAX_VALUE);
                } else if (isUnboundedPrecedingWindow(group)) {
                    lowerBoundary.add(Long.MIN_VALUE);
                    upperBoundary.add(
                            OverAggregateUtil.getLongBoundary(overSpec, group.getUpperBound()));
                } else if (isUnboundedFollowingWindow(group)) {
                    lowerBoundary.add(
                            OverAggregateUtil.getLongBoundary(overSpec, group.getLowerBound()));
                    upperBoundary.add(Long.MAX_VALUE);
                } else if (isSlidingWindow(group)) {
                    lowerBoundary.add(
                            OverAggregateUtil.getLongBoundary(overSpec, group.getLowerBound()));
                    upperBoundary.add(
                            OverAggregateUtil.getLongBoundary(overSpec, group.getUpperBound()));
                } else {
                    throw new TableException("Unsupported row window group spec " + group);
                }
            } else {
                isRangeWindows[i] = true;
                // A RANGE bound is applied by its comparator, which compares the ORDER BY key by
                // its type. The boundary only tells the operator whether that side is unbounded.
                RexWindowBound lowerBound = group.getLowerBound();
                RexWindowBound upperBound = group.getUpperBound();
                lowerBoundary.add(lowerBound.isUnbounded() ? Long.MIN_VALUE : 0L);
                upperBoundary.add(upperBound.isUnbounded() ? Long.MAX_VALUE : 0L);
                if (!lowerBound.isUnbounded()) {
                    lowerBoundComparators[i] =
                            BatchExecOverAggregate.createBoundComparator(
                                    relBuilder,
                                    config,
                                    classLoader,
                                    overSpec,
                                    sortSpec,
                                    lowerBound,
                                    true,
                                    inputType);
                }
                if (!upperBound.isUnbounded()) {
                    upperBoundComparators[i] =
                            BatchExecOverAggregate.createBoundComparator(
                                    relBuilder,
                                    config,
                                    classLoader,
                                    overSpec,
                                    sortSpec,
                                    upperBound,
                                    false,
                                    inputType);
                }
            }
        }
        Configuration pythonConfig =
                CommonPythonUtil.extractPythonConfiguration(
                        planner.getTableConfig(), planner.getFlinkContext().getClassLoader());
        OneInputTransformation<RowData, RowData> transform =
                createPythonOneInputTransformation(
                        inputTransform,
                        inputType,
                        InternalTypeInfo.of(getOutputType()).toRowType(),
                        isRangeWindows,
                        lowerBoundComparators,
                        upperBoundComparators,
                        pythonConfig,
                        config,
                        planner.getFlinkContext().getClassLoader());
        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(
                pythonConfig, planner.getFlinkContext().getClassLoader())) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }
        return transform;
    }

    private OneInputTransformation<RowData, RowData> createPythonOneInputTransformation(
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            RowType outputRowType,
            boolean[] isRangeWindows,
            GeneratedRecordComparator[] lowerBoundComparators,
            GeneratedRecordComparator[] upperBoundComparators,
            Configuration pythonConfig,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        Tuple2<int[], PythonFunctionInfo[]> aggCallInfos =
                CommonPythonUtil.extractPythonAggregateFunctionInfosFromAggregateCall(
                        aggCalls.toArray(new AggregateCall[0]));
        int[] pythonUdafInputOffsets = aggCallInfos.f0;
        PythonFunctionInfo[] pythonFunctionInfos = aggCallInfos.f1;
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonOverWindowAggregateFunctionOperator(
                        config,
                        classLoader,
                        pythonConfig,
                        inputRowType,
                        outputRowType,
                        isRangeWindows,
                        lowerBoundComparators,
                        upperBoundComparators,
                        pythonUdafInputOffsets,
                        pythonFunctionInfos);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationName(pythonConfig),
                createTransformationDescription(pythonConfig),
                pythonOperator,
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism(),
                false);
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonOverWindowAggregateFunctionOperator(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonConfig,
            RowType inputRowType,
            RowType outputRowType,
            boolean[] isRangeWindows,
            GeneratedRecordComparator[] lowerBoundComparators,
            GeneratedRecordComparator[] upperBoundComparators,
            int[] udafInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos) {
        Class<?> clazz =
                CommonPythonUtil.loadClass(
                        ARROW_PYTHON_OVER_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME, classLoader);

        RowType udfInputType = (RowType) Projection.of(udafInputOffsets).project(inputRowType);
        RowType udfOutputType =
                (RowType)
                        Projection.range(
                                        inputRowType.getFieldCount(), outputRowType.getFieldCount())
                                .project(outputRowType);

        PartitionSpec partitionSpec = overSpec.getPartition();

        try {
            Constructor<?> ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            PythonFunctionInfo[].class,
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            long[].class,
                            long[].class,
                            boolean[].class,
                            int[].class,
                            GeneratedRecordComparator[].class,
                            GeneratedRecordComparator[].class,
                            GeneratedProjection.class,
                            GeneratedProjection.class,
                            GeneratedProjection.class);
            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            pythonConfig,
                            pythonFunctionInfos,
                            inputRowType,
                            udfInputType,
                            udfOutputType,
                            lowerBoundary.stream().mapToLong(i -> i).toArray(),
                            upperBoundary.stream().mapToLong(i -> i).toArray(),
                            isRangeWindows,
                            aggWindowIndex.stream().mapToInt(i -> i).toArray(),
                            lowerBoundComparators,
                            upperBoundComparators,
                            ProjectionCodeGenerator.generateProjection(
                                    new CodeGeneratorContext(config, classLoader),
                                    "UdafInputProjection",
                                    inputRowType,
                                    udfInputType,
                                    udafInputOffsets),
                            ProjectionCodeGenerator.generateProjection(
                                    new CodeGeneratorContext(config, classLoader),
                                    "GroupKey",
                                    inputRowType,
                                    (RowType)
                                            Projection.of(partitionSpec.getFieldIndices())
                                                    .project(inputRowType),
                                    partitionSpec.getFieldIndices()),
                            ProjectionCodeGenerator.generateProjection(
                                    new CodeGeneratorContext(config, classLoader),
                                    "GroupSet",
                                    inputRowType,
                                    (RowType)
                                            Projection.of(partitionSpec.getFieldIndices())
                                                    .project(inputRowType),
                                    partitionSpec.getFieldIndices()));
        } catch (NoSuchMethodException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new TableException(
                    "Python BatchArrowPythonOverWindowAggregateFunctionOperator constructed failed.",
                    e);
        }
    }
}
