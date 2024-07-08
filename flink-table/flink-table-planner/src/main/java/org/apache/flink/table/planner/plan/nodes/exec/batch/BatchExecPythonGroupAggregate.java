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

import org.apache.flink.FlinkVersion;
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
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_AGG_CALLS;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_AUX_GROUPING;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_GROUPING;

/** Batch {@link ExecNode} for Python unbounded group aggregate. */
@ExecNodeMetadata(
        name = "batch-exec-python-group-aggregate",
        version = 1,
        minPlanVersion = FlinkVersion.v1_20,
        minStateVersion = FlinkVersion.v1_20)
public class BatchExecPythonGroupAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final String ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch."
                    + "BatchArrowPythonGroupAggregateFunctionOperator";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AUX_GROUPING)
    private final int[] auxGrouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    public static final String FIELD_NAME_MATCH_SPEC = "matchSpec";

    public BatchExecPythonGroupAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecPythonGroupAggregate.class),
                ExecNodeContext.newPersistedConfig(
                        BatchExecPythonGroupAggregate.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
    }

    @JsonCreator
    public BatchExecPythonGroupAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            // TODO: Wrong imports?
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AUX_GROUPING) int[] auxGrouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTY) InputProperty inputProperty,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final RowType outputRowType = InternalTypeInfo.of(getOutputType()).toRowType();
        Configuration pythonConfig =
                CommonPythonUtil.extractPythonConfiguration(
                        planner.getTableConfig(), planner.getFlinkContext().getClassLoader());
        OneInputTransformation<RowData, RowData> transform =
                createPythonOneInputTransformation(
                        inputTransform,
                        inputRowType,
                        outputRowType,
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
            Configuration pythonConfig,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        final Tuple2<int[], PythonFunctionInfo[]> aggInfos =
                CommonPythonUtil.extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls);
        int[] pythonUdafInputOffsets = aggInfos.f0;
        PythonFunctionInfo[] pythonFunctionInfos = aggInfos.f1;
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonAggregateFunctionOperator(
                        config,
                        classLoader,
                        pythonConfig,
                        inputRowType,
                        outputRowType,
                        pythonUdafInputOffsets,
                        pythonFunctionInfos);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                pythonOperator,
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism(),
                false);
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonAggregateFunctionOperator(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonConfig,
            RowType inputRowType,
            RowType outputRowType,
            int[] udafInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos) {
        final Class<?> clazz =
                CommonPythonUtil.loadClass(
                        ARROW_PYTHON_AGGREGATE_FUNCTION_OPERATOR_NAME, classLoader);

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
                            pythonConfig,
                            pythonFunctionInfos,
                            inputRowType,
                            udfInputType,
                            udfOutputType,
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
                                    (RowType) Projection.of(grouping).project(inputRowType),
                                    grouping),
                            ProjectionCodeGenerator.generateProjection(
                                    new CodeGeneratorContext(config, classLoader),
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
