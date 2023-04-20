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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

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
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for python time-based over operator. */
@ExecNodeMetadata(
        name = "stream-exec-python-over-aggregate",
        version = 1,
        producedTransformations =
                StreamExecPythonOverAggregate.PYTHON_OVER_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_16,
        minStateVersion = FlinkVersion.v1_16)
public class StreamExecPythonOverAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecPythonOverAggregate.class);

    public static final String PYTHON_OVER_AGGREGATE_TRANSFORMATION = "python-over-aggregate";

    public static final String FIELD_NAME_OVER_SPEC = "overSpec";

    private static final String
            ARROW_PYTHON_OVER_WINDOW_RANGE_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME =
                    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream."
                            + "StreamArrowPythonRowTimeBoundedRangeOperator";
    private static final String
            ARROW_PYTHON_OVER_WINDOW_RANGE_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME =
                    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream."
                            + "StreamArrowPythonProcTimeBoundedRangeOperator";
    private static final String
            ARROW_PYTHON_OVER_WINDOW_ROWS_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME =
                    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream."
                            + "StreamArrowPythonRowTimeBoundedRowsOperator";
    private static final String
            ARROW_PYTHON_OVER_WINDOW_ROWS_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME =
                    "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream."
                            + "StreamArrowPythonProcTimeBoundedRowsOperator";

    @JsonProperty(FIELD_NAME_OVER_SPEC)
    private final OverSpec overSpec;

    public StreamExecPythonOverAggregate(
            ReadableConfig tableConfig,
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecPythonOverAggregate.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecPythonOverAggregate.class, tableConfig),
                overSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecPythonOverAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_OVER_SPEC) OverSpec overSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.overSpec = checkNotNull(overSpec);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (overSpec.getGroups().size() > 1) {
            throw new TableException("All aggregates must be computed on the same window.");
        }

        final OverSpec.GroupSpec group = overSpec.getGroups().get(0);
        final int[] orderKeys = group.getSort().getFieldIndices();
        final boolean[] isAscendingOrders = group.getSort().getAscendingOrders();
        if (orderKeys.length != 1 || isAscendingOrders.length != 1) {
            throw new TableException("The window can only be ordered by a single time column.");
        }

        if (!isAscendingOrders[0]) {
            throw new TableException("The window can only be ordered in ASCENDING mode.");
        }

        final int[] partitionKeys = overSpec.getPartition().getFieldIndices();
        if (partitionKeys.length > 0 && config.getStateRetentionTime() < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent "
                            + "excessive state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final int orderKey = orderKeys[0];
        final LogicalType orderKeyType = inputRowType.getFields().get(orderKey).getType();
        // check time field && identify window rowtime attribute
        final int rowTimeIdx;
        if (isRowtimeAttribute(orderKeyType)) {
            rowTimeIdx = orderKey;
        } else if (isProctimeAttribute(orderKeyType)) {
            rowTimeIdx = -1;
        } else {
            throw new TableException(
                    "OVER windows' ordering in stream mode must be defined on a time attribute.");
        }
        if (group.getLowerBound().isPreceding() && group.getLowerBound().isUnbounded()) {
            throw new TableException(
                    "Python UDAF is not supported to be used in UNBOUNDED PRECEDING OVER windows.");
        } else if (!group.getUpperBound().isCurrentRow()) {
            throw new TableException(
                    "Python UDAF is not supported to be used in UNBOUNDED FOLLOWING OVER windows.");
        }
        Object boundValue = OverAggregateUtil.getBoundary(overSpec, group.getLowerBound());
        if (boundValue instanceof BigDecimal) {
            throw new TableException(
                    "the specific value is decimal which haven not supported yet.");
        }
        long precedingOffset = -1 * (long) boundValue;
        Configuration pythonConfig =
                CommonPythonUtil.extractPythonConfiguration(
                        planner.getExecEnv(), config, planner.getFlinkContext().getClassLoader());
        OneInputTransformation<RowData, RowData> transform =
                createPythonOneInputTransformation(
                        inputTransform,
                        inputRowType,
                        InternalTypeInfo.of(getOutputType()).toRowType(),
                        rowTimeIdx,
                        group.getAggCalls().toArray(new AggregateCall[0]),
                        precedingOffset,
                        group.isRows(),
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        pythonConfig,
                        config,
                        planner.getFlinkContext().getClassLoader());

        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(
                pythonConfig, planner.getFlinkContext().getClassLoader())) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        partitionKeys,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }

    private OneInputTransformation<RowData, RowData> createPythonOneInputTransformation(
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            RowType outputRowType,
            int rowTimeIdx,
            AggregateCall[] aggCalls,
            long lowerBoundary,
            boolean isRowsClause,
            long minIdleStateRetentionTime,
            long maxIdleStateRetentionTime,
            Configuration pythonConfig,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        Tuple2<int[], PythonFunctionInfo[]> aggCallInfos =
                CommonPythonUtil.extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls);
        int[] pythonUdafInputOffsets = aggCallInfos.f0;
        PythonFunctionInfo[] pythonFunctionInfos = aggCallInfos.f1;
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonOverWindowAggregateFunctionOperator(
                        config,
                        classLoader,
                        pythonConfig,
                        inputRowType,
                        outputRowType,
                        rowTimeIdx,
                        lowerBoundary,
                        isRowsClause,
                        pythonUdafInputOffsets,
                        pythonFunctionInfos,
                        minIdleStateRetentionTime,
                        maxIdleStateRetentionTime);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(PYTHON_OVER_AGGREGATE_TRANSFORMATION, config),
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
            int rowTiemIdx,
            long lowerBoundary,
            boolean isRowsClause,
            int[] udafInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos,
            long minIdleStateRetentionTime,
            long maxIdleStateRetentionTime) {

        RowType userDefinedFunctionInputType =
                (RowType) Projection.of(udafInputOffsets).project(inputRowType);
        RowType userDefinedFunctionOutputType =
                (RowType)
                        Projection.range(
                                        inputRowType.getFieldCount(), outputRowType.getFieldCount())
                                .project(outputRowType);
        GeneratedProjection generatedProjection =
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(config, classLoader),
                        "UdafInputProjection",
                        inputRowType,
                        userDefinedFunctionInputType,
                        udafInputOffsets);

        if (isRowsClause) {
            String className;
            if (rowTiemIdx != -1) {
                className = ARROW_PYTHON_OVER_WINDOW_ROWS_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME;
            } else {
                className =
                        ARROW_PYTHON_OVER_WINDOW_ROWS_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME;
            }
            Class<?> clazz = CommonPythonUtil.loadClass(className, classLoader);

            try {
                Constructor<?> ctor =
                        clazz.getConstructor(
                                Configuration.class,
                                long.class,
                                long.class,
                                PythonFunctionInfo[].class,
                                RowType.class,
                                RowType.class,
                                RowType.class,
                                int.class,
                                long.class,
                                GeneratedProjection.class);
                return (OneInputStreamOperator<RowData, RowData>)
                        ctor.newInstance(
                                pythonConfig,
                                minIdleStateRetentionTime,
                                maxIdleStateRetentionTime,
                                pythonFunctionInfos,
                                inputRowType,
                                userDefinedFunctionInputType,
                                userDefinedFunctionOutputType,
                                rowTiemIdx,
                                lowerBoundary,
                                generatedProjection);
            } catch (NoSuchMethodException
                    | InstantiationException
                    | IllegalAccessException
                    | InvocationTargetException e) {
                throw new TableException(
                        "Python Arrow Over Rows Window Function Operator constructed failed.", e);
            }
        } else {
            String className;
            if (rowTiemIdx != -1) {
                className =
                        ARROW_PYTHON_OVER_WINDOW_RANGE_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME;
            } else {
                className =
                        ARROW_PYTHON_OVER_WINDOW_RANGE_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME;
            }
            Class<?> clazz = CommonPythonUtil.loadClass(className, classLoader);
            try {
                Constructor<?> ctor =
                        clazz.getConstructor(
                                Configuration.class,
                                PythonFunctionInfo[].class,
                                RowType.class,
                                RowType.class,
                                RowType.class,
                                int.class,
                                long.class,
                                GeneratedProjection.class);
                return (OneInputStreamOperator<RowData, RowData>)
                        ctor.newInstance(
                                pythonConfig,
                                pythonFunctionInfos,
                                inputRowType,
                                userDefinedFunctionInputType,
                                userDefinedFunctionOutputType,
                                rowTiemIdx,
                                lowerBoundary,
                                generatedProjection);
            } catch (NoSuchMethodException
                    | InstantiationException
                    | IllegalAccessException
                    | InvocationTargetException e) {
                throw new TableException(
                        "Python Arrow Over Range Window Function Operator constructed failed.", e);
            }
        }
    }
}
