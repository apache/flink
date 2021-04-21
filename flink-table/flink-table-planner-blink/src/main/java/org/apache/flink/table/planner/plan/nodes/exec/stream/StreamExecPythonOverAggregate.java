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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Collections;

/** Stream {@link ExecNode} for python time-based over operator. */
public class StreamExecPythonOverAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(StreamExecPythonOverAggregate.class);

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

    private final OverSpec overSpec;

    public StreamExecPythonOverAggregate(
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.overSpec = overSpec;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
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
        final TableConfig tableConfig = planner.getTableConfig();
        if (partitionKeys.length > 0 && tableConfig.getMinIdleStateRetentionTime() < 0) {
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
        if (orderKeyType instanceof TimestampType
                && ((TimestampType) orderKeyType).getKind() == TimestampKind.ROWTIME) {
            rowTimeIdx = orderKey;
        } else if (orderKeyType instanceof LocalZonedTimestampType
                && ((LocalZonedTimestampType) orderKeyType).getKind() == TimestampKind.PROCTIME) {
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
        Configuration config = CommonPythonUtil.getMergedConfig(planner.getExecEnv(), tableConfig);
        OneInputTransformation<RowData, RowData> transform =
                createPythonOneInputTransformation(
                        inputTransform,
                        inputRowType,
                        InternalTypeInfo.of(getOutputType()).toRowType(),
                        rowTimeIdx,
                        group.getAggCalls().toArray(new AggregateCall[0]),
                        precedingOffset,
                        group.isRows(),
                        partitionKeys,
                        tableConfig.getMinIdleStateRetentionTime(),
                        tableConfig.getMaxIdleStateRetentionTime(),
                        config);

        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(config)) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        partitionKeys, InternalTypeInfo.of(inputRowType));
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
            int[] grouping,
            long minIdleStateRetentionTime,
            long maxIdleStateRetentionTime,
            Configuration config) {
        Tuple2<int[], PythonFunctionInfo[]> aggCallInfos =
                CommonPythonUtil.extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls);
        int[] pythonUdafInputOffsets = aggCallInfos.f0;
        PythonFunctionInfo[] pythonFunctionInfos = aggCallInfos.f1;
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonOverWindowAggregateFunctionOperator(
                        config,
                        inputRowType,
                        outputRowType,
                        rowTimeIdx,
                        lowerBoundary,
                        isRowsClause,
                        grouping,
                        pythonUdafInputOffsets,
                        pythonFunctionInfos,
                        minIdleStateRetentionTime,
                        maxIdleStateRetentionTime);

        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                pythonOperator,
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism());
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonOverWindowAggregateFunctionOperator(
            Configuration config,
            RowType inputRowType,
            RowType outputRowType,
            int rowTiemIdx,
            long lowerBoundary,
            boolean isRowsClause,
            int[] grouping,
            int[] udafInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos,
            long minIdleStateRetentionTime,
            long maxIdleStateRetentionTime) {
        if (isRowsClause) {
            String className;
            if (rowTiemIdx != -1) {
                className = ARROW_PYTHON_OVER_WINDOW_ROWS_ROW_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME;
            } else {
                className =
                        ARROW_PYTHON_OVER_WINDOW_ROWS_PROC_TIME_AGGREGATE_FUNCTION_OPERATOR_NAME;
            }
            Class<?> clazz = CommonPythonUtil.loadClass(className);
            try {
                Constructor<?> ctor =
                        clazz.getConstructor(
                                Configuration.class,
                                long.class,
                                long.class,
                                PythonFunctionInfo[].class,
                                RowType.class,
                                RowType.class,
                                int.class,
                                long.class,
                                int[].class,
                                int[].class);
                return (OneInputStreamOperator<RowData, RowData>)
                        ctor.newInstance(
                                config,
                                minIdleStateRetentionTime,
                                maxIdleStateRetentionTime,
                                pythonFunctionInfos,
                                inputRowType,
                                outputRowType,
                                rowTiemIdx,
                                lowerBoundary,
                                grouping,
                                udafInputOffsets);
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
            Class<?> clazz = CommonPythonUtil.loadClass(className);
            try {
                Constructor<?> ctor =
                        clazz.getConstructor(
                                Configuration.class,
                                PythonFunctionInfo[].class,
                                RowType.class,
                                RowType.class,
                                int.class,
                                long.class,
                                int[].class,
                                int[].class);
                return (OneInputStreamOperator<RowData, RowData>)
                        ctor.newInstance(
                                config,
                                pythonFunctionInfos,
                                inputRowType,
                                outputRowType,
                                rowTiemIdx,
                                lowerBoundary,
                                grouping,
                                udafInputOffsets);
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
