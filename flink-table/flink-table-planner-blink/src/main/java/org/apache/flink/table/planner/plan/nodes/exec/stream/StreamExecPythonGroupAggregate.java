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
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;

/** Stream {@link ExecNode} for Python unbounded group aggregate. */
public class StreamExecPythonGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecPythonGroupAggregate.class);
    private static final String PYTHON_STREAM_AGGREAGTE_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.aggregate.PythonStreamGroupAggregateOperator";

    private final int[] grouping;
    private final AggregateCall[] aggCalls;
    private final boolean[] aggCallNeedRetractions;
    private final boolean generateUpdateBefore;
    private final boolean needRetraction;

    public StreamExecPythonGroupAggregate(
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            boolean generateUpdateBefore,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.grouping = grouping;
        this.aggCalls = aggCalls;
        this.aggCallNeedRetractions = aggCallNeedRetractions;
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        TableConfig tableConfig = planner.getTableConfig();

        if (grouping.length > 0 && tableConfig.getMinIdleStateRetentionTime() < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval "
                            + "to prevent excessive state size. You may specify a retention time "
                            + "of 0 to not clean up the state.");
        }
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo
        final int inputCountIndex = aggInfoList.getIndexOfCountStar();
        final boolean countStarInserted = aggInfoList.countStarInserted();
        Tuple2<PythonAggregateFunctionInfo[], DataViewUtils.DataViewSpec[][]>
                aggInfosAndDataViewSpecs =
                        CommonPythonUtil.extractPythonAggregateFunctionInfos(aggInfoList, aggCalls);
        PythonAggregateFunctionInfo[] pythonFunctionInfos = aggInfosAndDataViewSpecs.f0;
        DataViewUtils.DataViewSpec[][] dataViewSpecs = aggInfosAndDataViewSpecs.f1;
        Configuration config = CommonPythonUtil.getMergedConfig(planner.getExecEnv(), tableConfig);
        final OneInputStreamOperator<RowData, RowData> operator =
                getPythonAggregateFunctionOperator(
                        config,
                        inputRowType,
                        InternalTypeInfo.of(getOutputType()).toRowType(),
                        pythonFunctionInfos,
                        dataViewSpecs,
                        tableConfig.getMinIdleStateRetentionTime(),
                        tableConfig.getMaxIdleStateRetentionTime(),
                        inputCountIndex,
                        countStarInserted);
        // partitioned aggregation
        OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDescription(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(config)) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(grouping, InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonAggregateFunctionOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewUtils.DataViewSpec[][] dataViewSpecs,
            long minIdleStateRetentionTime,
            long maxIdleStateRetentionTime,
            int indexOfCountStar,
            boolean countStarInserted) {
        Class<?> clazz = CommonPythonUtil.loadClass(PYTHON_STREAM_AGGREAGTE_OPERATOR_NAME);
        try {
            Constructor<?> ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            RowType.class,
                            RowType.class,
                            PythonAggregateFunctionInfo[].class,
                            DataViewUtils.DataViewSpec[][].class,
                            int[].class,
                            int.class,
                            boolean.class,
                            boolean.class,
                            long.class,
                            long.class);
            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            config,
                            inputType,
                            outputType,
                            aggregateFunctions,
                            dataViewSpecs,
                            grouping,
                            indexOfCountStar,
                            countStarInserted,
                            generateUpdateBefore,
                            minIdleStateRetentionTime,
                            maxIdleStateRetentionTime);
        } catch (Exception e) {
            throw new TableException(
                    "Python Stream Aggregate Function Operator constructed failed.", e);
        }
    }
}
