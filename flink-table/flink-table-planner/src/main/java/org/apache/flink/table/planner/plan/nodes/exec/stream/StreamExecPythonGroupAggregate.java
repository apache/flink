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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.dataview.DataViewSpec;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for Python unbounded group aggregate. */
@ExecNodeMetadata(
        name = "stream-exec-python-group-aggregate",
        version = 1,
        producedTransformations =
                StreamExecPythonGroupAggregate.PYTHON_GROUP_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_16,
        minStateVersion = FlinkVersion.v1_16)
public class StreamExecPythonGroupAggregate extends StreamExecAggregateBase {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecPythonGroupAggregate.class);

    public static final String PYTHON_GROUP_AGGREGATE_TRANSFORMATION = "python-group-aggregate";

    private static final String PYTHON_STREAM_AGGREAGTE_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.aggregate.PythonStreamGroupAggregateOperator";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS)
    private final boolean[] aggCallNeedRetractions;

    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    @JsonProperty(FIELD_NAME_NEED_RETRACTION)
    private final boolean needRetraction;

    public StreamExecPythonGroupAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            boolean generateUpdateBefore,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecPythonGroupAggregate.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecPythonGroupAggregate.class, tableConfig),
                grouping,
                aggCalls,
                aggCallNeedRetractions,
                generateUpdateBefore,
                needRetraction,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecPythonGroupAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS) boolean[] aggCallNeedRetractions,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_NEED_RETRACTION) boolean needRetraction,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.aggCallNeedRetractions = checkNotNull(aggCallNeedRetractions);
        checkArgument(aggCalls.length == aggCallNeedRetractions.length);
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {

        if (grouping.length > 0 && config.getStateRetentionTime() < 0) {
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
                        planner.getTypeFactory(),
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo
        final int inputCountIndex = aggInfoList.getIndexOfCountStar();
        final boolean countStarInserted = aggInfoList.countStarInserted();
        Tuple2<PythonAggregateFunctionInfo[], DataViewSpec[][]> aggInfosAndDataViewSpecs =
                CommonPythonUtil.extractPythonAggregateFunctionInfos(aggInfoList, aggCalls);
        PythonAggregateFunctionInfo[] pythonFunctionInfos = aggInfosAndDataViewSpecs.f0;
        DataViewSpec[][] dataViewSpecs = aggInfosAndDataViewSpecs.f1;
        Configuration pythonConfig =
                CommonPythonUtil.extractPythonConfiguration(
                        planner.getExecEnv(), config, planner.getFlinkContext().getClassLoader());
        final OneInputStreamOperator<RowData, RowData> operator =
                getPythonAggregateFunctionOperator(
                        pythonConfig,
                        planner.getFlinkContext().getClassLoader(),
                        inputRowType,
                        InternalTypeInfo.of(getOutputType()).toRowType(),
                        pythonFunctionInfos,
                        dataViewSpecs,
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        inputCountIndex,
                        countStarInserted);
        // partitioned aggregation
        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(PYTHON_GROUP_AGGREGATE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(
                pythonConfig, planner.getFlinkContext().getClassLoader())) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        grouping,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonAggregateFunctionOperator(
            Configuration config,
            ClassLoader classLoader,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewSpec[][] dataViewSpecs,
            long minIdleStateRetentionTime,
            long maxIdleStateRetentionTime,
            int indexOfCountStar,
            boolean countStarInserted) {
        Class<?> clazz =
                CommonPythonUtil.loadClass(PYTHON_STREAM_AGGREAGTE_OPERATOR_NAME, classLoader);
        try {
            Constructor<?> ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            RowType.class,
                            RowType.class,
                            PythonAggregateFunctionInfo[].class,
                            DataViewSpec[][].class,
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
