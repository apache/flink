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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.KeyedCoProcessOperatorWithWatermarkDelay;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.operators.join.interval.FilterAllFlatMapFunction;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.operators.join.interval.PaddingLeftMapFunction;
import org.apache.flink.table.runtime.operators.join.interval.PaddingRightMapFunction;
import org.apache.flink.table.runtime.operators.join.interval.ProcTimeIntervalJoin;
import org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** {@link StreamExecNode} for a time interval stream join. */
@ExecNodeMetadata(
        name = "stream-exec-interval-join",
        version = 1,
        producedTransformations = {
            StreamExecIntervalJoin.FILTER_LEFT_TRANSFORMATION,
            StreamExecIntervalJoin.FILTER_RIGHT_TRANSFORMATION,
            StreamExecIntervalJoin.PAD_LEFT_TRANSFORMATION,
            StreamExecIntervalJoin.PAD_RIGHT_TRANSFORMATION,
            StreamExecIntervalJoin.INTERVAL_JOIN_TRANSFORMATION
        },
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecIntervalJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, MultipleTransformationTranslator<RowData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamExecIntervalJoin.class);

    public static final String FILTER_LEFT_TRANSFORMATION = "filter-left";
    public static final String FILTER_RIGHT_TRANSFORMATION = "filter-right";
    public static final String PAD_LEFT_TRANSFORMATION = "pad-left";
    public static final String PAD_RIGHT_TRANSFORMATION = "pad-right";
    public static final String INTERVAL_JOIN_TRANSFORMATION = "interval-join";

    public static final String FIELD_NAME_INTERVAL_JOIN_SPEC = "intervalJoinSpec";

    @JsonProperty(FIELD_NAME_INTERVAL_JOIN_SPEC)
    private final IntervalJoinSpec intervalJoinSpec;

    public StreamExecIntervalJoin(
            ReadableConfig tableConfig,
            IntervalJoinSpec intervalJoinSpec,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecIntervalJoin.class),
                ExecNodeContext.newPersistedConfig(StreamExecIntervalJoin.class, tableConfig),
                intervalJoinSpec,
                Lists.newArrayList(leftInputProperty, rightInputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecIntervalJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_INTERVAL_JOIN_SPEC) IntervalJoinSpec intervalJoinSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        Preconditions.checkArgument(inputProperties.size() == 2);
        this.intervalJoinSpec = Preconditions.checkNotNull(intervalJoinSpec);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);

        RowType leftRowType = (RowType) leftInputEdge.getOutputType();
        RowType rightRowType = (RowType) rightInputEdge.getOutputType();
        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        RowType returnType = (RowType) getOutputType();
        InternalTypeInfo<RowData> returnTypeInfo = InternalTypeInfo.of(returnType);
        JoinSpec joinSpec = intervalJoinSpec.getJoinSpec();
        IntervalJoinSpec.WindowBounds windowBounds = intervalJoinSpec.getWindowBounds();
        long minCleanUpIntervalMillis =
                planner.getTableConfig()
                        .get(ExecutionConfigOptions.TABLE_EXEC_INTERVAL_JOIN_MIN_CLEAN_UP_INTERVAL)
                        .toMillis();
        switch (joinSpec.getJoinType()) {
            case INNER:
            case LEFT:
            case RIGHT:
            case FULL:
                long relativeWindowSize =
                        windowBounds.getLeftUpperBound() - windowBounds.getLeftLowerBound();
                if (relativeWindowSize < 0) {
                    LOGGER.warn(
                            "The relative time interval size "
                                    + relativeWindowSize
                                    + "is negative, please check the join conditions.");
                    return createNegativeWindowSizeJoin(
                            joinSpec,
                            leftInputTransform,
                            rightInputTransform,
                            leftRowType.getFieldCount(),
                            rightRowType.getFieldCount(),
                            returnTypeInfo,
                            config);
                } else {
                    GeneratedJoinCondition joinCondition =
                            JoinUtil.generateConditionFunction(
                                    config,
                                    planner.getFlinkContext().getClassLoader(),
                                    joinSpec,
                                    leftRowType,
                                    rightRowType);
                    IntervalJoinFunction joinFunction =
                            new IntervalJoinFunction(
                                    joinCondition, returnTypeInfo, joinSpec.getFilterNulls());

                    TwoInputTransformation<RowData, RowData, RowData> transform;
                    if (windowBounds.isEventTime()) {
                        transform =
                                createRowTimeJoin(
                                        leftInputTransform,
                                        rightInputTransform,
                                        returnTypeInfo,
                                        joinFunction,
                                        joinSpec,
                                        windowBounds,
                                        minCleanUpIntervalMillis,
                                        config);
                    } else {
                        transform =
                                createProcTimeJoin(
                                        leftInputTransform,
                                        rightInputTransform,
                                        returnTypeInfo,
                                        joinFunction,
                                        joinSpec,
                                        windowBounds,
                                        minCleanUpIntervalMillis,
                                        config);
                    }

                    if (inputsContainSingleton()) {
                        transform.setParallelism(1);
                        transform.setMaxParallelism(1);
                    }

                    // set KeyType and Selector for state
                    RowDataKeySelector leftSelect =
                            KeySelectorUtil.getRowDataSelector(
                                    planner.getFlinkContext().getClassLoader(),
                                    joinSpec.getLeftKeys(),
                                    InternalTypeInfo.of(leftRowType));
                    RowDataKeySelector rightSelect =
                            KeySelectorUtil.getRowDataSelector(
                                    planner.getFlinkContext().getClassLoader(),
                                    joinSpec.getRightKeys(),
                                    InternalTypeInfo.of(rightRowType));
                    transform.setStateKeySelectors(leftSelect, rightSelect);
                    transform.setStateKeyType(leftSelect.getProducedType());
                    return transform;
                }
            default:
                throw new TableException(
                        "Interval Join: "
                                + joinSpec.getJoinType()
                                + " Join between stream "
                                + "and stream is not supported yet.\nplease re-check "
                                + "interval join statement according to description above.");
        }
    }

    private Transformation<RowData> createNegativeWindowSizeJoin(
            JoinSpec joinSpec,
            Transformation<RowData> leftInputTransform,
            Transformation<RowData> rightInputTransform,
            int leftArity,
            int rightArity,
            InternalTypeInfo<RowData> returnTypeInfo,
            ExecNodeConfig config) {
        boolean shouldCreateUid =
                config.get(ExecutionConfigOptions.TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS);

        // We filter all records instead of adding an empty source to preserve the watermarks.
        FilterAllFlatMapFunction allFilter = new FilterAllFlatMapFunction(returnTypeInfo);

        OuterJoinPaddingUtil paddingUtil = new OuterJoinPaddingUtil(leftArity, rightArity);
        PaddingLeftMapFunction leftPadder = new PaddingLeftMapFunction(paddingUtil, returnTypeInfo);

        PaddingRightMapFunction rightPadder =
                new PaddingRightMapFunction(paddingUtil, returnTypeInfo);

        int leftParallelism = leftInputTransform.getParallelism();
        int rightParallelism = rightInputTransform.getParallelism();

        OneInputTransformation<RowData, RowData> filterAllLeftStream =
                new OneInputTransformation<>(
                        leftInputTransform,
                        "FilterLeft",
                        new StreamFlatMap<>(allFilter),
                        returnTypeInfo,
                        leftParallelism,
                        false);
        if (shouldCreateUid) {
            filterAllLeftStream.setUid(createTransformationUid(FILTER_LEFT_TRANSFORMATION, config));
        }
        filterAllLeftStream.setDescription(
                createFormattedTransformationDescription(
                        "filter all left input transformation", config));
        filterAllLeftStream.setName(
                createFormattedTransformationName(
                        filterAllLeftStream.getDescription(), "FilterLeft", config));

        OneInputTransformation<RowData, RowData> filterAllRightStream =
                new OneInputTransformation<>(
                        rightInputTransform,
                        "FilterRight",
                        new StreamFlatMap<>(allFilter),
                        returnTypeInfo,
                        rightParallelism,
                        false);
        if (shouldCreateUid) {
            filterAllRightStream.setUid(
                    createTransformationUid(FILTER_RIGHT_TRANSFORMATION, config));
        }
        filterAllRightStream.setDescription(
                createFormattedTransformationDescription(
                        "filter all right input transformation", config));
        filterAllRightStream.setName(
                createFormattedTransformationName(
                        filterAllRightStream.getDescription(), "FilterRight", config));

        OneInputTransformation<RowData, RowData> padLeftStream =
                new OneInputTransformation<>(
                        leftInputTransform,
                        "PadLeft",
                        new StreamMap<>(leftPadder),
                        returnTypeInfo,
                        leftParallelism,
                        false);
        if (shouldCreateUid) {
            padLeftStream.setUid(createTransformationUid(PAD_LEFT_TRANSFORMATION, config));
        }
        padLeftStream.setDescription(
                createFormattedTransformationDescription("pad left input transformation", config));
        padLeftStream.setName(
                createFormattedTransformationName(
                        padLeftStream.getDescription(), "PadLeft", config));

        OneInputTransformation<RowData, RowData> padRightStream =
                new OneInputTransformation<>(
                        rightInputTransform,
                        "PadRight",
                        new StreamMap<>(rightPadder),
                        returnTypeInfo,
                        rightParallelism,
                        false);
        if (shouldCreateUid) {
            padRightStream.setUid(createTransformationUid(PAD_RIGHT_TRANSFORMATION, config));
        }
        padRightStream.setDescription(
                createFormattedTransformationDescription("pad right input transformation", config));
        padRightStream.setName(
                createFormattedTransformationName(
                        padRightStream.getDescription(), "PadRight", config));

        switch (joinSpec.getJoinType()) {
            case INNER:
                return new UnionTransformation<>(
                        Lists.newArrayList(filterAllLeftStream, filterAllRightStream));
            case LEFT:
                return new UnionTransformation<>(
                        Lists.newArrayList(padLeftStream, filterAllRightStream));
            case RIGHT:
                return new UnionTransformation<>(
                        Lists.newArrayList(filterAllLeftStream, padRightStream));
            case FULL:
                return new UnionTransformation<>(Lists.newArrayList(padLeftStream, padRightStream));
            default:
                throw new TableException("should no reach here");
        }
    }

    private TwoInputTransformation<RowData, RowData, RowData> createProcTimeJoin(
            Transformation<RowData> leftInputTransform,
            Transformation<RowData> rightInputTransform,
            InternalTypeInfo<RowData> returnTypeInfo,
            IntervalJoinFunction joinFunction,
            JoinSpec joinSpec,
            IntervalJoinSpec.WindowBounds windowBounds,
            long minCleanUpIntervalMillis,
            ExecNodeConfig config) {
        InternalTypeInfo<RowData> leftTypeInfo =
                (InternalTypeInfo<RowData>) leftInputTransform.getOutputType();
        InternalTypeInfo<RowData> rightTypeInfo =
                (InternalTypeInfo<RowData>) rightInputTransform.getOutputType();
        ProcTimeIntervalJoin procJoinFunc =
                new ProcTimeIntervalJoin(
                        joinSpec.getJoinType(),
                        windowBounds.getLeftLowerBound(),
                        windowBounds.getLeftUpperBound(),
                        minCleanUpIntervalMillis,
                        leftTypeInfo,
                        rightTypeInfo,
                        joinFunction);

        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                createTransformationMeta(INTERVAL_JOIN_TRANSFORMATION, config),
                new KeyedCoProcessOperator<>(procJoinFunc),
                returnTypeInfo,
                leftInputTransform.getParallelism(),
                false);
    }

    private TwoInputTransformation<RowData, RowData, RowData> createRowTimeJoin(
            Transformation<RowData> leftInputTransform,
            Transformation<RowData> rightInputTransform,
            InternalTypeInfo<RowData> returnTypeInfo,
            IntervalJoinFunction joinFunction,
            JoinSpec joinSpec,
            IntervalJoinSpec.WindowBounds windowBounds,
            long minCleanUpIntervalMillis,
            ExecNodeConfig config) {

        InternalTypeInfo<RowData> leftTypeInfo =
                (InternalTypeInfo<RowData>) leftInputTransform.getOutputType();
        InternalTypeInfo<RowData> rightTypeInfo =
                (InternalTypeInfo<RowData>) rightInputTransform.getOutputType();
        RowTimeIntervalJoin rowJoinFunc =
                new RowTimeIntervalJoin(
                        joinSpec.getJoinType(),
                        windowBounds.getLeftLowerBound(),
                        windowBounds.getLeftUpperBound(),
                        0L, // allowedLateness
                        minCleanUpIntervalMillis,
                        leftTypeInfo,
                        rightTypeInfo,
                        joinFunction,
                        windowBounds.getLeftTimeIdx(),
                        windowBounds.getRightTimeIdx());

        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                createTransformationMeta(INTERVAL_JOIN_TRANSFORMATION, config),
                new KeyedCoProcessOperatorWithWatermarkDelay<>(
                        rowJoinFunc, rowJoinFunc.getMaxOutputDelay()),
                returnTypeInfo,
                leftInputTransform.getParallelism(),
                false);
    }
}
