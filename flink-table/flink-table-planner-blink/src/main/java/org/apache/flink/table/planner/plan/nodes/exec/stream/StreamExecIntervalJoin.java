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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.JoinSpec;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.KeyedCoProcessOperatorWithWatermarkDelay;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.operators.join.interval.ProcTimeIntervalJoin;
import org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link StreamExecNode} for a time interval stream join. */
public class StreamExecIntervalJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamExecIntervalJoin.class);

    private final IntervalJoinSpec intervalJoinSpec;

    public StreamExecIntervalJoin(
            IntervalJoinSpec intervalJoinSpec,
            ExecEdge leftInputEdge,
            ExecEdge rightInputEdge,
            RowType outputType,
            String description) {
        super(Lists.newArrayList(leftInputEdge, rightInputEdge), outputType, description);
        this.intervalJoinSpec = intervalJoinSpec;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        ExecNode<RowData> leftInput = (ExecNode<RowData>) getInputNodes().get(0);
        ExecNode<RowData> rightInput = (ExecNode<RowData>) getInputNodes().get(1);

        RowType leftRowType = (RowType) leftInput.getOutputType();
        RowType rightRowType = (RowType) rightInput.getOutputType();
        Transformation<RowData> leftPlan = leftInput.translateToPlan(planner);
        Transformation<RowData> rightPlan = rightInput.translateToPlan(planner);

        RowType returnType = (RowType) getOutputType();
        InternalTypeInfo<RowData> returnTypeInfo = InternalTypeInfo.of(returnType);
        JoinSpec joinSpec = intervalJoinSpec.getJoinSpec();
        IntervalJoinSpec.WindowBounds windowBounds = intervalJoinSpec.getWindowBounds();
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
                            leftPlan,
                            rightPlan,
                            leftRowType.getFieldCount(),
                            rightRowType.getFieldCount(),
                            returnTypeInfo);
                } else {
                    GeneratedJoinCondition joinCondition =
                            JoinUtil.generateConditionFunction(
                                    planner.getTableConfig(), joinSpec, leftRowType, rightRowType);
                    IntervalJoinFunction joinFunction =
                            new IntervalJoinFunction(
                                    joinCondition, returnTypeInfo, joinSpec.getFilterNulls());

                    TwoInputTransformation<RowData, RowData, RowData> ret;
                    if (windowBounds.isEventTime()) {
                        ret =
                                createRowTimeJoin(
                                        leftPlan,
                                        rightPlan,
                                        returnTypeInfo,
                                        joinFunction,
                                        joinSpec,
                                        windowBounds);
                    } else {
                        ret =
                                createProcTimeJoin(
                                        leftPlan,
                                        rightPlan,
                                        returnTypeInfo,
                                        joinFunction,
                                        joinSpec,
                                        windowBounds);
                    }

                    if (inputsContainSingleton()) {
                        ret.setParallelism(1);
                        ret.setMaxParallelism(1);
                    }

                    // set KeyType and Selector for state
                    int[] leftKeys = joinSpec.getLeftKeys();
                    int[] rightKeys = joinSpec.getRightKeys();
                    RowDataKeySelector leftSelect =
                            KeySelectorUtil.getRowDataSelector(
                                    leftKeys, InternalTypeInfo.of(leftRowType));
                    RowDataKeySelector rightSelect =
                            KeySelectorUtil.getRowDataSelector(
                                    rightKeys, InternalTypeInfo.of(rightRowType));
                    ret.setStateKeySelectors(leftSelect, rightSelect);
                    ret.setStateKeyType(leftSelect.getProducedType());
                    return ret;
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

    private static class FilterAllFlatMapFunction
            implements FlatMapFunction<RowData, RowData>, ResultTypeQueryable<RowData> {

        private final InternalTypeInfo<RowData> outputTypeInfo;

        public FilterAllFlatMapFunction(InternalTypeInfo<RowData> inputTypeInfo) {
            this.outputTypeInfo = inputTypeInfo;
        }

        @Override
        public void flatMap(RowData value, Collector<RowData> out) {}

        @Override
        public TypeInformation<RowData> getProducedType() {
            return outputTypeInfo;
        }
    }

    private static class PaddingLeftMapFunction
            implements MapFunction<RowData, RowData>, ResultTypeQueryable<RowData> {

        private final OuterJoinPaddingUtil paddingUtil;
        private final InternalTypeInfo<RowData> outputTypeInfo;

        public PaddingLeftMapFunction(
                OuterJoinPaddingUtil paddingUtil, InternalTypeInfo<RowData> returnType) {
            this.paddingUtil = paddingUtil;
            this.outputTypeInfo = returnType;
        }

        @Override
        public RowData map(RowData value) {
            return paddingUtil.padLeft(value);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return outputTypeInfo;
        }
    }

    private static class PaddingRightMapFunction
            implements MapFunction<RowData, RowData>, ResultTypeQueryable<RowData> {

        private final OuterJoinPaddingUtil paddingUtil;
        private final InternalTypeInfo<RowData> outputTypeInfo;

        public PaddingRightMapFunction(
                OuterJoinPaddingUtil paddingUtil, InternalTypeInfo<RowData> returnType) {
            this.paddingUtil = paddingUtil;
            this.outputTypeInfo = returnType;
        }

        @Override
        public RowData map(RowData value) {
            return paddingUtil.padRight(value);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return outputTypeInfo;
        }
    }

    @SuppressWarnings("unchecked")
    private Transformation<RowData> createNegativeWindowSizeJoin(
            JoinSpec joinSpec,
            Transformation<RowData> leftPlan,
            Transformation<RowData> rightPlan,
            int leftArity,
            int rightArity,
            InternalTypeInfo<RowData> returnTypeInfo) {
        // We filter all records instead of adding an empty source to preserve the watermarks.
        FilterAllFlatMapFunction allFilter = new FilterAllFlatMapFunction(returnTypeInfo);

        OuterJoinPaddingUtil paddingUtil = new OuterJoinPaddingUtil(leftArity, rightArity);
        PaddingLeftMapFunction leftPadder = new PaddingLeftMapFunction(paddingUtil, returnTypeInfo);

        PaddingRightMapFunction rightPadder =
                new PaddingRightMapFunction(paddingUtil, returnTypeInfo);

        int leftParallelism = leftPlan.getParallelism();
        int rightParallelism = rightPlan.getParallelism();

        OneInputTransformation<RowData, RowData> filterAllLeftStream =
                new OneInputTransformation<>(
                        leftPlan,
                        "filter all left input transformation",
                        new StreamFlatMap<>(allFilter),
                        returnTypeInfo,
                        leftParallelism);

        OneInputTransformation<RowData, RowData> filterAllRightStream =
                new OneInputTransformation<>(
                        rightPlan,
                        "filter all right input transformation",
                        new StreamFlatMap<>(allFilter),
                        returnTypeInfo,
                        rightParallelism);

        OneInputTransformation<RowData, RowData> padLeftStream =
                new OneInputTransformation<>(
                        leftPlan,
                        "pad left input transformation",
                        new StreamMap<>(leftPadder),
                        returnTypeInfo,
                        leftParallelism);

        OneInputTransformation<RowData, RowData> padRightStream =
                new OneInputTransformation<>(
                        rightPlan,
                        "pad right input transformation",
                        new StreamMap<>(rightPadder),
                        returnTypeInfo,
                        rightParallelism);
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
            Transformation<RowData> leftPlan,
            Transformation<RowData> rightPlan,
            InternalTypeInfo<RowData> returnTypeInfo,
            IntervalJoinFunction joinFunction,
            JoinSpec joinSpec,
            IntervalJoinSpec.WindowBounds windowBounds) {
        InternalTypeInfo<RowData> leftTypeInfo =
                (InternalTypeInfo<RowData>) leftPlan.getOutputType();
        InternalTypeInfo<RowData> rightTypeInfo =
                (InternalTypeInfo<RowData>) rightPlan.getOutputType();
        ProcTimeIntervalJoin procJoinFunc =
                new ProcTimeIntervalJoin(
                        joinSpec.getJoinType(),
                        windowBounds.getLeftLowerBound(),
                        windowBounds.getLeftUpperBound(),
                        leftTypeInfo,
                        rightTypeInfo,
                        joinFunction);

        return new TwoInputTransformation<>(
                leftPlan,
                rightPlan,
                getDesc(),
                new KeyedCoProcessOperator<>(procJoinFunc),
                returnTypeInfo,
                leftPlan.getParallelism());
    }

    private TwoInputTransformation<RowData, RowData, RowData> createRowTimeJoin(
            Transformation<RowData> leftPlan,
            Transformation<RowData> rightPlan,
            InternalTypeInfo<RowData> returnTypeInfo,
            IntervalJoinFunction joinFunction,
            JoinSpec joinSpec,
            IntervalJoinSpec.WindowBounds windowBounds) {

        InternalTypeInfo<RowData> leftTypeInfo =
                (InternalTypeInfo<RowData>) leftPlan.getOutputType();
        InternalTypeInfo<RowData> rightTypeInfo =
                (InternalTypeInfo<RowData>) rightPlan.getOutputType();
        RowTimeIntervalJoin rowJoinFunc =
                new RowTimeIntervalJoin(
                        joinSpec.getJoinType(),
                        windowBounds.getLeftLowerBound(),
                        windowBounds.getLeftUpperBound(),
                        0L,
                        leftTypeInfo,
                        rightTypeInfo,
                        joinFunction,
                        windowBounds.getLeftTimeIdx(),
                        windowBounds.getRightTimeIdx());

        return new TwoInputTransformation<>(
                leftPlan,
                rightPlan,
                getDesc(),
                new KeyedCoProcessOperatorWithWatermarkDelay<>(
                        rowJoinFunc, rowJoinFunc.getMaxOutputDelay()),
                returnTypeInfo,
                leftPlan.getParallelism());
    }
}
