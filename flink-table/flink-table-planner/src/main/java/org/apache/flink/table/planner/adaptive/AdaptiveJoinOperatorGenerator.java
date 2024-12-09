/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.adaptive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.AdaptiveJoin;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.planner.plan.utils.HashJoinOperatorUtil;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.planner.plan.utils.SorMergeJoinOperatorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Implementation class for {@link AdaptiveJoin}. It can selectively generate broadcast hash join,
 * shuffle hash join or shuffle merge join operator based on actual conditions.
 */
public class AdaptiveJoinOperatorGenerator implements AdaptiveJoin {

    private final int[] leftKeys;

    private final int[] rightKeys;

    private final FlinkJoinType joinType;

    private final boolean[] filterNulls;

    private final RowType leftType;

    private final RowType rightType;

    private final GeneratedJoinCondition condFunc;

    private final int leftRowSize;

    private final long leftRowCount;

    private final int rightRowSize;

    private final long rightRowCount;

    private final boolean tryDistinctBuildRow;

    private final long managedMemory;

    private final OperatorType originalJoin;

    private boolean leftIsBuild;

    private boolean isBroadcastJoin;

    public AdaptiveJoinOperatorGenerator(
            int[] leftKeys,
            int[] rightKeys,
            FlinkJoinType joinType,
            boolean[] filterNulls,
            RowType leftType,
            RowType rightType,
            GeneratedJoinCondition condFunc,
            int leftRowSize,
            int rightRowSize,
            long leftRowCount,
            long rightRowCount,
            boolean tryDistinctBuildRow,
            long managedMemory,
            OperatorType originalJoin) {
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.joinType = joinType;
        this.filterNulls = filterNulls;
        this.leftType = leftType;
        this.rightType = rightType;
        this.condFunc = condFunc;
        this.leftRowSize = leftRowSize;
        this.rightRowSize = rightRowSize;
        this.leftRowCount = leftRowCount;
        this.rightRowCount = rightRowCount;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
        this.managedMemory = managedMemory;
        this.originalJoin = originalJoin;
    }

    @Override
    public StreamOperatorFactory<?> genOperatorFactory(
            ClassLoader classLoader, ReadableConfig config) {
        if (isBroadcastJoin || originalJoin == OperatorType.ShuffleHashJoin) {
            return HashJoinOperatorUtil.generateOperatorFactory(
                    leftKeys,
                    rightKeys,
                    joinType,
                    filterNulls,
                    leftType,
                    rightType,
                    condFunc,
                    leftIsBuild,
                    leftRowSize,
                    rightRowSize,
                    leftRowCount,
                    rightRowCount,
                    tryDistinctBuildRow,
                    managedMemory,
                    config,
                    classLoader);
        } else {
            return SorMergeJoinOperatorUtil.generateOperatorFactory(
                    condFunc,
                    leftType,
                    rightType,
                    leftKeys,
                    rightKeys,
                    joinType,
                    config,
                    leftIsBuild,
                    filterNulls,
                    managedMemory,
                    classLoader);
        }
    }

    @Override
    public Tuple2<Boolean, Boolean> enrichAndCheckBroadcast(
            long leftInputSize, long rightInputSize, long threshold) {
        Tuple2<Boolean, Boolean> isBroadcastAndLeftBuild;
        boolean leftSizeSmallerThanThreshold = leftInputSize <= threshold;
        boolean rightSizeSmallerThanThreshold = rightInputSize <= threshold;
        boolean leftSmallerThanRight = leftInputSize < rightInputSize;
        switch (joinType) {
            case RIGHT:
                // For a right outer join, if the left side can be broadcast, then the left side is
                // always the build side; otherwise, the smaller side is the build side.
                isBroadcastAndLeftBuild =
                        new Tuple2<>(
                                leftSizeSmallerThanThreshold,
                                leftSizeSmallerThanThreshold ? true : leftSmallerThanRight);
                break;
            case INNER:
                isBroadcastAndLeftBuild =
                        new Tuple2<>(
                                leftSizeSmallerThanThreshold || rightSizeSmallerThanThreshold,
                                leftSmallerThanRight);
                break;
            case LEFT:
            case SEMI:
            case ANTI:
                // For left outer / semi / anti join, if the right side can be broadcast, then the
                // right side is always the build side; otherwise, the smaller side is the build
                // side.
                isBroadcastAndLeftBuild =
                        new Tuple2<>(
                                rightSizeSmallerThanThreshold,
                                rightSizeSmallerThanThreshold ? false : leftSmallerThanRight);
                break;
            case FULL:
            default:
                throw new RuntimeException(String.format("Unexpected join type %s.", joinType));
        }

        isBroadcastJoin = isBroadcastAndLeftBuild.f0;
        leftIsBuild = isBroadcastAndLeftBuild.f1;
        // Sort merge join requires the left side to be read first if the broadcast threshold is not
        // met.
        if (!isBroadcastJoin && originalJoin == OperatorType.SortMergeJoin) {
            return new Tuple2<>(false, true);
        }
        return isBroadcastAndLeftBuild;
    }
}
