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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.planner.plan.utils.HashJoinOperatorUtil;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.planner.plan.utils.SorMergeJoinOperatorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoin;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation class for {@link AdaptiveJoin}. It can selectively generate broadcast hash join,
 * shuffle hash join or shuffle merge join operator based on actual conditions.
 */
public class AdaptiveJoinOperatorGenerator implements AdaptiveJoin {
    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveJoinOperatorGenerator.class);
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

    private boolean originalLeftIsBuild;

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
            boolean leftIsBuild,
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
        checkState(
                originalJoin == OperatorType.ShuffleHashJoin
                        || originalJoin == OperatorType.SortMergeJoin,
                String.format(
                        "Adaptive join "
                                + "currently only supports adaptive optimization for ShuffleHashJoin and "
                                + "SortMergeJoin, not including %s.",
                        originalJoin.toString()));
        this.leftIsBuild = leftIsBuild;
        this.originalLeftIsBuild = leftIsBuild;
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
    public FlinkJoinType getJoinType() {
        return joinType;
    }

    @Override
    public void markAsBroadcastJoin(boolean canBroadcast, boolean leftIsBuild) {
        this.isBroadcastJoin = canBroadcast;
        this.leftIsBuild = leftIsBuild;
    }

    @Override
    public boolean shouldReorderInputs() {
        // Sort merge join requires the left side to be read first if the broadcast threshold is not
        // met.
        if (!isBroadcastJoin && originalJoin == OperatorType.SortMergeJoin) {
            return false;
        }

        if (leftIsBuild != originalLeftIsBuild) {
            LOG.info(
                    "The build side of the adaptive join has been updated. Compile phase build side: {}, Runtime build side: {}.",
                    originalLeftIsBuild ? "left" : "right",
                    leftIsBuild ? "left" : "right");
        }
        return !leftIsBuild;
    }
}
