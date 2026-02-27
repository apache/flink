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
import org.apache.flink.table.planner.plan.utils.SorMergeJoinOperatorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoinGenerator;
import org.apache.flink.table.types.logical.RowType;

/**
 * Implementation class for {@link AdaptiveJoinGenerator}. It can selectively generate broadcast
 * hash join, shuffle hash join or shuffle merge join operator based on actual conditions.
 */
public class AdaptiveJoinOperatorGenerator implements AdaptiveJoinGenerator {
    private final int[] leftKeys;

    private final int[] rightKeys;

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

    public AdaptiveJoinOperatorGenerator(
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            RowType leftType,
            RowType rightType,
            GeneratedJoinCondition condFunc,
            int leftRowSize,
            int rightRowSize,
            long leftRowCount,
            long rightRowCount,
            boolean tryDistinctBuildRow,
            long managedMemory) {
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
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
    }

    @Override
    public StreamOperatorFactory<?> genOperatorFactory(
            ClassLoader classLoader,
            ReadableConfig config,
            FlinkJoinType joinType,
            boolean originIsSortMergeJoin,
            boolean isBroadcastJoin,
            boolean leftIsBuild) {
        if (isBroadcastJoin || !originIsSortMergeJoin) {
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
}
