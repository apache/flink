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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * JoinSpec describes how two tables will be joined.
 *
 * <p>This class corresponds to {@link org.apache.calcite.rel.core.Join} rel node.
 */
public class JoinSpec {
    /** {@link FlinkJoinType} of the join. */
    private final FlinkJoinType joinType;
    /** 0-based index of join keys in left side. */
    private final int[] leftKeys;
    /** 0-based index of join keys in right side. */
    private final int[] rightKeys;
    /** whether to filter null values or not for each corresponding index join key. */
    private final boolean[] filterNulls;
    /** Non Equi join conditions. */
    private final @Nullable RexNode nonEquiCondition;

    public JoinSpec(
            FlinkJoinType joinType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            @Nullable RexNode nonEquiCondition) {
        this.joinType = Preconditions.checkNotNull(joinType);
        this.leftKeys = Preconditions.checkNotNull(leftKeys);
        this.rightKeys = Preconditions.checkNotNull(rightKeys);
        this.filterNulls = Preconditions.checkNotNull(filterNulls);
        Preconditions.checkArgument(leftKeys.length == rightKeys.length);
        Preconditions.checkArgument(leftKeys.length == filterNulls.length);

        this.nonEquiCondition = nonEquiCondition;
    }

    public FlinkJoinType getJoinType() {
        return joinType;
    }

    public int[] getLeftKeys() {
        return leftKeys;
    }

    public int[] getRightKeys() {
        return rightKeys;
    }

    public boolean[] getFilterNulls() {
        return filterNulls;
    }

    public Optional<RexNode> getNonEquiCondition() {
        return Optional.ofNullable(nonEquiCondition);
    }

    /** Gets number of keys in join key. */
    public int getJoinKeySize() {
        return leftKeys.length;
    }
}
