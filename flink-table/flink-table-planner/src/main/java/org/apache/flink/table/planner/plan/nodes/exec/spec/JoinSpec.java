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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * JoinSpec describes how two tables will be joined.
 *
 * <p>This class corresponds to {@link org.apache.calcite.rel.core.Join} rel node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JoinSpec {
    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_LEFT_KEYS = "leftKeys";
    public static final String FIELD_NAME_RIGHT_KEYS = "rightKeys";
    public static final String FIELD_NAME_FILTER_NULLS = "filterNulls";
    public static final String FIELD_NAME_NON_EQUI_CONDITION = "nonEquiCondition";

    /** {@link FlinkJoinType} of the join. */
    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    /** 0-based index of join keys in left side. */
    @JsonProperty(FIELD_NAME_LEFT_KEYS)
    private final int[] leftKeys;

    /** 0-based index of join keys in right side. */
    @JsonProperty(FIELD_NAME_RIGHT_KEYS)
    private final int[] rightKeys;

    /** whether to filter null values or not for each corresponding index join key. */
    @JsonProperty(FIELD_NAME_FILTER_NULLS)
    private final boolean[] filterNulls;

    /** Non Equi join conditions. */
    @JsonProperty(FIELD_NAME_NON_EQUI_CONDITION)
    private final @Nullable RexNode nonEquiCondition;

    @JsonCreator
    public JoinSpec(
            @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
            @JsonProperty(FIELD_NAME_LEFT_KEYS) int[] leftKeys,
            @JsonProperty(FIELD_NAME_RIGHT_KEYS) int[] rightKeys,
            @JsonProperty(FIELD_NAME_FILTER_NULLS) boolean[] filterNulls,
            @JsonProperty(FIELD_NAME_NON_EQUI_CONDITION) @Nullable RexNode nonEquiCondition) {
        this.joinType = Preconditions.checkNotNull(joinType);
        this.leftKeys = Preconditions.checkNotNull(leftKeys);
        this.rightKeys = Preconditions.checkNotNull(rightKeys);
        this.filterNulls = Preconditions.checkNotNull(filterNulls);
        Preconditions.checkArgument(leftKeys.length == rightKeys.length);
        Preconditions.checkArgument(leftKeys.length == filterNulls.length);

        if (null != nonEquiCondition && nonEquiCondition.isAlwaysTrue()) {
            this.nonEquiCondition = null;
        } else {
            this.nonEquiCondition = nonEquiCondition;
        }
    }

    @JsonIgnore
    public FlinkJoinType getJoinType() {
        return joinType;
    }

    @JsonIgnore
    public int[] getLeftKeys() {
        return leftKeys;
    }

    @JsonIgnore
    public int[] getRightKeys() {
        return rightKeys;
    }

    @JsonIgnore
    public boolean[] getFilterNulls() {
        return filterNulls;
    }

    @JsonIgnore
    public Optional<RexNode> getNonEquiCondition() {
        return Optional.ofNullable(nonEquiCondition);
    }

    /** Gets number of keys in join key. */
    @JsonIgnore
    public int getJoinKeySize() {
        return leftKeys.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinSpec joinSpec = (JoinSpec) o;
        return joinType == joinSpec.joinType
                && Arrays.equals(leftKeys, joinSpec.leftKeys)
                && Arrays.equals(rightKeys, joinSpec.rightKeys)
                && Arrays.equals(filterNulls, joinSpec.filterNulls)
                && Objects.equals(nonEquiCondition, joinSpec.nonEquiCondition);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(joinType, nonEquiCondition);
        result = 31 * result + Arrays.hashCode(leftKeys);
        result = 31 * result + Arrays.hashCode(rightKeys);
        result = 31 * result + Arrays.hashCode(filterNulls);
        return result;
    }
}
