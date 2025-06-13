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

import org.apache.flink.table.planner.plan.utils.FunctionCallUtils.FunctionParam;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * The {@link DeltaJoinSpec} describes how one side of the delta join is performed when using
 * another table as the lookup target.
 *
 * <p>The {@link DeltaJoinSpec} of the left side is treating the table of the right side as a dim
 * table.
 *
 * <p>The {@link DeltaJoinSpec} of the right side is treating the table of the left side as a dim
 * table.
 */
public class DeltaJoinSpec {

    public static final String FIELD_NAME_REMAINING_CONDITION = "remainingCondition";
    public static final String FIELD_NAME_LOOKUP_KEYS = "lookupKeys";

    /** join condition except equi-conditions extracted as lookup keys. */
    @JsonProperty(FIELD_NAME_REMAINING_CONDITION)
    private final @Nullable RexNode remainingCondition;

    // <lookup column index of another side dim table, this table's related key>
    @JsonProperty(FIELD_NAME_LOOKUP_KEYS)
    private final Map<Integer, FunctionParam> lookupKeyMap;

    @JsonCreator
    public DeltaJoinSpec(
            @JsonProperty(FIELD_NAME_REMAINING_CONDITION) @Nullable RexNode remainingCondition,
            @JsonProperty(FIELD_NAME_LOOKUP_KEYS) Map<Integer, FunctionParam> lookupKeyMap) {
        this.remainingCondition = remainingCondition;
        this.lookupKeyMap = lookupKeyMap;
    }

    @JsonIgnore
    public Optional<RexNode> getRemainingCondition() {
        return Optional.ofNullable(remainingCondition);
    }

    @JsonIgnore
    public Map<Integer, FunctionParam> getLookupKeyMap() {
        return lookupKeyMap;
    }
}
