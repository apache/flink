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

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * {@link DeltaJoinSpec} defines how one side looks up the dimension table on the other side.
 *
 * <p>This class corresponds to {@link StreamPhysicalDeltaJoin}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeltaJoinSpec {

    public static final String FIELD_NAME_LOOKUP_TABLE = "lookupTable";
    public static final String FIELD_NAME_LOOKUP_KEYS = "lookupKeys";
    public static final String FIELD_NAME_REMAINING_CONDITION = "remainingCondition";

    @JsonProperty(FIELD_NAME_LOOKUP_TABLE)
    private final TemporalTableSourceSpec lookupTable;

    /** The map between lookup column index of the dim table and stream side's related key. */
    @JsonProperty(FIELD_NAME_LOOKUP_KEYS)
    private final Map<Integer, FunctionCallUtil.FunctionParam> lookupKeyMap;

    /** join condition except equi-conditions extracted as lookup keys. */
    @JsonProperty(FIELD_NAME_REMAINING_CONDITION)
    private final @Nullable RexNode remainingCondition;

    @JsonCreator
    public DeltaJoinSpec(
            @JsonProperty(FIELD_NAME_LOOKUP_TABLE) TemporalTableSourceSpec lookupTable,
            @JsonProperty(FIELD_NAME_LOOKUP_KEYS)
                    Map<Integer, FunctionCallUtil.FunctionParam> lookupKeyMap,
            @JsonProperty(FIELD_NAME_REMAINING_CONDITION) @Nullable RexNode remainingCondition) {
        this.lookupKeyMap = lookupKeyMap;
        this.lookupTable = lookupTable;
        this.remainingCondition = remainingCondition;
    }

    @JsonIgnore
    public TemporalTableSourceSpec getLookupTable() {
        return lookupTable;
    }

    @JsonIgnore
    public Map<Integer, FunctionCallUtil.FunctionParam> getLookupKeyMap() {
        return lookupKeyMap;
    }

    @JsonIgnore
    public Optional<RexNode> getRemainingCondition() {
        return Optional.ofNullable(remainingCondition);
    }
}
