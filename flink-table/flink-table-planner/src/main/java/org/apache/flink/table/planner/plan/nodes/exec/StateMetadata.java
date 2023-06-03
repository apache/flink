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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * It is used to describe the state metadata of a stateful operator, which is
 * serialized/deserialized into/from those {@link
 * org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode}s that can generate stateful
 * operators. For ExecNodes that generates {@link
 * org.apache.flink.streaming.api.operators.TwoInputStreamOperator} or {@link
 * org.apache.flink.streaming.api.operators.MultipleInputStreamOperator}, there will be multiple
 * metadata describing information about each input's state.
 *
 * <p>The metadata describes the following attributes.
 *
 * <ul>
 *   <li>{@code stateIndex}: annotates the state is from the i-th input, index based on zero
 *   <li>{@code ttl}: annotates the state retention time for the i-th input's state, the time unit
 *       is ms.
 *   <li>{@code name}: annotates the state description, such as deduplicate-state, join-left-state.
 * </ul>
 */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class StateMetadata {
    public static final String FIELD_NAME_STATE_INDEX = "index";
    public static final String FIELD_NAME_STATE_TTL = "ttl";
    public static final String FIELD_NAME_STATE_NAME = "name";

    @JsonProperty(value = FIELD_NAME_STATE_INDEX, index = 0)
    private final int stateIndex;

    @JsonProperty(value = FIELD_NAME_STATE_TTL, index = 1)
    private final Duration stateTtl;

    @JsonProperty(value = FIELD_NAME_STATE_NAME, index = 2)
    private final String stateName;

    @JsonCreator
    public StateMetadata(
            @JsonProperty(FIELD_NAME_STATE_INDEX) int stateIndex,
            @JsonProperty(FIELD_NAME_STATE_TTL) String stateTtl,
            @JsonProperty(FIELD_NAME_STATE_NAME) String stateName) {
        this(
                stateIndex,
                TimeUtils.parseDuration(
                        Preconditions.checkNotNull(stateTtl, "state ttl should not be null")),
                stateName);
    }

    public StateMetadata(int stateIndex, Duration stateTtl, String stateName) {
        Preconditions.checkArgument(stateIndex >= 0, "state index should start from 0");
        this.stateIndex = stateIndex;
        this.stateTtl = Preconditions.checkNotNull(stateTtl, "state ttl should not be null");
        this.stateName = Preconditions.checkNotNull(stateName, "state name should not be null");
    }

    public int getStateIndex() {
        return stateIndex;
    }

    @JsonGetter(value = FIELD_NAME_STATE_TTL)
    public String getStateTtl() {
        return TimeUtils.formatWithHighestUnit(stateTtl);
    }

    public static List<StateMetadata> getOneInputOperatorDefaultMeta(
            ReadableConfig tableConfig, String stateName) {
        return Collections.singletonList(
                new StateMetadata(
                        0,
                        tableConfig.get(ExecutionConfigOptions.IDLE_STATE_RETENTION),
                        stateName));
    }

    public static List<StateMetadata> getMultiInputOperatorDefaultMeta(
            ReadableConfig tableConfig, String... stateNameList) {
        Duration stateRetentionTime = tableConfig.get(ExecutionConfigOptions.IDLE_STATE_RETENTION);
        return IntStream.range(0, stateNameList.length)
                .boxed()
                .map(i -> new StateMetadata(i, stateRetentionTime, stateNameList[i]))
                .collect(Collectors.toList());
    }

    public static long getStateTtlForOneInputOperator(
            ExecNodeConfig config, @Nullable List<StateMetadata> stateMetadataList) {
        return getStateTtlForMultiInputOperator(config, 1, stateMetadataList).get(0);
    }

    public static List<Long> getStateTtlForMultiInputOperator(
            ExecNodeConfig config,
            int inputNumOfOperator,
            @Nullable List<StateMetadata> stateMetadataList) {
        // for backward compatibility
        if (CollectionUtil.isNullOrEmpty(stateMetadataList)) {
            return Stream.generate(config::getStateRetentionTime)
                    .limit(inputNumOfOperator)
                    .collect(Collectors.toList());
        }
        // in case malformed json plan
        validateStateMetadata(inputNumOfOperator, stateMetadataList);
        return stateMetadataList.stream()
                .sorted(Comparator.comparing(StateMetadata::getStateIndex))
                .map(metadata -> metadata.stateTtl.toMillis())
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StateMetadata)) {
            return false;
        }
        StateMetadata that = (StateMetadata) o;
        return stateIndex == that.stateIndex
                && stateTtl.equals(that.stateTtl)
                && stateName.equals(that.stateName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateIndex, stateTtl, stateName);
    }

    /**
     * Validate deserialized state metadata from json content of {@link
     * org.apache.flink.table.api.CompiledPlan}.
     *
     * @param inputNumOfOperator the input number of the stateful operator that the exec node to
     *     translate to.
     * @param stateMetadataList the deserialized state metadata list.
     */
    private static void validateStateMetadata(
            int inputNumOfOperator, List<StateMetadata> stateMetadataList) {

        // the state metadata list size should be equal to the input number of the operator
        Preconditions.checkArgument(
                inputNumOfOperator == stateMetadataList.size(),
                String.format(
                        "The compiled plan contains inconsistent state metadata configuration.\n"
                                + "Received %s state meta for a %sInputStreamOperator.",
                        stateMetadataList.size(),
                        inputNumOfOperator > 2
                                ? "Multiple"
                                : inputNumOfOperator == 2 ? "Two" : "One"));

        // the state index should not contain duplicates, and should start from 0 to inputNum - 1
        List<Integer> normalizedIndexList =
                stateMetadataList.stream()
                        .map(StateMetadata::getStateIndex)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList());
        Preconditions.checkArgument(
                normalizedIndexList.size() == inputNumOfOperator
                        && normalizedIndexList.get(0) == 0
                        && normalizedIndexList.get(inputNumOfOperator - 1)
                                == inputNumOfOperator - 1,
                "The compiled plan contains inconsistent state metadata configuration.\n"
                        + "The state index should not contain duplicates and start from 0 (inclusive) "
                        + "and monotonically increase to the input size (exclusive) of the operator.");
    }
}
