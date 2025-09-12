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

package org.apache.flink.table.planner.hint;

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;
import org.apache.flink.util.TimeUtils;

import org.apache.calcite.rel.hint.RelHint;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Hint strategy to configure different {@link
 * org.apache.flink.table.api.config.ExecutionConfigOptions#IDLE_STATE_RETENTION} for stream joins.
 *
 * <p>TODO support agg state ttl hint.
 */
public enum StateTtlHint {

    /**
     * Instructs the optimizer to use the specified state ttl for the underlying table.
     *
     * <p>Only accept key-value hint options.
     */
    STATE_TTL("STATE_TTL");

    public static final String NO_STATE_TTL = "0s";

    private final String hintName;

    StateTtlHint(String hintName) {
        this.hintName = hintName;
    }

    public String getHintName() {
        return hintName;
    }

    public static boolean isStateTtlHint(String hintName) {
        try {
            String formalizedHintName = hintName.toUpperCase(Locale.ROOT);
            return StateTtlHint.valueOf(formalizedHintName) == STATE_TTL;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get the state ttl from hints on the {@link org.apache.calcite.rel.BiRel} such as Join and
     * Correlate.
     *
     * @return Returns a map where the key is the input side (0 for LEFT and 1 for RIGHT side). The
     *     value of the map is the state ttl in milliseconds.
     */
    public static Map<Integer, Long> getStateTtlFromHintOnBiRel(List<RelHint> hints) {
        Map<Integer, Long> stateTtlFromHint = new java.util.HashMap<>();
        hints.stream()
                .filter(hint -> StateTtlHint.isStateTtlHint(hint.hintName))
                .forEach(
                        hint ->
                                hint.kvOptions.forEach(
                                        (input, ttl) -> {
                                            int side;
                                            if (FlinkHints.LEFT_INPUT.equals(input)) {
                                                side = 0;
                                            } else {
                                                side = 1;
                                            }

                                            stateTtlFromHint.put(
                                                    side, TimeUtils.parseDuration(ttl).toMillis());
                                        }));

        return stateTtlFromHint;
    }

    /**
     * Get the state ttl from hints from the listOptions inside the STATE_TTL {@link RelHint} if
     * present. Else if returns an empty map. Used for nodes with multiple inputs such as {@link
     * StreamPhysicalMultiJoin}.
     *
     * @return Returns a map where the key is the input identifier (0-based index of the input) and
     *     the value is the state ttl in milliseconds.
     */
    public static Map<Integer, Long> getStateTtlFromHintOnMultiRel(List<RelHint> hints) {
        Map<Integer, Long> stateTtlFromHint = new java.util.HashMap<>();
        hints.stream()
                .filter(hint -> StateTtlHint.isStateTtlHint(hint.hintName))
                .forEach(
                        hint -> {
                            List<String> ttls = hint.listOptions;
                            IntStream.range(0, ttls.size())
                                    .forEach(
                                            id ->
                                                    stateTtlFromHint.put(
                                                            id,
                                                            TimeUtils.parseDuration(ttls.get(id))
                                                                    .toMillis()));
                        });
        return stateTtlFromHint;
    }

    /**
     * Get the state ttl from hints on the {@link org.apache.calcite.rel.SingleRel} such as
     * Aggregate.
     *
     * @return the state ttl in milliseconds. If no state ttl hints set from hint, return "null".
     */
    @Nullable
    public static Long getStateTtlFromHintOnSingleRel(List<RelHint> hints) {
        List<Long> allStateTtl =
                hints.stream()
                        .filter(hint -> StateTtlHint.isStateTtlHint(hint.hintName))
                        .flatMap(hint -> hint.listOptions.stream())
                        .map(ttl -> TimeUtils.parseDuration(ttl).toMillis())
                        .collect(Collectors.toList());

        return allStateTtl.isEmpty() ? null : allStateTtl.get(0);
    }
}
