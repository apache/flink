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

import org.apache.flink.util.TimeUtils;

import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import java.util.Locale;
import java.util.Map;

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
     * Merge the state ttl from hints, and the first ttl with same input side will finally be
     * chosen.
     *
     * @return The key of the map is the side of the join (0 represents the left, and 1 represents
     *     the right). The value of the map is the state ttl in milliseconds.
     */
    public static Map<Integer, Long> getStateTtlFromHint(List<RelHint> hints) {
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

                                            // choose the first hint to take effect
                                            stateTtlFromHint.computeIfAbsent(
                                                    side,
                                                    key -> TimeUtils.parseDuration(ttl).toMillis());
                                        }));

        return stateTtlFromHint;
    }
}
