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

import java.util.List;
import java.util.Locale;

/**
 * Currently available join strategies and corresponding join hint names.
 *
 * <p>The process for handling join hints is as follows:
 *
 * <ol>
 *   <li>Resolve join hint propagation:
 *       <ol>
 *         <li>The join hints are resolved using Calcite's functionality to propagate them from the
 *             sink to the source and within sub-queries
 *         <li>Capitalize join hints: All join hints are capitalized to ensure consistency, as they
 *             are expected to be in uppercase.
 *         <li>Clear incorrectly propagated join hints: Any join hints that have been mistakenly
 *             propagated into the query block are cleared.
 *         <li>Clear join hints from unmatched nodes: Join hints attached to unmatched nodes, such
 *             as {@link org.apache.calcite.rel.core.Project}, are also cleared.
 *       </ol>
 *   <li>Validate and modify join hints: The join hints are validated, and the table names in the
 *       hints are replaced with LEFT or RIGHT to indicate the join input ordinal.
 *   <li>Clear query block aliases: The query block aliases are cleared from the sink to the source.
 *   <li>Consume join hints in applicable rules: Finally, the join hints are consumed in specific
 *       rules where they are relevant.
 * </ol>
 */
public enum JoinStrategy {
    /**
     * Instructs the optimizer to use broadcast hash join strategy. If both sides are specified in
     * this hint, the side that is first written will be broadcast.
     */
    BROADCAST("BROADCAST"),

    /**
     * Instructs the optimizer to use shuffle hash join strategy. If both sides are specified in
     * this hint, the side that is first written will be treated as the build side.
     */
    SHUFFLE_HASH("SHUFFLE_HASH"),

    /**
     * Instructs the optimizer to use shuffle sort merge join strategy. As long as one of the side
     * is specified in this hint, it will be tried.
     */
    SHUFFLE_MERGE("SHUFFLE_MERGE"),

    /**
     * Instructs the optimizer to use nest loop join strategy. If both sides are specified in this
     * hint, the side that is first written will be treated as the build side.
     */
    NEST_LOOP("NEST_LOOP"),

    /** Instructs the optimizer to use lookup join strategy. Only accept key-value hint options. */
    LOOKUP("LOOKUP");

    private final String joinHintName;

    JoinStrategy(String joinHintName) {
        this.joinHintName = joinHintName;
    }

    // ~ option name for join hint
    public static final String LEFT_INPUT = "LEFT";
    public static final String RIGHT_INPUT = "RIGHT";

    public static boolean isJoinStrategy(String hintName) {
        try {
            JoinStrategy.valueOf(hintName.toUpperCase(Locale.ROOT));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public String getJoinHintName() {
        return joinHintName;
    }

    public static boolean validOptions(String hintName, List<String> options) {
        if (!isJoinStrategy(hintName)) {
            return false;
        }

        JoinStrategy strategy = JoinStrategy.valueOf(hintName);
        switch (strategy) {
            case SHUFFLE_HASH:
            case SHUFFLE_MERGE:
            case BROADCAST:
            case NEST_LOOP:
                return options.size() > 0;
            case LOOKUP:
                return null == options || options.size() == 0;
        }
        return false;
    }

    public static boolean isLookupHint(String hintName) {
        String formalizedHintName = hintName.toUpperCase(Locale.ROOT);
        return isJoinStrategy(formalizedHintName)
                && JoinStrategy.valueOf(formalizedHintName) == LOOKUP;
    }
}
