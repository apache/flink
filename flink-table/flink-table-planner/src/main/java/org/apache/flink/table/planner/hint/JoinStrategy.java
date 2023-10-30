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
 * <p>The current process about join hint is following:
 *
 * <ol>
 *   <li>resolve the propagation about the join hints by calcite
 *       <ol>
 *         <li>propagate the join hints from sink to source and in sub-query
 *         <li>capitalize join hint to let all join hints in upper case
 *         <li>clear the join hints that are propagated into query block wrongly
 *         <li>clear the join hints that attaches in the unmatched nodes such as Project
 *       </ol>
 *   <li>validate the join hints and replace the table name in hints with LEFT or RIGHT
 *   <li>clear the query block alias from sink to source
 *   <li>consume join hints in some rules
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
