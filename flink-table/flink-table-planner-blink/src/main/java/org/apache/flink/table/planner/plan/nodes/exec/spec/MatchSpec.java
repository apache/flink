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

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

/**
 * {@link MatchSpec} describes the MATCH_RECOGNIZE info, see {@link
 * org.apache.calcite.rel.core.Match}.
 */
public class MatchSpec {
    /** Regular expression that defines pattern variables. */
    private final RexNode pattern;
    /** Pattern definitions. */
    private final Map<String, RexNode> patternDefinitions;
    /** Measure definitions. */
    private final Map<String, RexNode> measures;
    /** After match definitions. */
    private final RexNode after;
    /** Subsets of pattern variables. */
    private final Map<String, SortedSet<String>> subsets;
    /** Whether all rows per match (false means one row per match). */
    private final boolean allRows;
    /** Partition by columns. */
    private final PartitionSpec partition;
    /** Order by columns. */
    private final SortSpec orderKeys;
    /** Interval definition, null if WITHIN clause is not defined. */
    private final @Nullable RexNode interval;

    public MatchSpec(
            RexNode pattern,
            Map<String, RexNode> patternDefinitions,
            Map<String, RexNode> measures,
            RexNode after,
            Map<String, SortedSet<String>> subsets,
            boolean allRows,
            PartitionSpec partition,
            SortSpec orderKeys,
            @Nullable RexNode interval) {
        this.pattern = pattern;
        this.patternDefinitions = patternDefinitions;
        this.measures = measures;
        this.after = after;
        this.subsets = subsets;
        this.allRows = allRows;
        this.partition = partition;
        this.orderKeys = orderKeys;
        this.interval = interval;
    }

    public RexNode getPattern() {
        return pattern;
    }

    public Map<String, RexNode> getPatternDefinitions() {
        return patternDefinitions;
    }

    public Map<String, RexNode> getMeasures() {
        return measures;
    }

    public RexNode getAfter() {
        return after;
    }

    public Map<String, SortedSet<String>> getSubsets() {
        return subsets;
    }

    public boolean isAllRows() {
        return allRows;
    }

    public PartitionSpec getPartition() {
        return partition;
    }

    public SortSpec getOrderKeys() {
        return orderKeys;
    }

    public Optional<RexNode> getInterval() {
        return Optional.ofNullable(interval);
    }

    @Override
    public String toString() {
        return "Match{"
                + "pattern="
                + pattern
                + ", patternDefinitions="
                + patternDefinitions
                + ", measures="
                + measures
                + ", after="
                + after
                + ", subsets="
                + subsets
                + ", allRows="
                + allRows
                + ", partition="
                + partition
                + ", orderKeys="
                + orderKeys
                + ", interval="
                + interval
                + '}';
    }
}
