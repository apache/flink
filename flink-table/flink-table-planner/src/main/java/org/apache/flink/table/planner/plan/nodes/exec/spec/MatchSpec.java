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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link MatchSpec} describes the MATCH_RECOGNIZE info, see {@link
 * org.apache.calcite.rel.core.Match}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MatchSpec {
    public static final String FIELD_NAME_PATTERN = "pattern";
    public static final String FIELD_NAME_PATTERN_DEFINITIONS = "patternDefinitions";
    public static final String FIELD_NAME_MEASURES = "measures";
    public static final String FIELD_NAME_AFTER = "after";
    public static final String FIELD_NAME_SUBSETS = "subsets";
    public static final String FIELD_NAME_ALL_ROWS = "allRows";
    public static final String FIELD_NAME_PARTITION = "partition";
    public static final String FIELD_NAME_SORT_SPEC = "orderBy";
    public static final String FIELD_NAME_INTERVAL = "interval";

    /** Regular expression that defines pattern variables. */
    @JsonProperty(FIELD_NAME_PATTERN)
    private final RexNode pattern;

    /** Pattern definitions. */
    @JsonProperty(FIELD_NAME_PATTERN_DEFINITIONS)
    private final Map<String, RexNode> patternDefinitions;

    /** Measure definitions. */
    @JsonProperty(FIELD_NAME_MEASURES)
    private final Map<String, RexNode> measures;

    /** After match definitions. */
    @JsonProperty(FIELD_NAME_AFTER)
    private final RexNode after;

    /** Subsets of pattern variables. */
    @JsonProperty(FIELD_NAME_SUBSETS)
    private final Map<String, SortedSet<String>> subsets;

    /** Whether all rows per match (false means one row per match). */
    @JsonProperty(FIELD_NAME_ALL_ROWS)
    private final boolean allRows;

    /** Partition by columns. */
    @JsonProperty(FIELD_NAME_PARTITION)
    private final PartitionSpec partition;

    /** Order by columns. */
    @JsonProperty(FIELD_NAME_SORT_SPEC)
    private final SortSpec orderKeys;

    /** Interval definition, null if WITHIN clause is not defined. */
    @JsonProperty(FIELD_NAME_INTERVAL)
    private final @Nullable RexNode interval;

    @JsonCreator
    public MatchSpec(
            @JsonProperty(FIELD_NAME_PATTERN) RexNode pattern,
            @JsonProperty(FIELD_NAME_PATTERN_DEFINITIONS) Map<String, RexNode> patternDefinitions,
            @JsonProperty(FIELD_NAME_MEASURES) Map<String, RexNode> measures,
            @JsonProperty(FIELD_NAME_AFTER) RexNode after,
            @JsonProperty(FIELD_NAME_SUBSETS) Map<String, SortedSet<String>> subsets,
            @JsonProperty(FIELD_NAME_ALL_ROWS) boolean allRows,
            @JsonProperty(FIELD_NAME_PARTITION) PartitionSpec partition,
            @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec orderKeys,
            @JsonProperty(FIELD_NAME_INTERVAL) @Nullable RexNode interval) {
        this.pattern = checkNotNull(pattern);
        this.patternDefinitions = checkNotNull(patternDefinitions);
        this.measures = checkNotNull(measures);
        this.after = checkNotNull(after);
        this.subsets = checkNotNull(subsets);
        this.allRows = allRows;
        this.partition = checkNotNull(partition);
        this.orderKeys = checkNotNull(orderKeys);
        this.interval = interval;
    }

    @JsonIgnore
    public RexNode getPattern() {
        return pattern;
    }

    @JsonIgnore
    public Map<String, RexNode> getPatternDefinitions() {
        return patternDefinitions;
    }

    @JsonIgnore
    public Map<String, RexNode> getMeasures() {
        return measures;
    }

    @JsonIgnore
    public RexNode getAfter() {
        return after;
    }

    @JsonIgnore
    public Map<String, SortedSet<String>> getSubsets() {
        return subsets;
    }

    @JsonIgnore
    public boolean isAllRows() {
        return allRows;
    }

    @JsonIgnore
    public PartitionSpec getPartition() {
        return partition;
    }

    @JsonIgnore
    public SortSpec getOrderKeys() {
        return orderKeys;
    }

    @JsonIgnore
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
