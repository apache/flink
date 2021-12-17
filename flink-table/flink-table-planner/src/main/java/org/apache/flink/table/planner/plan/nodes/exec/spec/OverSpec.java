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

import org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonDeserializer;
import org.apache.flink.table.planner.plan.nodes.exec.serde.RexWindowBoundJsonSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindowBound;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link OverSpec} describes a set of over aggregates.
 *
 * <p>This class corresponds to {@link org.apache.calcite.rel.core.Window} rel node, different from
 * Window rel, OverSpec requires all groups should have same partition.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OverSpec {
    public static final String FIELD_NAME_PARTITION = "partition";
    public static final String FIELD_NAME_GROUPS = "groups";
    public static final String FIELD_NAME_CONSTANTS = "constants";
    public static final String FIELD_NAME_ORIGINAL_INPUT_FIELDS = "originalInputFields";

    /** Describes the partition-by part, all groups in this over have same partition. */
    @JsonProperty(FIELD_NAME_PARTITION)
    private final PartitionSpec partition;

    /**
     * The groups in a over operator, each group has the same window specification with one or more
     * aggregate calls.
     */
    @JsonProperty(FIELD_NAME_GROUPS)
    private final List<GroupSpec> groups;

    /** List of constants that are additional inputs. */
    @JsonProperty(FIELD_NAME_CONSTANTS)
    private final List<RexLiteral> constants;

    /** The number of the over operator's input fields. */
    @JsonProperty(FIELD_NAME_ORIGINAL_INPUT_FIELDS)
    private final int originalInputFields;

    @JsonCreator
    public OverSpec(
            @JsonProperty(FIELD_NAME_PARTITION) PartitionSpec partition,
            @JsonProperty(FIELD_NAME_GROUPS) List<GroupSpec> groups,
            @JsonProperty(FIELD_NAME_CONSTANTS) List<RexLiteral> constants,
            @JsonProperty(FIELD_NAME_ORIGINAL_INPUT_FIELDS) int originalInputFields) {
        this.partition = checkNotNull(partition);
        this.groups = checkNotNull(groups);
        this.constants = checkNotNull(constants);
        this.originalInputFields = originalInputFields;
    }

    @JsonIgnore
    public PartitionSpec getPartition() {
        return partition;
    }

    @JsonIgnore
    public List<GroupSpec> getGroups() {
        return groups;
    }

    @JsonIgnore
    public List<RexLiteral> getConstants() {
        return constants;
    }

    @JsonIgnore
    public int getOriginalInputFields() {
        return originalInputFields;
    }

    /**
     * {@link GroupSpec} describes group of over aggregate calls that have the same window
     * specification.
     *
     * <p>The specification is defined by an upper and lower bound, and also has zero or more
     * order-by columns.
     *
     * <p>This class corresponds to {@link org.apache.calcite.rel.core.Window.Group}, but different
     * from Group, the partition spec is defined in OverSpec.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GroupSpec {
        public static final String FIELD_NAME_SORT_SPEC = "orderBy";
        public static final String FIELD_NAME_IS_ROWS = "isRows";
        public static final String FIELD_NAME_LOWER_BOUND = "lowerBound";
        public static final String FIELD_NAME_UPPER_BOUND = "upperBound";
        public static final String FIELD_NAME_AGG_CALLS = "aggCalls";

        /** Describes the order-by part. */
        @JsonProperty(FIELD_NAME_SORT_SPEC)
        private final SortSpec sort;

        /** If true, the group is in row clause, else the group is in range clause. */
        @JsonProperty(FIELD_NAME_IS_ROWS)
        private final boolean isRows;

        /** The lower bound of the window. */
        @JsonProperty(FIELD_NAME_LOWER_BOUND)
        @JsonSerialize(using = RexWindowBoundJsonSerializer.class)
        @JsonDeserialize(using = RexWindowBoundJsonDeserializer.class)
        private final RexWindowBound lowerBound;

        /** The upper bound of the window. */
        @JsonProperty(FIELD_NAME_UPPER_BOUND)
        @JsonSerialize(using = RexWindowBoundJsonSerializer.class)
        @JsonDeserialize(using = RexWindowBoundJsonDeserializer.class)
        private final RexWindowBound upperBound;

        /** The agg functions set. */
        @JsonProperty(FIELD_NAME_AGG_CALLS)
        private final List<AggregateCall> aggCalls;

        @JsonCreator
        public GroupSpec(
                @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sort,
                @JsonProperty(FIELD_NAME_IS_ROWS) boolean isRows,
                @JsonProperty(FIELD_NAME_LOWER_BOUND) RexWindowBound lowerBound,
                @JsonProperty(FIELD_NAME_UPPER_BOUND) RexWindowBound upperBound,
                @JsonProperty(FIELD_NAME_AGG_CALLS) List<AggregateCall> aggCalls) {
            this.sort = checkNotNull(sort);
            this.isRows = isRows;
            this.lowerBound = checkNotNull(lowerBound);
            this.upperBound = checkNotNull(upperBound);
            this.aggCalls = checkNotNull(aggCalls);
        }

        @JsonIgnore
        public SortSpec getSort() {
            return sort;
        }

        @JsonIgnore
        public boolean isRows() {
            return isRows;
        }

        @JsonIgnore
        public RexWindowBound getLowerBound() {
            return lowerBound;
        }

        @JsonIgnore
        public RexWindowBound getUpperBound() {
            return upperBound;
        }

        @JsonIgnore
        public List<AggregateCall> getAggCalls() {
            return aggCalls;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupSpec groupSpec = (GroupSpec) o;
            return isRows == groupSpec.isRows
                    && sort.equals(groupSpec.sort)
                    && lowerBound.equals(groupSpec.lowerBound)
                    && upperBound.equals(groupSpec.upperBound)
                    && aggCalls.equals(groupSpec.aggCalls);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sort, isRows, lowerBound, upperBound, aggCalls);
        }

        @Override
        public String toString() {
            return "Group{"
                    + "sort="
                    + sort
                    + ", isRows="
                    + isRows
                    + ", lowerBound="
                    + lowerBound
                    + ", upperBound="
                    + upperBound
                    + ", aggCalls="
                    + aggCalls
                    + '}';
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OverSpec overSpec = (OverSpec) o;
        return originalInputFields == overSpec.originalInputFields
                && partition.equals(overSpec.partition)
                && groups.equals(overSpec.groups)
                && constants.equals(overSpec.constants);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, groups, constants, originalInputFields);
    }

    @Override
    public String toString() {
        return "Over{"
                + "partition="
                + partition
                + ", groups="
                + groups
                + ", constants="
                + constants
                + ", originalInputFields="
                + originalInputFields
                + '}';
    }
}
