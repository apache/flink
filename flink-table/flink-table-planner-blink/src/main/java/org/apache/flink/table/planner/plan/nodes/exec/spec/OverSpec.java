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
public class OverSpec {
    /** Describes the partition-by part, all groups in this over have same partition. */
    private final PartitionSpec partition;
    /**
     * The groups in a over operator, each group has the same window specification with one or more
     * aggregate calls.
     */
    private final List<GroupSpec> groups;
    /** List of constants that are additional inputs. */
    private final List<RexLiteral> constants;
    /** The number of the over operator's input fields. */
    private final int originalInputFields;

    public OverSpec(
            PartitionSpec partition,
            List<GroupSpec> groups,
            List<RexLiteral> constants,
            int originalInputFields) {
        this.partition = checkNotNull(partition);
        this.groups = checkNotNull(groups);
        this.constants = checkNotNull(constants);
        this.originalInputFields = originalInputFields;
    }

    public PartitionSpec getPartition() {
        return partition;
    }

    public List<GroupSpec> getGroups() {
        return groups;
    }

    public List<RexLiteral> getConstants() {
        return constants;
    }

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
    public static class GroupSpec {
        /** Describes the order-by part. */
        private final SortSpec sort;
        /** If true, the group is in row clause, else the group is in range clause. */
        private final boolean isRows;
        /** The lower bound of the window. */
        private final RexWindowBound lowerBound;
        /** The upper bound of the window. */
        private final RexWindowBound upperBound;
        /** The agg functions set. */
        private final List<AggregateCall> aggCalls;

        public GroupSpec(
                SortSpec sort,
                boolean isRows,
                RexWindowBound lowerBound,
                RexWindowBound upperBound,
                List<AggregateCall> aggCalls) {
            this.sort = checkNotNull(sort);
            this.isRows = isRows;
            this.lowerBound = checkNotNull(lowerBound);
            this.upperBound = (upperBound);
            this.aggCalls = checkNotNull(aggCalls);
        }

        public SortSpec getSort() {
            return sort;
        }

        public boolean isRows() {
            return isRows;
        }

        public RexWindowBound getLowerBound() {
            return lowerBound;
        }

        public RexWindowBound getUpperBound() {
            return upperBound;
        }

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
