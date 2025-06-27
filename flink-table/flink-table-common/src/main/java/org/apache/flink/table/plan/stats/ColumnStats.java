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

package org.apache.flink.table.plan.stats;

import org.apache.flink.annotation.PublicEvolving;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BinaryOperator;

/** Column statistics. */
@PublicEvolving
public final class ColumnStats {

    /** Unknown definition for column stats. */
    public static final ColumnStats UNKNOWN = Builder.builder().build();

    /** number of distinct values. */
    private final Long ndv;

    /** number of nulls. */
    private final Long nullCount;

    /** average length of column values. */
    private final Double avgLen;

    /** max length of column values. */
    private final Integer maxLen;

    /** max value of column values, null if the value is unknown or not comparable. */
    private final Comparable<?> max;

    /** min value of column values, null if the value is unknown or not comparable. */
    private final Comparable<?> min;

    public ColumnStats(
            Long ndv,
            Long nullCount,
            Double avgLen,
            Integer maxLen,
            Comparable<?> max,
            Comparable<?> min) {
        this.ndv = ndv;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
        this.max = max;
        this.min = min;
    }

    public Long getNdv() {
        return ndv;
    }

    public Long getNullCount() {
        return nullCount;
    }

    public Double getAvgLen() {
        return avgLen;
    }

    public Integer getMaxLen() {
        return maxLen;
    }

    public Comparable<?> getMax() {
        return max;
    }

    public Comparable<?> getMin() {
        return min;
    }

    public String toString() {
        List<String> columnStats = new ArrayList<>();
        if (ndv != null) {
            columnStats.add("ndv=" + ndv);
        }
        if (nullCount != null) {
            columnStats.add("nullCount=" + nullCount);
        }
        if (avgLen != null) {
            columnStats.add("avgLen=" + avgLen);
        }
        if (maxLen != null) {
            columnStats.add("maxLen=" + maxLen);
        }
        if (max != null) {
            columnStats.add("max=" + max);
        }
        if (min != null) {
            columnStats.add("min=" + min);
        }
        String columnStatsStr = String.join(", ", columnStats);
        return "ColumnStats(" + columnStatsStr + ")";
    }

    /**
     * Create a deep copy of "this" instance.
     *
     * @return a deep copy
     */
    public ColumnStats copy() {
        return new ColumnStats(
                this.ndv, this.nullCount, this.avgLen, this.maxLen, this.max, this.min);
    }

    /**
     * Merges two column stats. When the stats are unknown, whatever the other are, we need return
     * unknown stats. The unknown definition for column stats is null.
     *
     * @param other The other column stats to merge.
     * @return The merged column stats.
     */
    public ColumnStats merge(ColumnStats other, boolean isPartitionKey) {
        if (this == UNKNOWN || other == UNKNOWN) {
            return UNKNOWN;
        }
        Long ndv;
        if (isPartitionKey) {
            ndv = combineIfNonNull(Long::sum, this.ndv, other.ndv);
        } else {
            ndv = combineIfNonNull(Long::max, this.ndv, other.ndv);
        }

        Long nullCount = combineIfNonNull(Long::sum, this.nullCount, other.nullCount);
        Double avgLen = combineIfNonNull((a1, a2) -> (a1 + a2) / 2, this.avgLen, other.avgLen);
        Integer maxLen = combineIfNonNull(Math::max, this.maxLen, other.maxLen);

        @SuppressWarnings("unchecked")
        Comparable max =
                combineIfNonNull(
                        (c1, c2) -> ((Comparable) c1).compareTo(c2) > 0 ? c1 : c2,
                        this.max,
                        other.max);
        @SuppressWarnings("unchecked")
        Comparable min =
                combineIfNonNull(
                        (c1, c2) -> ((Comparable) c1).compareTo(c2) < 0 ? c1 : c2,
                        this.min,
                        other.min);

        return new ColumnStats(ndv, nullCount, avgLen, maxLen, max, min);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnStats that = (ColumnStats) o;
        return Objects.equals(ndv, that.ndv)
                && Objects.equals(nullCount, that.nullCount)
                && Objects.equals(avgLen, that.avgLen)
                && Objects.equals(maxLen, that.maxLen)
                && Objects.equals(max, that.max)
                && Objects.equals(min, that.min);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ndv, nullCount, avgLen, maxLen, max, min);
    }

    private static <T> T combineIfNonNull(BinaryOperator<T> op, T t1, T t2) {
        if (t1 == null || t2 == null) {
            return null;
        }
        return op.apply(t1, t2);
    }

    /** ColumnStats builder. */
    @PublicEvolving
    public static class Builder {
        private Long ndv = null;
        private Long nullCount = null;
        private Double avgLen = null;
        private Integer maxLen = null;
        private Comparable<?> max;
        private Comparable<?> min;

        public static Builder builder() {
            return new Builder();
        }

        public Builder setNdv(Long ndv) {
            this.ndv = ndv;
            return this;
        }

        public Builder setNullCount(Long nullCount) {
            this.nullCount = nullCount;
            return this;
        }

        public Builder setAvgLen(Double avgLen) {
            this.avgLen = avgLen;
            return this;
        }

        public Builder setMaxLen(Integer maxLen) {
            this.maxLen = maxLen;
            return this;
        }

        public Builder setMax(Comparable<?> max) {
            this.max = max;
            return this;
        }

        public Builder setMin(Comparable<?> min) {
            this.min = min;
            return this;
        }

        public ColumnStats build() {
            return new ColumnStats(ndv, nullCount, avgLen, maxLen, max, min);
        }
    }
}
