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

package org.apache.flink.table.catalog.stats;

import java.util.HashMap;
import java.util.Map;

/** Column statistics value of binary type. */
public class CatalogColumnStatisticsDataBinary extends CatalogColumnStatisticsDataBase {
    /** max length of all values. */
    private final Long maxLength;

    /** average length of all values. */
    private final Double avgLength;

    public CatalogColumnStatisticsDataBinary(Long maxLength, Double avgLength, Long nullCount) {
        super(nullCount);
        this.maxLength = maxLength;
        this.avgLength = avgLength;
    }

    public CatalogColumnStatisticsDataBinary(
            Long maxLength, Double avgLength, Long nullCount, Map<String, String> properties) {
        super(nullCount, properties);
        this.maxLength = maxLength;
        this.avgLength = avgLength;
    }

    public Long getMaxLength() {
        return maxLength;
    }

    public Double getAvgLength() {
        return avgLength;
    }

    public CatalogColumnStatisticsDataBinary copy() {
        return new CatalogColumnStatisticsDataBinary(
                maxLength, avgLength, getNullCount(), new HashMap<>(getProperties()));
    }

    public static Builder builder() {
        return new Builder();
    }

    /** {@link CatalogColumnStatisticsDataBinary} builder static inner class. */
    public static final class Builder
            implements CatalogColumnStatisticDataBuilder<CatalogColumnStatisticsDataBinary> {
        private Long nullCount;
        private Map<String, String> properties;
        private Long maxLength;
        private Double avgLength;

        private Long count = 0L;

        private Builder() {}

        public Builder nullCount(Long nullCount) {
            this.nullCount = nullCount;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder maxLength(Long maxLength) {
            this.maxLength = StatisticDataUtils.max(this.maxLength, maxLength);
            return this;
        }

        public Builder accumulateAvgLength(Long newLength) {
            this.avgLength =
                    StatisticDataUtils.acculmulateAverage(
                            this.avgLength, this.count, newLength, 1L);

            this.count++;
            return this;
        }

        @Override
        public CatalogColumnStatisticsDataBinary build() {
            return new CatalogColumnStatisticsDataBinary(
                    maxLength, avgLength, nullCount, properties);
        }
    }
}
