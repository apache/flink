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

/** Column statistics value of long type. */
public class CatalogColumnStatisticsDataLong extends CatalogColumnStatisticsDataBase {
    /** mim value. */
    private final Long min;

    /** max value. */
    private final Long max;

    /** number of distinct values. */
    private final Long ndv;

    public CatalogColumnStatisticsDataLong(Long min, Long max, Long ndv, Long nullCount) {
        super(nullCount);
        this.min = min;
        this.max = max;
        this.ndv = ndv;
    }

    public CatalogColumnStatisticsDataLong(
            Long min, Long max, Long ndv, Long nullCount, Map<String, String> properties) {
        super(nullCount, properties);
        this.min = min;
        this.max = max;
        this.ndv = ndv;
    }

    public Long getMin() {
        return min;
    }

    public Long getMax() {
        return max;
    }

    public Long getNdv() {
        return ndv;
    }

    public CatalogColumnStatisticsDataLong copy() {
        return new CatalogColumnStatisticsDataLong(
                min, max, ndv, getNullCount(), new HashMap<>(getProperties()));
    }

    public static Builder builder() {
        return new Builder();
    }

    /** {@link CatalogColumnStatisticsDataLong} builder static inner class. */
    public static final class Builder
            implements CatalogColumnStatisticDataBuilder<CatalogColumnStatisticsDataLong> {
        private Long nullCount;
        private Map<String, String> properties;
        private Long min;
        private Long max;
        private Long ndv;

        private Builder() {}

        public Builder nullCount(Long nullCount) {
            this.nullCount = nullCount;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder min(Long min) {
            this.min = StatisticDataUtils.min(this.min, min);
            return this;
        }

        public Builder max(Long max) {
            this.max = StatisticDataUtils.max(this.max, max);
            return this;
        }

        public Builder ndv(Long ndv) {
            this.ndv = ndv;
            return this;
        }

        @Override
        public CatalogColumnStatisticsDataLong build() {
            return new CatalogColumnStatisticsDataLong(min, max, ndv, nullCount, properties);
        }
    }
}
