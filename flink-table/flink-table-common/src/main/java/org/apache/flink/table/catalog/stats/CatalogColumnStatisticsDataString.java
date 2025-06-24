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

import org.apache.flink.annotation.PublicEvolving;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Column statistics value of string type. */
@PublicEvolving
public class CatalogColumnStatisticsDataString extends CatalogColumnStatisticsDataBase {
    /** max length of all values. */
    private final Long maxLength;

    /** average length of all values. */
    private final Double avgLength;

    /** number of distinct values. */
    private final Long ndv;

    public CatalogColumnStatisticsDataString(
            Long maxLength, Double avgLength, Long ndv, Long nullCount) {
        super(nullCount);
        this.maxLength = maxLength;
        this.avgLength = avgLength;
        this.ndv = ndv;
    }

    public CatalogColumnStatisticsDataString(
            Long maxLength,
            Double avgLength,
            Long ndv,
            Long nullCount,
            Map<String, String> properties) {
        super(nullCount, properties);
        this.maxLength = maxLength;
        this.avgLength = avgLength;
        this.ndv = ndv;
    }

    public Long getMaxLength() {
        return maxLength;
    }

    public Double getAvgLength() {
        return avgLength;
    }

    public Long getNdv() {
        return ndv;
    }

    public CatalogColumnStatisticsDataString copy() {
        return new CatalogColumnStatisticsDataString(
                maxLength, avgLength, ndv, getNullCount(), new HashMap<>(getProperties()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogColumnStatisticsDataString that = (CatalogColumnStatisticsDataString) o;
        return Objects.equals(maxLength, that.maxLength)
                && Objects.equals(avgLength, that.avgLength)
                && Objects.equals(ndv, that.ndv)
                && Objects.equals(getNullCount(), that.getNullCount());
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxLength, avgLength, ndv, getNullCount());
    }

    @Override
    public String toString() {
        return "CatalogColumnStatisticsDataString{"
                + "maxLength="
                + maxLength
                + ", avgLength="
                + avgLength
                + ", ndv="
                + ndv
                + ", nullCount="
                + getNullCount()
                + '}';
    }
}
