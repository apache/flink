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

/** Column statistics value of long type. */
@PublicEvolving
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogColumnStatisticsDataLong that = (CatalogColumnStatisticsDataLong) o;
        return Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Objects.equals(ndv, that.ndv)
                && Objects.equals(getNullCount(), that.getNullCount());
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, ndv, getNullCount());
    }

    @Override
    public String toString() {
        return "CatalogColumnStatisticsDataLong{"
                + "min="
                + min
                + ", max="
                + max
                + ", ndv="
                + ndv
                + ", nullCount="
                + getNullCount()
                + '}';
    }
}
