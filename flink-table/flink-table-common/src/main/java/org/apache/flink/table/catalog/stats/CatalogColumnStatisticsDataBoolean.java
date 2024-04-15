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

/** Column statistics value of boolean type. */
@PublicEvolving
public class CatalogColumnStatisticsDataBoolean extends CatalogColumnStatisticsDataBase {
    /** number of "true" values. */
    private final Long trueCount;

    /** number of "false" values. */
    private final Long falseCount;

    public CatalogColumnStatisticsDataBoolean(Long trueCount, Long falseCount, Long nullCount) {
        super(nullCount);
        this.trueCount = trueCount;
        this.falseCount = falseCount;
    }

    public CatalogColumnStatisticsDataBoolean(
            Long trueCount, Long falseCount, Long nullCount, Map<String, String> properties) {
        super(nullCount, properties);
        this.trueCount = trueCount;
        this.falseCount = falseCount;
    }

    public Long getTrueCount() {
        return trueCount;
    }

    public Long getFalseCount() {
        return falseCount;
    }

    public CatalogColumnStatisticsDataBoolean copy() {
        return new CatalogColumnStatisticsDataBoolean(
                trueCount, falseCount, getNullCount(), new HashMap<>(getProperties()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogColumnStatisticsDataBoolean that = (CatalogColumnStatisticsDataBoolean) o;
        return Objects.equals(trueCount, that.trueCount)
                && Objects.equals(falseCount, that.falseCount)
                && Objects.equals(getNullCount(), that.getNullCount());
    }

    @Override
    public int hashCode() {
        return Objects.hash(trueCount, falseCount, getNullCount());
    }

    @Override
    public String toString() {
        return "CatalogColumnStatisticsDataBoolean{"
                + "trueCount="
                + trueCount
                + ", falseCount="
                + falseCount
                + ", nullCount="
                + getNullCount()
                + '}';
    }
}
