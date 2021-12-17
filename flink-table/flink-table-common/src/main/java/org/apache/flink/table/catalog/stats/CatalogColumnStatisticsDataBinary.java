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
}
