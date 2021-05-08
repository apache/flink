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

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Column statistics of a table or partition. */
public class CatalogColumnStatistics {
    public static final CatalogColumnStatistics UNKNOWN =
            new CatalogColumnStatistics(new HashMap<>());

    /** A map of column name and column statistic data. */
    private final Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData;

    private final Map<String, String> properties;

    public CatalogColumnStatistics(
            Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData) {
        this(columnStatisticsData, new HashMap<>());
    }

    public CatalogColumnStatistics(
            Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData,
            Map<String, String> properties) {
        checkNotNull(columnStatisticsData);
        checkNotNull(properties);

        this.columnStatisticsData = columnStatisticsData;
        this.properties = properties;
    }

    public Map<String, CatalogColumnStatisticsDataBase> getColumnStatisticsData() {
        return columnStatisticsData;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    /**
     * Create a deep copy of "this" instance.
     *
     * @return a deep copy
     */
    public CatalogColumnStatistics copy() {
        Map<String, CatalogColumnStatisticsDataBase> copy =
                new HashMap<>(columnStatisticsData.size());
        for (Map.Entry<String, CatalogColumnStatisticsDataBase> entry :
                columnStatisticsData.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().copy());
        }
        return new CatalogColumnStatistics(copy, new HashMap<>(this.properties));
    }
}
