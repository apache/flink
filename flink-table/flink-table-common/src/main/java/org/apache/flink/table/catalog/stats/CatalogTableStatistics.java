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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Statistics for a non-partitioned table or a partition of a partitioned table. */
@PublicEvolving
public class CatalogTableStatistics {
    public static final CatalogTableStatistics UNKNOWN = new CatalogTableStatistics(-1, -1, -1, -1);

    /** The number of rows in the table or partition. */
    private final long rowCount;

    /** The number of files on disk. */
    private final long fileCount;

    /** The total size in bytes. */
    private final long totalSize;

    /** The raw data size (size when loaded in memory) in bytes. */
    private final long rawDataSize;

    private final Map<String, String> properties;

    public CatalogTableStatistics(long rowCount, long fileCount, long totalSize, long rawDataSize) {
        this(rowCount, fileCount, totalSize, rawDataSize, Collections.emptyMap());
    }

    public CatalogTableStatistics(
            long rowCount,
            long fileCount,
            long totalSize,
            long rawDataSize,
            Map<String, String> properties) {
        this.rowCount = rowCount;
        this.fileCount = fileCount;
        this.totalSize = totalSize;
        this.rawDataSize = rawDataSize;
        this.properties = properties;
    }

    /** The number of rows. */
    public long getRowCount() {
        return this.rowCount;
    }

    public long getFileCount() {
        return this.fileCount;
    }

    public long getTotalSize() {
        return this.totalSize;
    }

    public long getRawDataSize() {
        return this.rawDataSize;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public static CatalogTableStatistics accumulateStatistics(
            List<CatalogTableStatistics> tableStatisticsList) {
        if (tableStatisticsList.isEmpty()) {
            return UNKNOWN;
        }
        CatalogTableStatistics catalogTableStatistics = tableStatisticsList.get(0);
        long rowCount = catalogTableStatistics.getRowCount();
        long fileCount = catalogTableStatistics.getFileCount();
        long totalSize = catalogTableStatistics.getTotalSize();
        long rawDataSize = catalogTableStatistics.getRawDataSize();
        for (int i = 1; i < tableStatisticsList.size(); i++) {
            catalogTableStatistics = tableStatisticsList.get(i);
            rowCount += catalogTableStatistics.getRowCount();
            fileCount += catalogTableStatistics.getFileCount();
            totalSize += catalogTableStatistics.getTotalSize();
            rawDataSize += catalogTableStatistics.getRawDataSize();
        }
        return new CatalogTableStatistics(rowCount, fileCount, totalSize, rawDataSize);
    }

    /**
     * Create a deep copy of "this" instance.
     *
     * @return a deep copy
     */
    public CatalogTableStatistics copy() {
        return new CatalogTableStatistics(
                this.rowCount,
                this.fileCount,
                this.totalSize,
                this.rawDataSize,
                new HashMap<>(this.properties));
    }

    @Override
    public String toString() {
        return "CatalogTableStatistics{"
                + "rowCount="
                + rowCount
                + ", fileCount="
                + fileCount
                + ", totalSize="
                + totalSize
                + ", rawDataSize="
                + rawDataSize
                + ", properties="
                + properties
                + '}';
    }
}
