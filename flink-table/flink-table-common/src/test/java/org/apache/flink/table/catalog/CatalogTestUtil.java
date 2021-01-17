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

package org.apache.flink.table.catalog;

import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.Map;

import static org.apache.flink.table.catalog.config.CatalogConfig.FLINK_PROPERTY_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for catalog testing. TODO: Move util methods to CatalogTest and remove this class
 */
public class CatalogTestUtil {
    public static void checkEquals(CatalogTable t1, CatalogTable t2) {
        assertEquals(t1.getClass(), t2.getClass());
        assertEquals(t1.getSchema(), t2.getSchema());
        assertEquals(t1.getComment(), t2.getComment());
        assertEquals(t1.getPartitionKeys(), t2.getPartitionKeys());
        assertEquals(t1.isPartitioned(), t2.isPartitioned());

        assertEquals(
                t1.getProperties().get(CatalogConfig.IS_GENERIC),
                t2.getProperties().get(CatalogConfig.IS_GENERIC));

        // Hive tables may have properties created by itself
        // thus properties of Hive table is a super set of those in its corresponding Flink table
        if (Boolean.valueOf(t1.getProperties().get(CatalogConfig.IS_GENERIC))) {
            assertEquals(t1.getProperties(), t2.getProperties());
        } else {
            assertTrue(
                    t2.getProperties().keySet().stream()
                            .noneMatch(k -> k.startsWith(FLINK_PROPERTY_PREFIX)));
            assertTrue(t2.getProperties().entrySet().containsAll(t1.getProperties().entrySet()));
        }
    }

    public static void checkEquals(CatalogView v1, CatalogView v2) {
        assertEquals(v1.getClass(), v2.getClass());
        assertEquals(v1.getSchema(), v1.getSchema());
        assertEquals(v1.getComment(), v2.getComment());
        assertEquals(v1.getOriginalQuery(), v2.getOriginalQuery());
        assertEquals(v1.getExpandedQuery(), v2.getExpandedQuery());

        // Hive tables may have properties created by itself
        // thus properties of Hive table is a super set of those in its corresponding Flink table
        if (Boolean.valueOf(v1.getProperties().get(CatalogConfig.IS_GENERIC))) {
            assertEquals(v1.getProperties(), v2.getProperties());
        } else {
            assertTrue(
                    v2.getProperties().keySet().stream()
                            .noneMatch(k -> k.startsWith(FLINK_PROPERTY_PREFIX)));
            assertTrue(v2.getProperties().entrySet().containsAll(v1.getProperties().entrySet()));
        }
    }

    public static void checkEquals(CatalogPartition p1, CatalogPartition p2) {
        assertEquals(p1.getClass(), p2.getClass());
        assertEquals(p1.getComment(), p2.getComment());

        // Hive tables may have properties created by itself
        // thus properties of Hive table is a super set of those in its corresponding Flink table
        if (Boolean.valueOf(p1.getProperties().get(CatalogConfig.IS_GENERIC))) {
            assertEquals(p1.getProperties(), p2.getProperties());
        } else {
            assertTrue(p2.getProperties().entrySet().containsAll(p1.getProperties().entrySet()));
        }
    }

    public static void checkEquals(TableStats ts1, TableStats ts2) {
        assertEquals(ts1.getRowCount(), ts2.getRowCount());
        assertEquals(ts1.getColumnStats().size(), ts2.getColumnStats().size());
    }

    public static void checkEquals(CatalogDatabase d1, CatalogDatabase d2) {
        assertEquals(d1.getClass(), d2.getClass());
        assertEquals(d1.getComment(), d2.getComment());

        // d2 should contain all properties of d1's, and may or may not contain extra properties
        assertTrue(d2.getProperties().entrySet().containsAll(d1.getProperties().entrySet()));
    }

    static void checkEquals(CatalogTableStatistics ts1, CatalogTableStatistics ts2) {
        assertEquals(ts1.getRowCount(), ts2.getRowCount());
        assertEquals(ts1.getFileCount(), ts2.getFileCount());
        assertEquals(ts1.getTotalSize(), ts2.getTotalSize());
        assertEquals(ts1.getRawDataSize(), ts2.getRawDataSize());
        assertEquals(ts1.getProperties(), ts2.getProperties());
    }

    static void checkEquals(CatalogColumnStatistics cs1, CatalogColumnStatistics cs2) {
        checkEquals(cs1.getColumnStatisticsData(), cs2.getColumnStatisticsData());
        assertEquals(cs1.getProperties(), cs2.getProperties());
    }

    private static void checkEquals(
            Map<String, CatalogColumnStatisticsDataBase> m1,
            Map<String, CatalogColumnStatisticsDataBase> m2) {
        assertEquals(m1.size(), m2.size());
        for (Map.Entry<String, CatalogColumnStatisticsDataBase> entry : m2.entrySet()) {
            assertTrue(m1.containsKey(entry.getKey()));
            checkEquals(m2.get(entry.getKey()), entry.getValue());
        }
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataBase v1, CatalogColumnStatisticsDataBase v2) {
        assertEquals(v1.getClass(), v2.getClass());
        if (v1 instanceof CatalogColumnStatisticsDataBoolean) {
            checkEquals(
                    (CatalogColumnStatisticsDataBoolean) v1,
                    (CatalogColumnStatisticsDataBoolean) v2);
        } else if (v1 instanceof CatalogColumnStatisticsDataLong) {
            checkEquals((CatalogColumnStatisticsDataLong) v1, (CatalogColumnStatisticsDataLong) v2);
        } else if (v1 instanceof CatalogColumnStatisticsDataBinary) {
            checkEquals(
                    (CatalogColumnStatisticsDataBinary) v1, (CatalogColumnStatisticsDataBinary) v2);
        } else if (v1 instanceof CatalogColumnStatisticsDataDate) {
            checkEquals((CatalogColumnStatisticsDataDate) v1, (CatalogColumnStatisticsDataDate) v2);
        } else if (v1 instanceof CatalogColumnStatisticsDataString) {
            checkEquals(
                    (CatalogColumnStatisticsDataString) v1, (CatalogColumnStatisticsDataString) v2);
        } else if (v1 instanceof CatalogColumnStatisticsDataDouble) {
            checkEquals(
                    (CatalogColumnStatisticsDataDouble) v1, (CatalogColumnStatisticsDataDouble) v2);
        }
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataBoolean v1, CatalogColumnStatisticsDataBoolean v2) {
        assertEquals(v1.getFalseCount(), v2.getFalseCount());
        assertEquals(v1.getTrueCount(), v2.getTrueCount());
        assertEquals(v1.getNullCount(), v2.getNullCount());
        assertEquals(v1.getProperties(), v2.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataLong v1, CatalogColumnStatisticsDataLong v2) {
        assertEquals(v1.getMin(), v2.getMin());
        assertEquals(v1.getMax(), v2.getMax());
        assertEquals(v1.getNdv(), v2.getNdv());
        assertEquals(v1.getNullCount(), v2.getNullCount());
        assertEquals(v1.getProperties(), v2.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataDouble v1, CatalogColumnStatisticsDataDouble v2) {
        assertEquals(v1.getMin(), v2.getMin(), 0.05D);
        assertEquals(v1.getMax(), v2.getMax(), 0.05D);
        assertEquals(v1.getNdv(), v2.getNdv());
        assertEquals(v1.getNullCount(), v2.getNullCount());
        assertEquals(v1.getProperties(), v2.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataString v1, CatalogColumnStatisticsDataString v2) {
        assertEquals(v1.getMaxLength(), v2.getMaxLength());
        assertEquals(v1.getAvgLength(), v2.getAvgLength(), 0.05D);
        assertEquals(v1.getNdv(), v2.getNdv());
        assertEquals(v1.getNullCount(), v2.getNullCount());
        assertEquals(v1.getProperties(), v2.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataBinary v1, CatalogColumnStatisticsDataBinary v2) {
        assertEquals(v1.getMaxLength(), v2.getMaxLength());
        assertEquals(v1.getAvgLength(), v2.getAvgLength(), 0.05D);
        assertEquals(v1.getNullCount(), v2.getNullCount());
        assertEquals(v1.getProperties(), v2.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataDate v1, CatalogColumnStatisticsDataDate v2) {
        checkEquals(v1.getMin(), v2.getMin());
        checkEquals(v1.getMax(), v2.getMax());
        assertEquals(v1.getNdv(), v2.getNdv());
        assertEquals(v1.getNullCount(), v2.getNullCount());
        assertEquals(v1.getProperties(), v2.getProperties());
    }

    private static void checkEquals(Date v1, Date v2) {
        assertEquals(v1.getDaysSinceEpoch(), v2.getDaysSinceEpoch());
    }
}
