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
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.Map;

import static org.apache.flink.table.catalog.CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Utility class for catalog testing. TODO: Move util methods to CatalogTest and remove this class
 */
public class CatalogTestUtil {
    public static void checkEquals(CatalogTable t1, CatalogTable t2) {
        assertThat(t2.getTableKind()).isEqualTo(t1.getTableKind());

        assertThat(getResolvedSchema(t2)).isEqualTo(getResolvedSchema(t1));
        assertThat(t2.getComment()).isEqualTo(t1.getComment());
        assertThat(t2.getPartitionKeys()).isEqualTo(t1.getPartitionKeys());
        assertThat(t2.isPartitioned()).isEqualTo(t1.isPartitioned());

        // Hive tables may have properties created by itself
        // thus properties of Hive table is a super set of those in its corresponding Flink table
        if (isHiveTable(t1.getOptions())) {
            assertThat(
                            t2.getOptions().keySet().stream()
                                    .noneMatch(k -> k.startsWith(FLINK_PROPERTY_PREFIX)))
                    .isTrue();
            assertThat(t2.getOptions().entrySet().containsAll(t1.getOptions().entrySet())).isTrue();
        } else {
            assertThat(t2.getOptions()).isEqualTo(t1.getOptions());
        }
    }

    public static void checkEquals(CatalogView v1, CatalogView v2) {
        assertThat(v2.getTableKind()).isEqualTo(v1.getTableKind());

        assertThat(getResolvedSchema(v2)).isEqualTo(getResolvedSchema(v1));
        assertThat(v2.getComment()).isEqualTo(v1.getComment());
        assertThat(v2.getOriginalQuery()).isEqualTo(v1.getOriginalQuery());
        assertThat(v2.getExpandedQuery()).isEqualTo(v1.getExpandedQuery());

        // Hive tables may have properties created by itself
        // thus properties of Hive table is a super set of those in its corresponding Flink table
        if (isHiveTable(v1.getOptions())) {
            assertThat(
                            v2.getOptions().keySet().stream()
                                    .noneMatch(k -> k.startsWith(FLINK_PROPERTY_PREFIX)))
                    .isTrue();
            assertThat(v2.getOptions().entrySet().containsAll(v1.getOptions().entrySet())).isTrue();
        } else {
            assertThat(v2.getOptions()).isEqualTo(v1.getOptions());
        }
    }

    public static void checkEquals(CatalogPartition p1, CatalogPartition p2) {
        assertThat(p2.getClass()).isEqualTo(p1.getClass());
        assertThat(p2.getComment()).isEqualTo(p1.getComment());

        // Hive tables may have properties created by itself
        // thus properties of Hive table is a super set of those in its corresponding Flink table
        if (isHiveTable(p1.getProperties())) {
            assertThat(p2.getProperties().entrySet().containsAll(p1.getProperties().entrySet()))
                    .isTrue();
        } else {
            assertThat(p2.getProperties()).isEqualTo(p1.getProperties());
        }
    }

    public static void checkEquals(TableStats ts1, TableStats ts2) {
        assertThat(ts2.getRowCount()).isEqualTo(ts1.getRowCount());
        assertThat(ts2.getColumnStats()).hasSize(ts1.getColumnStats().size());
    }

    public static void checkEquals(CatalogDatabase d1, CatalogDatabase d2) {
        assertThat(d2.getClass()).isEqualTo(d1.getClass());
        assertThat(d2.getComment()).isEqualTo(d1.getComment());

        // d2 should contain all properties of d1's, and may or may not contain extra properties
        assertThat(d2.getProperties().entrySet().containsAll(d1.getProperties().entrySet()))
                .isTrue();
    }

    static void checkEquals(CatalogTableStatistics ts1, CatalogTableStatistics ts2) {
        assertThat(ts2.getRowCount()).isEqualTo(ts1.getRowCount());
        assertThat(ts2.getFileCount()).isEqualTo(ts1.getFileCount());
        assertThat(ts2.getTotalSize()).isEqualTo(ts1.getTotalSize());
        assertThat(ts2.getRawDataSize()).isEqualTo(ts1.getRawDataSize());
        assertThat(ts2.getProperties()).isEqualTo(ts1.getProperties());
    }

    static void checkEquals(CatalogColumnStatistics cs1, CatalogColumnStatistics cs2) {
        checkEquals(cs1.getColumnStatisticsData(), cs2.getColumnStatisticsData());
        assertThat(cs2.getProperties()).isEqualTo(cs1.getProperties());
    }

    private static void checkEquals(
            Map<String, CatalogColumnStatisticsDataBase> m1,
            Map<String, CatalogColumnStatisticsDataBase> m2) {
        assertThat(m2).hasSize(m1.size());
        for (Map.Entry<String, CatalogColumnStatisticsDataBase> entry : m2.entrySet()) {
            assertThat(m1).containsKey(entry.getKey());
            checkEquals(m2.get(entry.getKey()), entry.getValue());
        }
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataBase v1, CatalogColumnStatisticsDataBase v2) {
        assertThat(v2.getClass()).isEqualTo(v1.getClass());
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
        assertThat(v2.getFalseCount()).isEqualTo(v1.getFalseCount());
        assertThat(v2.getTrueCount()).isEqualTo(v1.getTrueCount());
        assertThat(v2.getNullCount()).isEqualTo(v1.getNullCount());
        assertThat(v2.getProperties()).isEqualTo(v1.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataLong v1, CatalogColumnStatisticsDataLong v2) {
        assertThat(v2.getMin()).isEqualTo(v1.getMin());
        assertThat(v2.getMax()).isEqualTo(v1.getMax());
        assertThat(v2.getNdv()).isEqualTo(v1.getNdv());
        assertThat(v2.getNullCount()).isEqualTo(v1.getNullCount());
        assertThat(v2.getProperties()).isEqualTo(v1.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataDouble v1, CatalogColumnStatisticsDataDouble v2) {
        assertThat(v2.getMin()).isCloseTo(v1.getMin(), within(0.05D));
        assertThat(v2.getMax()).isCloseTo(v1.getMax(), within(0.05D));
        assertThat(v2.getNdv()).isEqualTo(v1.getNdv());
        assertThat(v2.getNullCount()).isEqualTo(v1.getNullCount());
        assertThat(v2.getProperties()).isEqualTo(v1.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataString v1, CatalogColumnStatisticsDataString v2) {
        assertThat(v2.getMaxLength()).isEqualTo(v1.getMaxLength());
        assertThat(v2.getAvgLength()).isCloseTo(v1.getAvgLength(), within(0.05D));
        assertThat(v2.getNdv()).isEqualTo(v1.getNdv());
        assertThat(v2.getNullCount()).isEqualTo(v1.getNullCount());
        assertThat(v2.getProperties()).isEqualTo(v1.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataBinary v1, CatalogColumnStatisticsDataBinary v2) {
        assertThat(v2.getMaxLength()).isEqualTo(v1.getMaxLength());
        assertThat(v2.getAvgLength()).isCloseTo(v1.getAvgLength(), within(0.05D));
        assertThat(v2.getNullCount()).isEqualTo(v1.getNullCount());
        assertThat(v2.getProperties()).isEqualTo(v1.getProperties());
    }

    private static void checkEquals(
            CatalogColumnStatisticsDataDate v1, CatalogColumnStatisticsDataDate v2) {
        checkEquals(v1.getMin(), v2.getMin());
        checkEquals(v1.getMax(), v2.getMax());
        assertThat(v2.getNdv()).isEqualTo(v1.getNdv());
        assertThat(v2.getNullCount()).isEqualTo(v1.getNullCount());
        assertThat(v2.getProperties()).isEqualTo(v1.getProperties());
    }

    private static void checkEquals(Date v1, Date v2) {
        assertThat(v2.getDaysSinceEpoch()).isEqualTo(v1.getDaysSinceEpoch());
    }

    private static boolean isHiveTable(Map<String, String> properties) {
        return "hive".equalsIgnoreCase(properties.get(FactoryUtil.CONNECTOR.key()));
    }

    /**
     * We unify it to ResolvedSchema for comparing.
     *
     * @param catalogBaseTable The target catalog base table.
     * @return The resolved schema.
     */
    private static ResolvedSchema getResolvedSchema(CatalogBaseTable catalogBaseTable) {
        if (catalogBaseTable instanceof ResolvedCatalogBaseTable) {
            return ((ResolvedCatalogBaseTable<?>) catalogBaseTable).getResolvedSchema();
        } else {
            return catalogBaseTable.getUnresolvedSchema().resolve(new TestSchemaResolver());
        }
    }
}
