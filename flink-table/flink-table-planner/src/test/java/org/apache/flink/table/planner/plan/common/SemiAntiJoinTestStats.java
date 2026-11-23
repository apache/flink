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

package org.apache.flink.table.planner.plan.common;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.TableTestUtil;

import java.util.Map;

/**
 * Tables modeled on TPC-H q16: partsupp, part and supplier. The key columns carry NDVs, so the
 * partsupp-part foreign key join is estimated to keep every partsupp row.
 *
 * <p>A semi/anti join without equi-join keys has quadratic CPU cost, so it must stay above the
 * partsupp-part join instead of being pushed below it.
 */
public final class SemiAntiJoinTestStats {

    private static final TypeInformation<?> INT = BasicTypeInfo.INT_TYPE_INFO;
    private static final TypeInformation<?> STRING = BasicTypeInfo.STRING_TYPE_INFO;

    private SemiAntiJoinTestStats() {}

    public static void addTables(TableTestUtil util) {
        util.addTableSource(
                "partsupp",
                new TypeInformation<?>[] {INT, INT},
                new String[] {"ps_partkey", "ps_suppkey"},
                statistic(
                        800_000L,
                        Map.of("ps_partkey", keyStats(200_000L), "ps_suppkey", keyStats(10_000L))));
        util.addTableSource(
                "part",
                new TypeInformation<?>[] {INT, STRING, INT},
                new String[] {"p_partkey", "p_brand", "p_size"},
                statistic(
                        200_000L,
                        Map.of(
                                "p_partkey",
                                keyStats(200_000L),
                                "p_brand",
                                keyStats(25L),
                                "p_size",
                                keyStats(50L))));
        util.addTableSource(
                "supplier",
                new TypeInformation<?>[] {INT, STRING},
                new String[] {"s_suppkey", "s_comment"},
                statistic(10_000L, Map.of("s_suppkey", keyStats(10_000L))));
    }

    private static FlinkStatistic statistic(long rowCount, Map<String, ColumnStats> columnStats) {
        return FlinkStatistic.builder().tableStats(new TableStats(rowCount, columnStats)).build();
    }

    private static ColumnStats keyStats(long ndv) {
        return ColumnStats.Builder.builder().setNdv(ndv).setNullCount(0L).build();
    }
}
