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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.stats.ValueInterval$;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.planner.utils.TestPartitionableSourceFactory;
import org.apache.flink.table.planner.utils.TestTableSource;
import org.apache.flink.table.utils.DateTimeUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for Catalog Statistics. */
public class CatalogStatisticsTest {

    private final String databaseName = "default_database";

    private final ResolvedSchema resolvedSchema =
            ResolvedSchema.physical(
                    Arrays.asList("b1", "l2", "s3", "d4", "dd5"),
                    Arrays.asList(
                            DataTypes.BOOLEAN(),
                            DataTypes.BIGINT(),
                            DataTypes.STRING(),
                            DataTypes.DATE(),
                            DataTypes.DOUBLE()));

    private TableEnvironment tEnv;
    private Catalog catalog;

    @Before
    public void setup() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tEnv = TableEnvironment.create(settings);
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).orElse(null);
        assertThat(catalog).isNotNull();
    }

    @Test
    public void testGetStatsFromCatalogForConnectorCatalogTable() throws Exception {
        final TableSchema tableSchema = TableSchema.fromResolvedSchema(resolvedSchema);
        catalog.createTable(
                new ObjectPath(databaseName, "T1"),
                ConnectorCatalogTable.source(new TestTableSource(true, tableSchema), true),
                false);
        catalog.createTable(
                new ObjectPath(databaseName, "T2"),
                ConnectorCatalogTable.source(new TestTableSource(true, tableSchema), true),
                false);

        alterTableStatistics(catalog, "T1");
        assertStatistics(tEnv, "T1");

        alterTableStatisticsWithUnknownRowCount(catalog, "T2");
        assertTableStatisticsWithUnknownRowCount(tEnv, "T2");
    }

    @Test
    public void testGetStatsFromCatalogForCatalogTableImpl() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.type", "filesystem");
        properties.put("connector.property-version", "1");
        properties.put("connector.path", "/path/to/csv");

        properties.put("format.type", "csv");
        properties.put("format.property-version", "1");
        properties.put("format.field-delimiter", ";");

        final Schema schema = Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();
        catalog.createTable(
                new ObjectPath(databaseName, "T1"),
                CatalogTable.of(schema, "", Collections.emptyList(), properties),
                false);
        catalog.createTable(
                new ObjectPath(databaseName, "T2"),
                CatalogTable.of(schema, "", Collections.emptyList(), properties),
                false);

        alterTableStatistics(catalog, "T1");
        assertStatistics(tEnv, "T1");

        alterTableStatisticsWithUnknownRowCount(catalog, "T2");
        assertTableStatisticsWithUnknownRowCount(tEnv, "T2");
    }

    private void alterTableStatistics(Catalog catalog, String tableName)
            throws TableNotExistException, TablePartitionedException {
        catalog.alterTableStatistics(
                new ObjectPath(databaseName, tableName),
                new CatalogTableStatistics(100, 10, 1000L, 2000L),
                true);
        catalog.alterTableColumnStatistics(
                new ObjectPath(databaseName, tableName), createColumnStats(), true);
    }

    @Test
    public void testGetPartitionStatsFromCatalog() throws Exception {
        TestPartitionableSourceFactory.createTemporaryTable(tEnv, "PartT", true);
        createPartitionStats("A", 1);
        createPartitionColumnStats("A", 1);
        createPartitionStats("A", 2);
        createPartitionColumnStats("A", 2);

        RelNode t1 =
                ((PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner())
                        .optimize(
                                TableTestUtil.toRelNode(
                                        tEnv.sqlQuery(
                                                "select id, name from PartT where part1 = 'A'")));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getRowCount(t1)).isEqualTo(200.0);
        assertThat(mq.getAverageColumnSizes(t1)).isEqualTo(Arrays.asList(8.0, 43.5));

        // long type
        assertThat(mq.getDistinctRowCount(t1, ImmutableBitSet.of(0), null)).isEqualTo(23.0);
        assertThat(mq.getColumnNullCount(t1, 0)).isEqualTo(154.0);
        assertThat(mq.getColumnInterval(t1, 0))
                .isEqualTo(
                        ValueInterval$.MODULE$.apply(
                                BigDecimal.valueOf(-123L),
                                BigDecimal.valueOf(763322L),
                                true,
                                true));

        // string type
        assertThat(mq.getDistinctRowCount(t1, ImmutableBitSet.of(1), null)).isEqualTo(20.0);
        assertThat(mq.getColumnNullCount(t1, 1)).isEqualTo(0.0);
        assertThat(mq.getColumnInterval(t1, 1)).isNull();
    }

    @Test
    public void testGetPartitionStatsWithUnknownRowCount() throws Exception {
        TestPartitionableSourceFactory.createTemporaryTable(tEnv, "PartT", true);
        createPartitionStats("A", 1, TableStats.UNKNOWN.getRowCount());
        createPartitionColumnStats("A", 1);
        createPartitionStats("A", 2);
        createPartitionColumnStats("A", 2);

        RelNode t1 =
                ((PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner())
                        .optimize(
                                TableTestUtil.toRelNode(
                                        tEnv.sqlQuery(
                                                "select id, name from PartT where part1 = 'A'")));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getRowCount(t1)).isEqualTo(100_000_000);
        assertThat(mq.getAverageColumnSizes(t1)).isEqualTo(Arrays.asList(4.0, 12.0));

        // long type
        assertThat(mq.getDistinctRowCount(t1, ImmutableBitSet.of(0), null)).isNull();
        assertThat(mq.getColumnNullCount(t1, 0)).isNull();
        assertThat(mq.getColumnInterval(t1, 0)).isNull();

        // string type
        assertThat(mq.getDistinctRowCount(t1, ImmutableBitSet.of(1), null)).isNull();
        assertThat(mq.getColumnNullCount(t1, 1)).isNull();
        assertThat(mq.getColumnInterval(t1, 1)).isNull();
    }

    @Test
    public void testGetPartitionStatsWithUnknownColumnStats() throws Exception {
        TestPartitionableSourceFactory.createTemporaryTable(tEnv, "PartT", true);
        createPartitionStats("A", 1);
        createPartitionStats("A", 2);
        createPartitionColumnStats("A", 2);

        RelNode t1 =
                ((PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner())
                        .optimize(
                                TableTestUtil.toRelNode(
                                        tEnv.sqlQuery(
                                                "select id, name from PartT where part1 = 'A'")));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getRowCount(t1)).isEqualTo(200.0);

        // long type
        assertThat(mq.getDistinctRowCount(t1, ImmutableBitSet.of(0), null)).isNull();
        assertThat(mq.getColumnNullCount(t1, 0)).isNull();
        assertThat(mq.getColumnInterval(t1, 0)).isNull();

        // string type
        assertThat(mq.getDistinctRowCount(t1, ImmutableBitSet.of(1), null)).isNull();
        assertThat(mq.getColumnNullCount(t1, 1)).isNull();
    }

    @Test
    public void testGetPartitionStatsWithSomeUnknownColumnStats() throws Exception {
        TestPartitionableSourceFactory.createTemporaryTable(tEnv, "PartT", true);
        createPartitionStats("A", 1);
        createPartitionColumnStats("A", 1, true);
        createPartitionStats("A", 2);
        createPartitionColumnStats("A", 2);

        RelNode t1 =
                ((PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner())
                        .optimize(
                                TableTestUtil.toRelNode(
                                        tEnv.sqlQuery(
                                                "select id, name from PartT where part1 = 'A'")));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getRowCount(t1)).isEqualTo(200.0);

        // long type
        assertThat(mq.getDistinctRowCount(t1, ImmutableBitSet.of(0), null)).isNull();
        assertThat(mq.getColumnNullCount(t1, 0)).isNull();
        assertThat(mq.getColumnInterval(t1, 0)).isNull();

        // string type
        assertThat(mq.getDistinctRowCount(t1, ImmutableBitSet.of(1), null)).isNull();
        assertThat(mq.getColumnNullCount(t1, 1)).isNull();
    }

    private void createPartitionStats(String part1, int part2) throws Exception {
        createPartitionStats(part1, part2, 100);
    }

    private void createPartitionStats(String part1, int part2, long rowCount) throws Exception {
        ObjectPath path = ObjectPath.fromString("default_database.PartT");

        LinkedHashMap<String, String> partSpecMap = new LinkedHashMap<>();
        partSpecMap.put("part1", part1);
        partSpecMap.put("part2", String.valueOf(part2));
        CatalogPartitionSpec partSpec = new CatalogPartitionSpec(partSpecMap);
        catalog.createPartition(
                path, partSpec, new CatalogPartitionImpl(new HashMap<>(), ""), true);
        catalog.alterPartitionStatistics(
                path, partSpec, new CatalogTableStatistics(rowCount, 10, 1000L, 2000L), true);
    }

    private void createPartitionColumnStats(String part1, int part2) throws Exception {
        createPartitionColumnStats(part1, part2, false);
    }

    private void createPartitionColumnStats(String part1, int part2, boolean unknown)
            throws Exception {
        ObjectPath path = ObjectPath.fromString("default_database.PartT");
        LinkedHashMap<String, String> partSpecMap = new LinkedHashMap<>();
        partSpecMap.put("part1", part1);
        partSpecMap.put("part2", String.valueOf(part2));
        CatalogPartitionSpec partSpec = new CatalogPartitionSpec(partSpecMap);

        CatalogColumnStatisticsDataLong longColStats =
                new CatalogColumnStatisticsDataLong(-123L, 763322L, 23L, 77L);
        CatalogColumnStatisticsDataString stringColStats =
                new CatalogColumnStatisticsDataString(152L, 43.5D, 20L, 0L);
        Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>();
        colStatsMap.put(
                "id",
                unknown
                        ? new CatalogColumnStatisticsDataLong(null, null, null, null)
                        : longColStats);
        colStatsMap.put(
                "name",
                unknown
                        ? new CatalogColumnStatisticsDataString(null, null, null, null)
                        : stringColStats);
        catalog.alterPartitionColumnStatistics(
                path, partSpec, new CatalogColumnStatistics(colStatsMap), true);
    }

    private CatalogColumnStatistics createColumnStats() {
        CatalogColumnStatisticsDataBoolean booleanColStats =
                new CatalogColumnStatisticsDataBoolean(55L, 45L, 5L);
        CatalogColumnStatisticsDataLong longColStats =
                new CatalogColumnStatisticsDataLong(-123L, 763322L, 23L, 77L);
        CatalogColumnStatisticsDataString stringColStats =
                new CatalogColumnStatisticsDataString(152L, 43.5D, 20L, 0L);
        CatalogColumnStatisticsDataDate dateColStats =
                new CatalogColumnStatisticsDataDate(new Date(71L), new Date(17923L), 100L, 0L);
        CatalogColumnStatisticsDataDouble doubleColStats =
                new CatalogColumnStatisticsDataDouble(-123.35D, 7633.22D, 73L, 27L);
        Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>(6);
        colStatsMap.put("b1", booleanColStats);
        colStatsMap.put("l2", longColStats);
        colStatsMap.put("s3", stringColStats);
        colStatsMap.put("d4", dateColStats);
        colStatsMap.put("dd5", doubleColStats);
        return new CatalogColumnStatistics(colStatsMap);
    }

    private void assertStatistics(TableEnvironment tEnv, String tableName) {
        RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from " + tableName));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getRowCount(t1)).isEqualTo(100.0);
        assertColumnStatistics(t1, mq);
    }

    private void assertColumnStatistics(RelNode rel, FlinkRelMetadataQuery mq) {
        assertThat(mq.getAverageColumnSizes(rel))
                .isEqualTo(Arrays.asList(1.0, 8.0, 43.5, 12.0, 8.0));

        // boolean type
        assertThat(mq.getDistinctRowCount(rel, ImmutableBitSet.of(0), null)).isEqualTo(2.0);
        assertThat(mq.getColumnNullCount(rel, 0)).isEqualTo(5.0);
        assertThat(mq.getColumnInterval(rel, 0)).isNull();

        // long type
        assertThat(mq.getDistinctRowCount(rel, ImmutableBitSet.of(1), null)).isEqualTo(23.0);
        assertThat(mq.getColumnNullCount(rel, 1)).isEqualTo(77.0);
        assertThat(mq.getColumnInterval(rel, 1))
                .isEqualTo(
                        ValueInterval$.MODULE$.apply(
                                BigDecimal.valueOf(-123L),
                                BigDecimal.valueOf(763322L),
                                true,
                                true));

        // string type
        assertThat(mq.getDistinctRowCount(rel, ImmutableBitSet.of(2), null)).isEqualTo(20.0);
        assertThat(mq.getColumnNullCount(rel, 2)).isEqualTo(0.0);
        assertThat(mq.getColumnInterval(rel, 2)).isNull();

        // date type
        assertThat(mq.getDistinctRowCount(rel, ImmutableBitSet.of(3), null)).isEqualTo(100.0);
        assertThat(mq.getColumnNullCount(rel, 3)).isEqualTo(0.0);
        assertThat(mq.getColumnInterval(rel, 3))
                .isEqualTo(
                        ValueInterval$.MODULE$.apply(
                                java.sql.Date.valueOf(DateTimeUtils.formatDate(71)),
                                java.sql.Date.valueOf(DateTimeUtils.formatDate(17923)),
                                true,
                                true));

        // double type
        assertThat(mq.getDistinctRowCount(rel, ImmutableBitSet.of(4), null)).isEqualTo(73.0);
        assertThat(mq.getColumnNullCount(rel, 4)).isEqualTo(27.0);
        assertThat(mq.getColumnInterval(rel, 4))
                .isEqualTo(
                        ValueInterval$.MODULE$.apply(
                                BigDecimal.valueOf(-123.35),
                                BigDecimal.valueOf(7633.22),
                                true,
                                true));
    }

    private void alterTableStatisticsWithUnknownRowCount(Catalog catalog, String tableName)
            throws TableNotExistException, TablePartitionedException {
        catalog.alterTableStatistics(
                new ObjectPath(databaseName, tableName),
                new CatalogTableStatistics(
                        CatalogTableStatistics.UNKNOWN.getRowCount(), 1, 10000, 200000),
                true);
        catalog.alterTableColumnStatistics(
                new ObjectPath(databaseName, tableName), createColumnStats(), true);
    }

    private void assertTableStatisticsWithUnknownRowCount(TableEnvironment tEnv, String tableName) {
        RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from " + tableName));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        // 1E8 is default value defined in FlinkPreparingTableBase
        assertThat(mq.getRowCount(t1)).isEqualTo(1E8);
        assertColumnStatistics(t1, mq);
    }
}
