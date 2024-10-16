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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTestUtil;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
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
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.hive.util.AlterTableOp.CHANGE_TBL_PROPS;
import static org.apache.flink.table.catalog.hive.util.Constants.ALTER_TABLE_OP;
import static org.apache.flink.table.catalog.hive.util.Constants.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Test for HiveCatalog on Hive metadata. */
class HiveCatalogHiveMetadataTest extends HiveCatalogMetadataTestBase {

    @BeforeAll
    static void init() {
        catalog = HiveTestUtils.createHiveCatalog();
        catalog.open();
    }

    // =====================
    // HiveCatalog doesn't support streaming table operation. Ignore this test in CatalogTest.
    // =====================

    public void testCreateTable_Streaming() throws Exception {}

    @Test
    // verifies that input/output formats and SerDe are set for Hive tables
    void testCreateTable_StorageFormatSet() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        Table hiveTable = ((HiveCatalog) catalog).getHiveTable(path1);
        String inputFormat = hiveTable.getSd().getInputFormat();
        String outputFormat = hiveTable.getSd().getOutputFormat();
        String serde = hiveTable.getSd().getSerdeInfo().getSerializationLib();
        assertThat(inputFormat).isNotBlank();
        assertThat(outputFormat).isNotBlank();
        assertThat(serde).isNotBlank();
    }

    // ------ table and column stats ------

    @Test
    void testViewCompatibility() throws Exception {
        // we always store view schema via properties now
        // make sure non-generic views created previously can still be used
        catalog.createDatabase(db1, createDb(), false);
        Table hiveView =
                org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
                        path1.getDatabaseName(), path1.getObjectName());
        // mark as a view
        hiveView.setTableType(TableType.VIRTUAL_VIEW.name());
        final String originQuery = "view origin query";
        final String expandedQuery = "view expanded query";
        hiveView.setViewOriginalText(originQuery);
        hiveView.setViewExpandedText(expandedQuery);
        // set schema in SD
        Schema schema =
                Schema.newBuilder()
                        .fromFields(
                                new String[] {"i", "s"},
                                new AbstractDataType[] {DataTypes.INT(), DataTypes.STRING()})
                        .build();
        List<FieldSchema> fields = new ArrayList<>();
        for (Schema.UnresolvedColumn column : schema.getColumns()) {
            String name = column.getName();
            DataType type = HiveTestUtils.getType(column);
            fields.add(
                    new FieldSchema(
                            name, HiveTypeUtil.toHiveTypeInfo(type, true).getTypeName(), null));
        }
        hiveView.getSd().setCols(fields);
        // test mark as non-generic with is_generic
        hiveView.getParameters().put(CatalogPropertiesUtil.IS_GENERIC, "false");
        // add some other properties
        hiveView.getParameters().put("k1", "v1");

        ((HiveCatalog) catalog).client.createTable(hiveView);
        CatalogBaseTable baseTable = catalog.getTable(path1);
        assertThat(baseTable).isInstanceOf(CatalogView.class);
        CatalogView catalogView = (CatalogView) baseTable;
        assertThat(catalogView.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogView.getOriginalQuery()).isEqualTo(originQuery);
        assertThat(catalogView.getExpandedQuery()).isEqualTo(expandedQuery);
        assertThat(catalogView.getOptions().get("k1")).isEqualTo("v1");

        // test mark as non-generic with connector
        hiveView.setDbName(path3.getDatabaseName());
        hiveView.setTableName(path3.getObjectName());
        hiveView.getParameters().remove(CatalogPropertiesUtil.IS_GENERIC);
        hiveView.getParameters().put(CONNECTOR.key(), IDENTIFIER);

        ((HiveCatalog) catalog).client.createTable(hiveView);
        baseTable = catalog.getTable(path3);
        assertThat(baseTable).isInstanceOf(CatalogView.class);
        catalogView = (CatalogView) baseTable;
        assertThat(catalogView.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogView.getOriginalQuery()).isEqualTo(originQuery);
        assertThat(catalogView.getExpandedQuery()).isEqualTo(expandedQuery);
        assertThat(catalogView.getOptions().get("k1")).isEqualTo("v1");
    }

    @Test
    void testAlterTableColumnStatistics() throws Exception {
        String hiveVersion = ((HiveCatalog) catalog).getHiveVersion();
        boolean supportDateStats = hiveVersion.compareTo(HiveShimLoader.HIVE_VERSION_V2_3_0) >= 0;
        catalog.createDatabase(db1, createDb(), false);
        List<Column> columns = new ArrayList<>();
        columns.add(Column.physical("first", DataTypes.STRING()));
        columns.add(Column.physical("second", DataTypes.INT()));
        columns.add(Column.physical("third", DataTypes.BOOLEAN()));
        columns.add(Column.physical("fourth", DataTypes.DOUBLE()));
        columns.add(Column.physical("fifth", DataTypes.BIGINT()));
        columns.add(Column.physical("sixth", DataTypes.BYTES()));
        columns.add(Column.physical("seventh", DataTypes.DECIMAL(10, 3)));
        columns.add(Column.physical("eighth", DataTypes.DECIMAL(30, 3)));
        if (supportDateStats) {
            columns.add(Column.physical("ninth", DataTypes.DATE()));
        }
        ResolvedSchema resolvedSchema = new ResolvedSchema(columns, new ArrayList<>(), null);
        CatalogTable catalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                TEST_COMMENT,
                                new ArrayList<>(),
                                getBatchTableProperties()),
                        resolvedSchema);
        catalog.createTable(path1, catalogTable, false);
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsDataBaseMap = new HashMap<>();
        columnStatisticsDataBaseMap.put(
                "first", new CatalogColumnStatisticsDataString(10L, 5.2, 3L, 100L));
        columnStatisticsDataBaseMap.put(
                "second", new CatalogColumnStatisticsDataLong(0L, 1000L, 3L, 0L));
        columnStatisticsDataBaseMap.put(
                "third", new CatalogColumnStatisticsDataBoolean(15L, 20L, 3L));
        columnStatisticsDataBaseMap.put(
                "fourth", new CatalogColumnStatisticsDataDouble(15.02, 20.01, 3L, 10L));
        columnStatisticsDataBaseMap.put(
                "fifth", new CatalogColumnStatisticsDataLong(0L, 20L, 3L, 2L));
        columnStatisticsDataBaseMap.put(
                "sixth", new CatalogColumnStatisticsDataBinary(150L, 20D, 3L));
        columnStatisticsDataBaseMap.put(
                "seventh", new CatalogColumnStatisticsDataDouble(1.23, 99.456, 100L, 0L));
        columnStatisticsDataBaseMap.put(
                "eighth", new CatalogColumnStatisticsDataDouble(0.123, 123456.789, 5723L, 19L));
        if (supportDateStats) {
            columnStatisticsDataBaseMap.put(
                    "ninth",
                    new CatalogColumnStatisticsDataDate(new Date(71L), new Date(17923L), 132L, 0L));
        }
        CatalogColumnStatistics catalogColumnStatistics =
                new CatalogColumnStatistics(columnStatisticsDataBaseMap);
        catalog.alterTableColumnStatistics(path1, catalogColumnStatistics, false);

        checkEquals(catalogColumnStatistics, catalog.getTableColumnStatistics(path1));
    }

    @Test
    void testAlterPartitionColumnStatistics() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable catalogTable = createPartitionedTable();
        catalog.createTable(path1, catalogTable, false);
        CatalogPartitionSpec partitionSpec =
                new CatalogPartitionSpec(
                        new HashMap<String, String>() {
                            {
                                put("second", "2010-04-21 09:45:00");
                                put("third", "2000");
                            }
                        });
        catalog.createPartition(path1, partitionSpec, createPartition(), true);
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsDataBaseMap = new HashMap<>();
        columnStatisticsDataBaseMap.put(
                "first", new CatalogColumnStatisticsDataString(10L, 5.2, 3L, 100L));
        CatalogColumnStatistics catalogColumnStatistics =
                new CatalogColumnStatistics(columnStatisticsDataBaseMap);
        catalog.alterPartitionColumnStatistics(
                path1, partitionSpec, catalogColumnStatistics, false);

        checkEquals(
                catalogColumnStatistics,
                catalog.getPartitionColumnStatistics(path1, partitionSpec));
    }

    @Test
    void testHiveStatistics() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        checkStatistics(0, -1);
        checkStatistics(1, 1);
        checkStatistics(1000, 1000);
    }

    @Test
    void testCreateTableWithConstraints() throws Exception {
        assumeThat(HiveVersionTestUtil.HIVE_310_OR_LATER).isTrue();
        HiveCatalog hiveCatalog = (HiveCatalog) catalog;
        hiveCatalog.createDatabase(db1, createDb(), false);
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("x", DataTypes.INT().notNull()),
                                Column.physical("y", DataTypes.TIMESTAMP(9).notNull()),
                                Column.physical("z", DataTypes.BIGINT())),
                        new ArrayList<>(),
                        org.apache.flink.table.catalog.UniqueConstraint.primaryKey(
                                "pk_name", Collections.singletonList("x")));

        hiveCatalog.createTable(
                path1,
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                null,
                                new ArrayList<>(),
                                getBatchTableProperties()),
                        resolvedSchema),
                false);
        CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(path1);
        assertThat(catalogTable.getUnresolvedSchema().getPrimaryKey())
                .as("PK not present")
                .isPresent();
        Schema.UnresolvedPrimaryKey pk = catalogTable.getUnresolvedSchema().getPrimaryKey().get();
        assertThat(pk.getConstraintName()).isEqualTo("pk_name");
        assertThat(pk.getColumnNames()).containsExactly("x");

        List<Schema.UnresolvedColumn> columns = catalogTable.getUnresolvedSchema().getColumns();
        assertThat(HiveTestUtils.getType(columns.get(0)).getLogicalType().isNullable()).isFalse();
        assertThat(HiveTestUtils.getType(columns.get(1)).getLogicalType().isNullable()).isFalse();
        assertThat(HiveTestUtils.getType(columns.get(2)).getLogicalType().isNullable()).isTrue();

        hiveCatalog.dropDatabase(db1, false, true);
    }

    @Override
    @Test
    public void testAlterPartition() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);

        assertThat(catalog.listPartitions(path1)).containsExactly(createPartitionSpec());
        CatalogPartition cp = catalog.getPartition(path1, createPartitionSpec());
        CatalogTestUtil.checkEquals(createPartition(), cp);
        assertThat(cp.getProperties().get("k")).isNull();

        CatalogPartition another = createPartition();
        another.getProperties().put("k", "v");
        another.getProperties().put(ALTER_TABLE_OP, CHANGE_TBL_PROPS.name());

        catalog.alterPartition(path1, createPartitionSpec(), another, false);

        assertThat(catalog.listPartitions(path1)).containsExactly(createPartitionSpec());

        cp = catalog.getPartition(path1, createPartitionSpec());

        CatalogTestUtil.checkEquals(another, cp);
        assertThat(cp.getProperties().get("k")).isEqualTo("v");
    }

    private void checkStatistics(int inputStat, int expectStat) throws Exception {
        catalog.dropTable(path1, true);

        Map<String, String> properties = new HashMap<>();
        properties.put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
        properties.put(StatsSetupConst.ROW_COUNT, String.valueOf(inputStat));
        properties.put(StatsSetupConst.NUM_FILES, String.valueOf(inputStat));
        properties.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(inputStat));
        properties.put(StatsSetupConst.RAW_DATA_SIZE, String.valueOf(inputStat));
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(Column.physical("f0", DataTypes.INT())),
                        new ArrayList<>(),
                        null);
        CatalogTable resolveCatalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                "",
                                new ArrayList<>(),
                                properties),
                        resolvedSchema);
        catalog.createTable(path1, resolveCatalogTable, false);

        CatalogTableStatistics statistics = catalog.getTableStatistics(path1);
        assertThat(statistics.getRowCount()).isEqualTo(expectStat);
        assertThat(statistics.getFileCount()).isEqualTo(expectStat);
        assertThat(statistics.getRawDataSize()).isEqualTo(expectStat);
        assertThat(statistics.getTotalSize()).isEqualTo(expectStat);
    }

    @Test
    public void testBulkGetPartitionStatistics() throws Exception {
        final List<CatalogPartitionSpec> catalogPartitionSpecList = prepareCatalogPartition();
        List<CatalogTableStatistics> partitionStatistics =
                catalog.bulkGetPartitionStatistics(path1, catalogPartitionSpecList);
        // check the statistic for all partitions
        for (CatalogTableStatistics catalogTableStatistics : partitionStatistics) {
            // since we haven't put statistic to the partition, the statistic should be unknown
            assertThat(catalogTableStatistics).isEqualTo(CatalogTableStatistics.UNKNOWN);
        }

        // now, we put statistic to partition
        List<CatalogTableStatistics> expectedStatistic = new ArrayList<>();
        for (int i = 0; i < catalogPartitionSpecList.size(); i++) {
            CatalogTableStatistics statistics =
                    new CatalogTableStatistics((long) i + 1, i, i, i + 2);
            expectedStatistic.add(statistics);
            catalog.alterPartitionStatistics(
                    path1, catalogPartitionSpecList.get(i), statistics, false);
        }

        // get the statistic again
        partitionStatistics = catalog.bulkGetPartitionStatistics(path1, catalogPartitionSpecList);
        assertThat(partitionStatistics.size()).isEqualTo(expectedStatistic.size());
        for (int i = 0; i < partitionStatistics.size(); i++) {
            // we can't alter Hive's fileCount/totalSize by alterPartitionStatistics,
            // so, only check rowCount/rawDataSize
            assertThat(partitionStatistics.get(i).getRowCount())
                    .isEqualTo(expectedStatistic.get(i).getRowCount());
            assertThat(partitionStatistics.get(i).getRawDataSize())
                    .isEqualTo(expectedStatistic.get(i).getRawDataSize());
        }
    }

    @Test
    public void testBulkGetPartitionColumnStatistics() throws Exception {
        final List<CatalogPartitionSpec> catalogPartitionSpecList = prepareCatalogPartition();

        List<CatalogColumnStatistics> tableColumnStatistics =
                catalog.bulkGetPartitionColumnStatistics(path1, catalogPartitionSpecList);

        for (int i = 0; i < catalogPartitionSpecList.size(); i++) {
            CatalogPartitionSpec catalogPartitionSpec = catalogPartitionSpecList.get(i);
            // the non-partition column statistic should be empty since we haven't put any
            // statistic to the partition
            Map<String, CatalogColumnStatisticsDataBase> columnActualStatistics =
                    tableColumnStatistics.get(i).getColumnStatisticsData();
            assertThat(columnActualStatistics.get("first")).isNull();
            assertThat(columnActualStatistics.get("four")).isNull();
            assertThat(columnActualStatistics.get("five")).isNull();

            checkPartitionColumnStatistic(
                    catalogPartitionSpec,
                    (CatalogColumnStatisticsDataDate) columnActualStatistics.get("second"),
                    (CatalogColumnStatisticsDataLong) columnActualStatistics.get("third"));
        }

        // put statistic for non-partition column
        List<Map<String, CatalogColumnStatisticsDataBase>> nonPartitionColumnsExpectStatisticList =
                new ArrayList<>();
        for (int i = 0; i < catalogPartitionSpecList.size(); i++) {
            CatalogPartitionSpec spec = catalogPartitionSpecList.get(i);
            Map<String, CatalogColumnStatisticsDataBase> nonPartitionColumnsStatistic =
                    new HashMap<>();
            nonPartitionColumnsStatistic.put(
                    "first",
                    new CatalogColumnStatisticsDataString(
                            (long) i, (double) i, (long) i, (long) i));
            nonPartitionColumnsStatistic.put(
                    "four", new CatalogColumnStatisticsDataBoolean((long) i, (long) i, (long) i));
            nonPartitionColumnsStatistic.put(
                    "five",
                    new CatalogColumnStatisticsDataDouble(
                            (double) i, (double) i, (long) i, (long) i));
            nonPartitionColumnsExpectStatisticList.add(nonPartitionColumnsStatistic);
            catalog.alterPartitionColumnStatistics(
                    path1, spec, new CatalogColumnStatistics(nonPartitionColumnsStatistic), false);
        }

        tableColumnStatistics =
                catalog.bulkGetPartitionColumnStatistics(path1, catalogPartitionSpecList);

        // check statistic for each partition
        for (int i = 0; i < catalogPartitionSpecList.size(); i++) {
            CatalogPartitionSpec catalogPartitionSpec = catalogPartitionSpecList.get(i);
            Map<String, CatalogColumnStatisticsDataBase> columnActualStatistics =
                    tableColumnStatistics.get(i).getColumnStatisticsData();
            // check the statistic for non-partition column
            checkColumnStatistics(
                    columnActualStatistics.get("first"),
                    nonPartitionColumnsExpectStatisticList.get(i).get("first"));
            checkColumnStatistics(
                    columnActualStatistics.get("four"),
                    nonPartitionColumnsExpectStatisticList.get(i).get("four"));
            checkColumnStatistics(
                    columnActualStatistics.get("five"),
                    nonPartitionColumnsExpectStatisticList.get(i).get("five"));

            // check the statistic for partition column
            checkPartitionColumnStatistic(
                    catalogPartitionSpec,
                    (CatalogColumnStatisticsDataDate) columnActualStatistics.get("second"),
                    (CatalogColumnStatisticsDataLong) columnActualStatistics.get("third"));
        }
    }

    @Test
    public void testBulkGetNullValuePartitionColumnStatistics() throws Exception {
        prepareCatalogPartition();
        List<CatalogPartitionSpec> catalogPartitionSpecs = new ArrayList<>();
        List<Map<String, CatalogColumnStatisticsDataBase>> columnExpectStatisticsList =
                new ArrayList<>();
        // we mock four partition, (null, 0), (null, 1), (2022-8-8, null), (2022-8-9, null)
        // add twos partition: (null, 0), (null, 1)
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("second", null);
        partitionSpec.put("third", "0");
        catalogPartitionSpecs.add(new CatalogPartitionSpec(new HashMap<>(partitionSpec)));
        // expect statistic for this partition is as follows:
        columnExpectStatisticsList.add(
                new HashMap<String, CatalogColumnStatisticsDataBase>() {
                    {
                        // we haven't put statistic to the partition, so the nullCount should be
                        // null
                        put("second", new CatalogColumnStatisticsDataDate(null, null, 1L, null));
                        put("third", new CatalogColumnStatisticsDataLong(0L, 0L, 1L, 0L));
                    }
                });

        createPartition(partitionSpec);
        partitionSpec.put("third", "1");
        createPartition(partitionSpec);
        catalogPartitionSpecs.add(new CatalogPartitionSpec(new HashMap<>(partitionSpec)));
        // expect statistic for this partition is as follows:
        columnExpectStatisticsList.add(
                new HashMap<String, CatalogColumnStatisticsDataBase>() {
                    {
                        put("second", new CatalogColumnStatisticsDataDate(null, null, 1L, null));
                        put("third", new CatalogColumnStatisticsDataLong(1L, 1L, 1L, 0L));
                    }
                });

        partitionSpec = new HashMap<>();
        // add twos partition: (2022-8-8, null), (2022-8-9, null)
        partitionSpec.put("second", "2022-8-8");
        partitionSpec.put("third", null);
        createPartition(partitionSpec);
        catalogPartitionSpecs.add(new CatalogPartitionSpec(new HashMap<>(partitionSpec)));
        // expect statistic for this partition is as follows:
        Date expectDate = new Date(java.sql.Date.valueOf("2022-8-8").toLocalDate().toEpochDay());
        columnExpectStatisticsList.add(
                new HashMap<String, CatalogColumnStatisticsDataBase>() {
                    {
                        put(
                                "second",
                                new CatalogColumnStatisticsDataDate(
                                        expectDate, expectDate, 1L, 0L));
                        put("third", new CatalogColumnStatisticsDataLong(null, null, 1L, null));
                    }
                });

        partitionSpec.put("second", "2022-8-9");
        createPartition(partitionSpec);
        catalogPartitionSpecs.add(new CatalogPartitionSpec(new HashMap<>(partitionSpec)));
        // expect statistic for this partition is as follows:
        Date expectDate1 = new Date(java.sql.Date.valueOf("2022-8-9").toLocalDate().toEpochDay());
        columnExpectStatisticsList.add(
                new HashMap<String, CatalogColumnStatisticsDataBase>() {
                    {
                        put(
                                "second",
                                new CatalogColumnStatisticsDataDate(
                                        expectDate1, expectDate1, 1L, 0L));
                        put("third", new CatalogColumnStatisticsDataLong(null, null, 1L, null));
                    }
                });

        // check the statistic for the partition column
        List<CatalogColumnStatistics> catalogColumnActualStatisticsList =
                catalog.bulkGetPartitionColumnStatistics(path1, catalogPartitionSpecs);
        for (int i = 0; i < catalogColumnActualStatisticsList.size(); i++) {
            Map<String, CatalogColumnStatisticsDataBase> columnActualStatistics =
                    catalogColumnActualStatisticsList.get(i).getColumnStatisticsData();
            assertThat(columnActualStatistics).isEqualTo(columnExpectStatisticsList.get(i));
        }

        // now we put statistic to the partition
        // (null, 0): rowCount 1
        catalog.alterPartitionStatistics(
                path1, catalogPartitionSpecs.get(0), new CatalogTableStatistics(1, 1, 1, 1), false);
        // (null, 1): rowCount 2
        catalog.alterPartitionStatistics(
                path1, catalogPartitionSpecs.get(1), new CatalogTableStatistics(2, 1, 1, 1), false);
        columnExpectStatisticsList
                .get(0)
                .put("second", new CatalogColumnStatisticsDataDate(null, null, 1L, 3L));
        columnExpectStatisticsList
                .get(1)
                .put("second", new CatalogColumnStatisticsDataDate(null, null, 1L, 3L));

        // (2022-8-8, null): rowCount 3
        catalog.alterPartitionStatistics(
                path1, catalogPartitionSpecs.get(2), new CatalogTableStatistics(3, 1, 1, 1), false);
        // (2022-8-9, null): rowCount 4
        catalog.alterPartitionStatistics(
                path1, catalogPartitionSpecs.get(3), new CatalogTableStatistics(4, 1, 1, 1), false);
        columnExpectStatisticsList
                .get(2)
                .put("third", new CatalogColumnStatisticsDataLong(null, null, 1L, 7L));
        columnExpectStatisticsList
                .get(3)
                .put("third", new CatalogColumnStatisticsDataLong(null, null, 1L, 7L));

        // check the statistic for the partition column again
        catalogColumnActualStatisticsList =
                catalog.bulkGetPartitionColumnStatistics(path1, catalogPartitionSpecs);
        for (int i = 0; i < catalogColumnActualStatisticsList.size(); i++) {
            Map<String, CatalogColumnStatisticsDataBase> columnActualStatistics =
                    catalogColumnActualStatisticsList.get(i).getColumnStatisticsData();
            assertThat(columnActualStatistics).isEqualTo(columnExpectStatisticsList.get(i));
        }
    }

    private void checkPartitionColumnStatistic(
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogColumnStatisticsDataDate secondColumnActualStatistics,
            CatalogColumnStatisticsDataLong thirdColumnActualStatistics) {
        // expect date for second column
        Date expectSecondDate =
                new Date(
                        java.sql.Date.valueOf(catalogPartitionSpec.getPartitionSpec().get("second"))
                                .toLocalDate()
                                .toEpochDay());
        checkColumnStatistics(
                secondColumnActualStatistics,
                new CatalogColumnStatisticsDataDate(expectSecondDate, expectSecondDate, 1L, 0L));

        // expect value for third column
        Long expectThirdValue = Long.valueOf(catalogPartitionSpec.getPartitionSpec().get("third"));
        checkColumnStatistics(
                thirdColumnActualStatistics,
                new CatalogColumnStatisticsDataLong(expectThirdValue, expectThirdValue, 1L, 0L));
    }

    // ------ utils ------

    /** Prepare catalog partition, which will create partition table, and create partitions. */
    private List<CatalogPartitionSpec> prepareCatalogPartition() throws Exception {
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("first", DataTypes.STRING()),
                                Column.physical("four", DataTypes.BOOLEAN()),
                                Column.physical("five", DataTypes.DOUBLE()),
                                Column.physical("second", DataTypes.DATE()),
                                Column.physical("third", DataTypes.INT())),
                        Collections.emptyList(),
                        null);
        catalog.createDatabase(db1, createDb(), false);
        final CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        TEST_COMMENT,
                        createPartitionKeys(),
                        getBatchTableProperties());
        CatalogTable catalogTable = new ResolvedCatalogTable(origin, resolvedSchema);

        catalog.createTable(path1, catalogTable, false);

        int secondPartitionNum = 10;
        int thirdPartitionNum = 5;
        List<Map<String, String>> partitionSpecs = new ArrayList<>();
        final String testDatePrefix = "2010-04-";
        Map<String, String> partitionSpec;

        // create partitions, the number of partitions is: secondPartitionNum * thirdPartitionNum
        for (int i = 0; i < secondPartitionNum; i++) {
            for (int j = 0; j < thirdPartitionNum; j++) {
                partitionSpec = new HashMap<>();
                partitionSpec.put("second", testDatePrefix + (i + 1));
                partitionSpec.put("third", Integer.toString(j));
                createPartition(partitionSpec);
                partitionSpecs.add(partitionSpec);
            }
        }
        return partitionSpecs.stream().map(CatalogPartitionSpec::new).collect(Collectors.toList());
    }

    private void checkColumnStatistics(
            CatalogColumnStatisticsDataBase actual, CatalogColumnStatisticsDataBase expect) {
        assertThat(actual).isEqualTo(expect);
    }

    private void createPartition(Map<String, String> partitionSpec) throws Exception {
        catalog.createPartition(
                path1, new CatalogPartitionSpec(partitionSpec), createPartition(), true);
    }

    @Override
    protected boolean isGeneric() {
        return false;
    }

    @Override
    public CatalogTable createStreamingTable() {
        throw new UnsupportedOperationException("Hive table cannot be streaming.");
    }

    @Override
    protected CatalogFunction createFunction() {
        return new CatalogFunctionImpl(GenericUDFAbs.class.getName());
    }

    @Override
    protected CatalogFunction createAnotherFunction() {
        return new CatalogFunctionImpl(UDFRand.class.getName());
    }

    @Override
    protected boolean supportsModels() {
        return false;
    }
}
