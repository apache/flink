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

import org.apache.flink.table.catalog.exceptions.CatalogException;
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
import org.apache.flink.table.functions.TestGenericUDF;
import org.apache.flink.table.functions.TestSimpleUDF;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for GenericInMemoryCatalog. */
class GenericInMemoryCatalogTest extends CatalogTestBase {

    @BeforeAll
    static void init() {
        catalog = new GenericInMemoryCatalog(TEST_CATALOG_NAME);
        catalog.open();
    }

    // ------ tables ------

    @Test
    void testDropTable_partitionedTable() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        CatalogPartition catalogPartition = createPartition();
        CatalogPartitionSpec catalogPartitionSpec = createPartitionSpec();
        catalog.createPartition(path1, catalogPartitionSpec, catalogPartition, false);

        assertThat(catalog.tableExists(path1)).isTrue();

        catalog.dropTable(path1, false);

        assertThat(catalog.tableExists(path1)).isFalse();
        assertThat(catalog.partitionExists(path1, catalogPartitionSpec)).isFalse();
    }

    @Test
    void testRenameTable_partitionedTable() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createPartitionedTable();
        catalog.createTable(path1, table, false);
        CatalogPartition catalogPartition = createPartition();
        CatalogPartitionSpec catalogPartitionSpec = createPartitionSpec();
        catalog.createPartition(path1, catalogPartitionSpec, catalogPartition, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));
        assertThat(catalog.partitionExists(path1, catalogPartitionSpec)).isTrue();

        catalog.renameTable(path1, t2, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path3));
        assertThat(catalog.partitionExists(path3, catalogPartitionSpec)).isTrue();
        assertThat(catalog.tableExists(path1)).isFalse();
        assertThat(catalog.partitionExists(path1, catalogPartitionSpec)).isFalse();
    }

    // ------ statistics ------

    @Test
    void testStatistics() throws Exception {
        // Table related
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createTable();
        catalog.createTable(path1, table, false);

        CatalogTestUtil.checkEquals(
                catalog.getTableStatistics(path1), CatalogTableStatistics.UNKNOWN);
        CatalogTestUtil.checkEquals(
                catalog.getTableColumnStatistics(path1), CatalogColumnStatistics.UNKNOWN);

        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(5, 2, 100, 575);
        catalog.alterTableStatistics(path1, tableStatistics, false);
        CatalogTestUtil.checkEquals(tableStatistics, catalog.getTableStatistics(path1));
        CatalogColumnStatistics columnStatistics = createColumnStats();
        catalog.alterTableColumnStatistics(path1, columnStatistics, false);
        CatalogTestUtil.checkEquals(columnStatistics, catalog.getTableColumnStatistics(path1));

        // Partition related
        catalog.createDatabase(db2, createDb(), false);
        CatalogTable table2 = createPartitionedTable();
        catalog.createTable(path2, table2, false);
        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        catalog.createPartition(path2, partitionSpec, createPartition(), false);

        CatalogTestUtil.checkEquals(
                catalog.getPartitionStatistics(path2, partitionSpec),
                CatalogTableStatistics.UNKNOWN);
        CatalogTestUtil.checkEquals(
                catalog.getPartitionColumnStatistics(path2, partitionSpec),
                CatalogColumnStatistics.UNKNOWN);

        catalog.alterPartitionStatistics(path2, partitionSpec, tableStatistics, false);
        CatalogTestUtil.checkEquals(
                tableStatistics, catalog.getPartitionStatistics(path2, partitionSpec));
        catalog.alterPartitionColumnStatistics(path2, partitionSpec, columnStatistics, false);
        CatalogTestUtil.checkEquals(
                columnStatistics, catalog.getPartitionColumnStatistics(path2, partitionSpec));

        // Clean up
        catalog.dropTable(path1, false);
        catalog.dropDatabase(db1, false, false);
        catalog.dropTable(path2, false);
        catalog.dropDatabase(db2, false, false);
    }

    @Test
    void testBulkGetPartitionStatistics() throws Exception {
        // create table
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table1 = createPartitionedTable();
        catalog.createTable(path1, table1, false);
        // create two partition specs
        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        catalog.createPartition(path1, partitionSpec, createPartition(), false);
        CatalogPartitionSpec anotherPartitionSpec = createAnotherPartitionSpec();
        catalog.createPartition(path1, anotherPartitionSpec, createPartition(), false);

        List<CatalogTableStatistics> catalogTableStatistics =
                catalog.bulkGetPartitionStatistics(
                        path1, Arrays.asList(partitionSpec, anotherPartitionSpec));
        // got statistic from catalog should be unknown since no statistic has been put into
        // partition
        for (CatalogTableStatistics statistics : catalogTableStatistics) {
            CatalogTestUtil.checkEquals(statistics, CatalogTableStatistics.UNKNOWN);
        }

        // put statistic for partition
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(5, 2, 100, 575);
        CatalogTableStatistics anotherTableStatistics = new CatalogTableStatistics(1, 1, 1, 5);
        catalog.alterPartitionStatistics(path1, partitionSpec, tableStatistics, false);
        catalog.alterPartitionStatistics(
                path1, anotherPartitionSpec, anotherTableStatistics, false);

        catalogTableStatistics =
                catalog.bulkGetPartitionStatistics(
                        path1, Arrays.asList(partitionSpec, anotherPartitionSpec));
        CatalogTestUtil.checkEquals(catalogTableStatistics.get(0), tableStatistics);
        CatalogTestUtil.checkEquals(catalogTableStatistics.get(1), anotherTableStatistics);
    }

    @Test
    void testBulkGetPartitionColumnStatistics() throws Exception {
        // create table
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table1 = createPartitionedTable();
        catalog.createTable(path1, table1, false);
        // create two partition specs
        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        catalog.createPartition(path1, partitionSpec, createPartition(), false);
        CatalogPartitionSpec anotherPartitionSpec = createAnotherPartitionSpec();
        catalog.createPartition(path1, anotherPartitionSpec, createPartition(), false);
        List<CatalogPartitionSpec> catalogPartitionSpecs =
                Arrays.asList(partitionSpec, anotherPartitionSpec);

        List<CatalogColumnStatistics> catalogColumnStatistics =
                catalog.bulkGetPartitionColumnStatistics(path1, catalogPartitionSpecs);
        // got statistic from catalog should be unknown since no statistic has been put into
        // partition
        for (CatalogColumnStatistics statistics : catalogColumnStatistics) {
            CatalogTestUtil.checkEquals(statistics, CatalogColumnStatistics.UNKNOWN);
        }

        // put statistic for partition
        CatalogColumnStatistics columnStatistics = createColumnStats();
        catalog.alterPartitionColumnStatistics(path1, partitionSpec, columnStatistics, false);
        catalog.alterPartitionColumnStatistics(
                path1, anotherPartitionSpec, columnStatistics, false);

        catalogColumnStatistics =
                catalog.bulkGetPartitionColumnStatistics(path1, catalogPartitionSpecs);
        for (CatalogColumnStatistics statistics : catalogColumnStatistics) {
            CatalogTestUtil.checkEquals(statistics, columnStatistics);
        }
    }

    // ------ utilities ------

    @Override
    protected boolean isGeneric() {
        return true;
    }

    private CatalogColumnStatistics createColumnStats() {
        CatalogColumnStatisticsDataBoolean booleanColStats =
                new CatalogColumnStatisticsDataBoolean(55L, 45L, 5L);
        CatalogColumnStatisticsDataLong longColStats =
                new CatalogColumnStatisticsDataLong(-123L, 763322L, 23L, 79L);
        CatalogColumnStatisticsDataString stringColStats =
                new CatalogColumnStatisticsDataString(152L, 43.5D, 20L, 0L);
        CatalogColumnStatisticsDataDate dateColStats =
                new CatalogColumnStatisticsDataDate(new Date(71L), new Date(17923L), 1321L, 0L);
        CatalogColumnStatisticsDataDouble doubleColStats =
                new CatalogColumnStatisticsDataDouble(-123.35D, 7633.22D, 23L, 79L);
        CatalogColumnStatisticsDataBinary binaryColStats =
                new CatalogColumnStatisticsDataBinary(755L, 43.5D, 20L);
        Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>(6);
        colStatsMap.put("b1", booleanColStats);
        colStatsMap.put("l2", longColStats);
        colStatsMap.put("s3", stringColStats);
        colStatsMap.put("d4", dateColStats);
        colStatsMap.put("dd5", doubleColStats);
        colStatsMap.put("bb6", binaryColStats);
        return new CatalogColumnStatistics(colStatsMap);
    }

    @Override
    protected CatalogFunction createFunction() {
        return new CatalogFunctionImpl(TestGenericUDF.class.getCanonicalName());
    }

    @Override
    protected CatalogFunction createAnotherFunction() {
        return new CatalogFunctionImpl(
                TestSimpleUDF.class.getCanonicalName(), FunctionLanguage.SCALA);
    }

    @Override
    protected boolean supportsModels() {
        return true;
    }

    @Override
    protected CatalogFunction createPythonFunction() {
        return new CatalogFunctionImpl("test.func1", FunctionLanguage.PYTHON);
    }

    @Test
    void testRegisterCatalog() {
        final TableEnvironmentMock tableEnv = TableEnvironmentMock.getStreamingInstance();
        try {
            tableEnv.registerCatalog(TEST_CATALOG_NAME, new MyCatalog(TEST_CATALOG_NAME));
        } catch (CatalogException e) {
        }
        assertThat(tableEnv.getCatalog(TEST_CATALOG_NAME)).isNotPresent();
    }

    class MyCatalog extends GenericInMemoryCatalog {

        public MyCatalog(String name) {
            super(name);
        }

        @Override
        public void open() {
            throw new CatalogException("open catalog failed.");
        }
    }
}
