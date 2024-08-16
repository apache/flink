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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.ModelAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.ModelNotExistException;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for GenericInMemoryCatalog. */
class GenericInMemoryCatalogTest extends CatalogTestBase {

    @BeforeAll
    static void init() {
        catalog = new GenericInMemoryCatalog(TEST_CATALOG_NAME);
        catalog.open();
    }

    // TODO (FLINK-35020) : remove after implementing dropModel in catalog
    @AfterEach
    void cleanup() throws Exception {
        super.cleanup();
        ((GenericInMemoryCatalog) catalog).dropAllModels();
    }

    // These are put here instead of CatalogTest class since model operations are not implemented
    // in Hive catalogs
    // TODO (FLINK-35020): move to CatalogTest after implementing model interfaces in Hive catalogs
    // ------ models ------
    @Test
    public void testCreateModel() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogModel model = createModel();
        catalog.createModel(modelPath1, model, false);

        List<String> models = catalog.listModels(db1);
        // TODO (FLINK-35020): replace below with catalog.getModel
        assertThat(models).isEqualTo(Collections.singletonList(m1));
    }

    @Test
    public void testCreateModel_DatabaseNotExistException() {
        assertThat(catalog.databaseExists(db1)).isFalse();

        assertThatThrownBy(() -> catalog.createModel(nonExistObjectPath, createModel(), false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testCreateModel_ModelAlreadyExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createModel(modelPath1, createModel(), false);

        assertThatThrownBy(() -> catalog.createModel(modelPath1, createModel(), false))
                .isInstanceOf(ModelAlreadyExistException.class)
                .hasMessage("Model db1.m1 already exists in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testCreateModel_ModelAlreadyExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogModel model = createModel();
        catalog.createModel(modelPath1, model, false);
        catalog.createModel(modelPath1, model, true);

        List<String> models = catalog.listModels(db1);
        // TODO (FLINK-35020): replace below with catalog.getModel
        assertThat(models).isEqualTo(Collections.singletonList(m1));
    }

    @Test
    public void testListModels() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        catalog.createModel(modelPath1, createModel(), false);
        catalog.createModel(modelPath2, createModel(), false);

        assertThat(catalog.listModels(db1)).isEqualTo(Arrays.asList(m1, m2));
    }

    @Test
    public void testGetModel() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createModel(modelPath1, createModel(), false);
        assertThat(catalog.getModel(modelPath1)).isNotNull();
    }

    @Test
    public void testGetModel_ModelNotExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        assertThatThrownBy(() -> assertThat(catalog.getModel(modelPath1)).isNotNull())
                .isInstanceOf(ModelNotExistException.class)
                .hasMessage("Model '`test-catalog`.`db1`.`m1`' does not exist.");
    }

    @Test
    public void testDropModel() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createModel(modelPath1, createModel(), false);
        assertThat(catalog.getModel(modelPath1)).isNotNull();
        catalog.dropModel(modelPath1, false);
        assertThatThrownBy(() -> catalog.getModel(modelPath1))
                .isInstanceOf(ModelNotExistException.class)
                .hasMessage("Model '`test-catalog`.`db1`.`m1`' does not exist.");
    }

    @Test
    public void testAlterModel() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createModel(modelPath1, createModel(), false);
        assertThat(catalog.getModel(modelPath1)).isNotNull();
        Schema inputSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        Schema outputSchema = Schema.newBuilder().column("label", DataTypes.STRING()).build();
        CatalogModel newModel =
                CatalogModel.of(
                        inputSchema,
                        outputSchema,
                        new HashMap<String, String>() {
                            {
                                put("provider", "openai");
                                put("task", "regression"); // Changed option
                                put("endpoint", "some-endpoint"); // New option
                            }
                        },
                        null);
        catalog.alterModel(modelPath1, newModel, false);
        assertThat(catalog.getModel(modelPath1).getComment()).isNull();
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("task", "regression");
        expectedOptions.put("provider", "openai");
        expectedOptions.put("endpoint", "some-endpoint");
        assertThat(catalog.getModel(modelPath1).getOptions()).isEqualTo(expectedOptions);
    }

    @Test
    public void testAlterModel_ModelNotExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        Schema inputSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        Schema outputSchema = Schema.newBuilder().column("label", DataTypes.STRING()).build();
        CatalogModel newModel =
                CatalogModel.of(
                        inputSchema,
                        outputSchema,
                        new HashMap<String, String>() {
                            {
                                put("task", "clustering");
                                put("provider", "openai");
                            }
                        },
                        "new model");
        assertThatThrownBy(() -> catalog.alterModel(modelPath1, newModel, false))
                .isInstanceOf(ModelNotExistException.class)
                .hasMessage("Model '`test-catalog`.`db1`.`m1`' does not exist.");
    }

    @Test
    public void testAlterMissingModelIgnoreIfNotExist() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        Schema inputSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        Schema outputSchema = Schema.newBuilder().column("label", DataTypes.STRING()).build();
        CatalogModel newModel =
                CatalogModel.of(
                        inputSchema,
                        outputSchema,
                        new HashMap<String, String>() {
                            {
                                put("task", "clustering");
                                put("provider", "openai");
                            }
                        },
                        "new model");
        // Nothing happens since ignoreIfNotExists is true
        catalog.alterModel(modelPath1, newModel, true);
    }

    @Test
    public void testDropMissingModelNotExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        assertThatThrownBy(() -> catalog.dropModel(modelPath1, false))
                .isInstanceOf(ModelNotExistException.class)
                .hasMessage("Model '`test-catalog`.`db1`.`m1`' does not exist.");
    }

    @Test
    public void testDropMissingModelIgnoreIfNotExist() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        // Nothing happens since ignoreIfNotExists is true
        catalog.dropModel(modelPath1, true);
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
