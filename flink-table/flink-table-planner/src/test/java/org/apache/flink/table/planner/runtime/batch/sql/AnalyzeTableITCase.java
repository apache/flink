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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for `ANALYZE TABLE`. */
public class AnalyzeTableITCase extends BatchTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        tEnv = tEnv();
        Catalog catalog = new TestValuesCatalog("cat", "db", true);
        tEnv.registerCatalog("cat", catalog);
        tEnv.useCatalog("cat");
        tEnv.useDatabase("db");
        String dataId1 = TestValuesTableFactory.registerData(TestData.fullDataTypesData());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE NonPartitionTable (\n"
                                + "  `a` BOOLEAN,\n"
                                + "  `b` TINYINT,\n"
                                + "  `c` SMALLINT,\n"
                                + "  `d` INT,\n"
                                + "  `e` BIGINT,\n"
                                + "  `f` FLOAT,\n"
                                + "  `g` DOUBLE,\n"
                                + "  `h` DECIMAL(5, 2),\n"
                                + "  `x` DECIMAL(30, 10),\n"
                                + "  `i` VARCHAR(5),\n"
                                + "  `j` CHAR(5),\n"
                                + "  `k` DATE,\n"
                                + "  `l` TIME(0),\n"
                                + "  `m` TIMESTAMP(9),\n"
                                + "  `n` TIMESTAMP(9) WITH LOCAL TIME ZONE,\n"
                                + "  `o` ARRAY<BIGINT>,\n"
                                + "  `p` ROW<f1 BIGINT, f2 STRING, f3 DOUBLE>,\n"
                                + "  `q` MAP<STRING, INT>\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId1));

        String dataId2 = TestValuesTableFactory.registerData(TestData.data5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE PartitionTable (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` INT,\n"
                                + "  `d` VARCHAR,\n"
                                + "  `e` BIGINT\n"
                                + ") partitioned by (e, a)\n"
                                + " WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'partition-list' = 'e:1,a:1;e:1,a:2;e:1,a:4;e:1,a:5;e:2,a:2;e:2,a:3;e:2,a:4;e:2,a:5;e:3,a:3;e:3,a:5;',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId2));
        createPartition(catalog, "db", "PartitionTable", "e=1,a=1");
        createPartition(catalog, "db", "PartitionTable", "e=1,a=2");
        createPartition(catalog, "db", "PartitionTable", "e=1,a=4");
        createPartition(catalog, "db", "PartitionTable", "e=1,a=5");
        createPartition(catalog, "db", "PartitionTable", "e=2,a=2");
        createPartition(catalog, "db", "PartitionTable", "e=2,a=3");
        createPartition(catalog, "db", "PartitionTable", "e=2,a=4");
        createPartition(catalog, "db", "PartitionTable", "e=2,a=5");
        createPartition(catalog, "db", "PartitionTable", "e=3,a=3");
        createPartition(catalog, "db", "PartitionTable", "e=3,a=5");

        String dataId3 = TestValuesTableFactory.registerData(TestData.smallData5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE NonPartitionTable2 (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` INT,\n"
                                + "  `d` VARCHAR METADATA VIRTUAL,\n"
                                + "  `e` BIGINT METADATA,"
                                + "  `f` as a + 1\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'readable-metadata'='d:varchar,e:bigint',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId3));

        String dataId4 = TestValuesTableFactory.registerData(TestData.smallData5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE PartitionTable2 (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` INT,\n"
                                + "  `d` VARCHAR METADATA VIRTUAL,\n"
                                + "  `e` BIGINT METADATA,"
                                + "  `f` as a + 1\n"
                                + ") partitioned by (a)\n"
                                + " WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'partition-list' = 'a:1;a:2;',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'readable-metadata'='d:varchar,e:bigint',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId4));
        createPartition(catalog, "db", "PartitionTable2", "a=1");
        createPartition(catalog, "db", "PartitionTable2", "a=2");
    }

    private void createPartition(Catalog catalog, String db, String table, String partitionSpecs)
            throws Exception {
        catalog.createPartition(
                new ObjectPath(db, table),
                createCatalogPartitionSpec(partitionSpecs),
                new CatalogPartitionImpl(new HashMap<>(), ""),
                false);
    }

    private CatalogPartitionSpec createCatalogPartitionSpec(String partitionSpecs) {
        Map<String, String> partitionSpec = new HashMap<>();
        for (String partition : partitionSpecs.split(",")) {
            String[] items = partition.split("=");
            Preconditions.checkArgument(
                    items.length == 2, "Partition key value should be joined with '='");
            partitionSpec.put(items[0], items[1]);
        }
        return new CatalogPartitionSpec(partitionSpec);
    }

    @Test
    public void testNonPartitionTableWithoutTableNotExisted() {
        assertThatThrownBy(
                        () -> tEnv.executeSql("analyze table not_exist_table compute statistics"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Table `cat`.`db`.`not_exist_table` doesn't exist");
    }

    @Test
    public void testNonPartitionTableWithoutColumns() throws Exception {
        tEnv.executeSql("analyze table NonPartitionTable compute statistics");
        ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "NonPartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(5L, -1, -1L, -1L));
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path))
                .isEqualTo(new CatalogColumnStatistics(new HashMap<>()));
    }

    @Test
    public void testNonPartitionTableWithColumnsNotExisted() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table NonPartitionTable compute statistics for columns not_existed_column"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column: not_existed_column does not exist in the table: `cat`.`db`.`NonPartitionTable`");
    }

    @Test
    public void testNonPartitionTableWithComputeColumn() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table NonPartitionTable2 compute statistics for columns f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column: f is a computed column, ANALYZE TABLE does not support computed column");
    }

    @Test
    public void testNonPartitionTableWithVirtualMetadataColumn() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table NonPartitionTable2 compute statistics for columns d"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column: d is a metadata column, ANALYZE TABLE does not support metadata column");
    }

    @Test
    public void testNonPartitionTableWithMetadataColumn() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table NonPartitionTable2 compute statistics for columns e"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column: e is a metadata column, ANALYZE TABLE does not support metadata column");
    }

    @Test
    public void testNonPartitionTableWithPartition() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table NonPartitionTable PARTITION(a) compute statistics"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid ANALYZE TABLE statement. Table: `cat`.`db`.`NonPartitionTable` is not a partition table, while partition values are given");
    }

    @Test
    public void testNonPartitionTableWithPartialColumns() throws Exception {
        tEnv.executeSql("analyze table NonPartitionTable compute statistics for columns f, a, d");
        ObjectPath path1 = new ObjectPath(tEnv.getCurrentDatabase(), "NonPartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path1))
                .isEqualTo(new CatalogTableStatistics(5L, -1, -1L, -1L));
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData1 = new HashMap<>();
        columnStatisticsData1.put("a", new CatalogColumnStatisticsDataBoolean(2L, 2L, 1L));
        columnStatisticsData1.put(
                "f", new CatalogColumnStatisticsDataDouble(-1.123d, 3.4d, 4L, 1L));
        columnStatisticsData1.put(
                "d",
                new CatalogColumnStatisticsDataLong(
                        (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE, 4L, 1L));
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path1))
                .isEqualTo(new CatalogColumnStatistics(columnStatisticsData1));

        tEnv.executeSql("analyze table NonPartitionTable2 compute statistics for columns a, b, c");
        ObjectPath path2 = new ObjectPath(tEnv.getCurrentDatabase(), "NonPartitionTable2");
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData2 = new HashMap<>();
        columnStatisticsData2.put("a", new CatalogColumnStatisticsDataLong(1L, 2L, 2L, 0L));
        columnStatisticsData2.put("b", new CatalogColumnStatisticsDataLong(1L, 3L, 3L, 0L));
        columnStatisticsData2.put("c", new CatalogColumnStatisticsDataLong(0L, 2L, 3L, 0L));
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path2))
                .isEqualTo(new CatalogColumnStatistics(columnStatisticsData2));
    }

    @Test
    public void testNonPartitionTableWithAllColumns() throws Exception {
        tEnv.executeSql("analyze table NonPartitionTable compute statistics for all columns");
        ObjectPath path1 = new ObjectPath(tEnv.getCurrentDatabase(), "NonPartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path1))
                .isEqualTo(new CatalogTableStatistics(5L, -1, -1L, -1L));
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData1 = new HashMap<>();
        // boolean
        columnStatisticsData1.put("a", new CatalogColumnStatisticsDataBoolean(2L, 2L, 1L));
        // byte
        columnStatisticsData1.put(
                "b",
                new CatalogColumnStatisticsDataLong(
                        (long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE, 4L, 1L));
        // short
        columnStatisticsData1.put(
                "c",
                new CatalogColumnStatisticsDataLong(
                        (long) Short.MIN_VALUE, (long) Short.MAX_VALUE, 4L, 1L));
        // int
        columnStatisticsData1.put(
                "d",
                new CatalogColumnStatisticsDataLong(
                        (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE, 4L, 1L));
        // long
        columnStatisticsData1.put(
                "e", new CatalogColumnStatisticsDataLong(Long.MIN_VALUE, Long.MAX_VALUE, 4L, 1L));
        // float
        columnStatisticsData1.put(
                "f", new CatalogColumnStatisticsDataDouble(-1.123d, 3.4d, 4L, 1L));
        // double
        columnStatisticsData1.put(
                "g", new CatalogColumnStatisticsDataDouble(-1.123d, 3.4d, 4L, 1L));
        // DECIMAL(5, 2)
        columnStatisticsData1.put("h", new CatalogColumnStatisticsDataDouble(5.1d, 8.12d, 4L, 1L));
        // DECIMAL(30, 10)
        columnStatisticsData1.put(
                "x",
                new CatalogColumnStatisticsDataDouble(
                        1234567891012345.1d, 812345678910123451.0123456789d, 4L, 1L));
        // varchar
        columnStatisticsData1.put("i", new CatalogColumnStatisticsDataString(4L, 2.5d, 4L, 1L));
        // char
        columnStatisticsData1.put("j", new CatalogColumnStatisticsDataString(4L, 2.5d, 4L, 1L));
        // date
        columnStatisticsData1.put(
                "k", new CatalogColumnStatisticsDataDate(new Date(-365), new Date(18383), 4L, 1L));
        // time
        columnStatisticsData1.put(
                "l", new CatalogColumnStatisticsDataLong(123000000L, 84203000000000L, 4L, 1L));
        // timestamp
        columnStatisticsData1.put(
                "m", new CatalogColumnStatisticsDataLong(-31536000L, 1588375403L, 4L, 1L));
        // timestamp with local timezone
        columnStatisticsData1.put(
                "n", new CatalogColumnStatisticsDataLong(-31535999877L, 1588375403000L, 4L, 1L));

        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path1))
                .isEqualTo(new CatalogColumnStatistics(columnStatisticsData1));

        tEnv.executeSql("analyze table NonPartitionTable2 compute statistics for all columns");
        ObjectPath path2 = new ObjectPath(tEnv.getCurrentDatabase(), "NonPartitionTable2");
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData2 = new HashMap<>();
        columnStatisticsData2.put("a", new CatalogColumnStatisticsDataLong(1L, 2L, 2L, 0L));
        columnStatisticsData2.put("b", new CatalogColumnStatisticsDataLong(1L, 3L, 3L, 0L));
        columnStatisticsData2.put("c", new CatalogColumnStatisticsDataLong(0L, 2L, 3L, 0L));
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path2))
                .isEqualTo(new CatalogColumnStatistics(columnStatisticsData2));
    }

    @Test
    public void testNonPartitionTableAnalyzePartialColumnsWithSomeColumnsHaveColumnStats()
            throws TableNotExistException {
        // If some columns have table column stats, analyze table for partial columns will merge
        // these exist columns stats instead of covering it.
        // Adding column stats to partial columns.
        tEnv.executeSql("analyze table NonPartitionTable compute statistics for columns f, a, d");
        ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "NonPartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(5L, -1, -1L, -1L));
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData = new HashMap<>();
        columnStatisticsData.put("a", new CatalogColumnStatisticsDataBoolean(2L, 2L, 1L));
        columnStatisticsData.put("f", new CatalogColumnStatisticsDataDouble(-1.123d, 3.4d, 4L, 1L));
        columnStatisticsData.put(
                "d",
                new CatalogColumnStatisticsDataLong(
                        (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE, 4L, 1L));
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path))
                .isEqualTo(new CatalogColumnStatistics(columnStatisticsData));

        // Analyze different column sets.
        tEnv.executeSql("analyze table NonPartitionTable compute statistics for columns d, e");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(5L, -1, -1L, -1L));
        columnStatisticsData.put(
                "e", new CatalogColumnStatisticsDataLong(Long.MIN_VALUE, Long.MAX_VALUE, 4L, 1L));
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path))
                .isEqualTo(new CatalogColumnStatistics(columnStatisticsData));
    }

    @Test
    public void testPartitionTableWithoutPartition() {
        assertThatThrownBy(() -> tEnv.executeSql("analyze table PartitionTable compute statistics"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid ANALYZE TABLE statement. For partition table, all partition keys should be specified explicitly. "
                                + "The given partition keys: [] are not match the target partition keys: [e,a]");
    }

    @Test
    public void testPartitionTableWithPartitionKeyNotExisted() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table PartitionTable PARTITION(d) compute statistics"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid ANALYZE TABLE statement. For partition table, all partition keys should be specified explicitly. "
                                + "The given partition keys: [d] are not match the target partition keys: [e,a]");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table PartitionTable PARTITION(e=1) compute statistics"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid ANALYZE TABLE statement. For partition table, all partition keys should be specified explicitly. "
                                + "The given partition keys: [e] are not match the target partition keys: [e,a]");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table PartitionTable PARTITION(e=1,d) compute statistics"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid ANALYZE TABLE statement. For partition table, all partition keys should be specified explicitly. "
                                + "The given partition keys: [e,d] are not match the target partition keys: [e,a]");
    }

    @Test
    public void testPartitionTableWithPartitionValueNotExisted() throws Exception {
        tEnv.executeSql("analyze table PartitionTable partition(e=10,a) compute statistics");
        ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "PartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(-1L, -1, -1L, -1L));

        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path))
                .isEqualTo(new CatalogColumnStatistics(new HashMap<>()));
    }

    @Test
    public void testPartitionTableWithColumnsNotExisted() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table PartitionTable partition(e, a) compute statistics for columns not_existed_column"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column: not_existed_column does not exist in the table: `cat`.`db`.`PartitionTable`");
    }

    @Test
    public void testPartitionTableWithVirtualMetadataColumn() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table PartitionTable2 PARTITION(a) compute statistics for columns d"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column: d is a metadata column, ANALYZE TABLE does not support metadata column");
    }

    @Test
    public void testPartitionTableWithMetadataColumn() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table PartitionTable2 PARTITION(a) compute statistics for columns e"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column: e is a metadata column, ANALYZE TABLE does not support metadata column");
    }

    @Test
    public void testPartitionTableWithComputeColumn() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table PartitionTable2 PARTITION(a) compute statistics for columns f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column: f is a computed column, ANALYZE TABLE does not support computed column");
    }

    @Test
    public void testPartitionTableWithPartition() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "analyze table NonPartitionTable PARTITION(a) compute statistics"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid ANALYZE TABLE statement. Table: `cat`.`db`.`NonPartitionTable` is not a partition table, while partition values are given");
    }

    @Test
    public void testPartitionTableWithoutColumns() throws Exception {
        // Strict order is not required
        tEnv.executeSql("analyze table PartitionTable partition(a, e) compute statistics");
        ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "PartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(-1L, -1, -1L, -1L));
        assertPartitionStatistics(path, "e=1,a=1", 1L);
        assertPartitionStatistics(path, "e=1,a=2", 1L);
        assertPartitionStatistics(path, "e=1,a=4", 2L);
        assertPartitionStatistics(path, "e=1,a=5", 1L);
        assertPartitionStatistics(path, "e=2,a=2", 1L);
        assertPartitionStatistics(path, "e=2,a=3", 2L);
        assertPartitionStatistics(path, "e=2,a=4", 2L);
        assertPartitionStatistics(path, "e=2,a=5", 2L);
        assertPartitionStatistics(path, "e=3,a=3", 1L);
        assertPartitionStatistics(path, "e=3,a=5", 2L);

        tEnv.executeSql(
                "analyze table PartitionTable2 partition(a) compute statistics for all columns");
        ObjectPath path2 = new ObjectPath(tEnv.getCurrentDatabase(), "PartitionTable2");
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData2 = new HashMap<>();
        columnStatisticsData2.put("a", new CatalogColumnStatisticsDataLong(1L, 1L, 1L, 0L));
        columnStatisticsData2.put("b", new CatalogColumnStatisticsDataLong(1L, 1L, 1L, 0L));
        columnStatisticsData2.put("c", new CatalogColumnStatisticsDataLong(0L, 0L, 1L, 0L));
        assertPartitionStatistics(
                path2, "a=1", 1L, new CatalogColumnStatistics(columnStatisticsData2));

        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData3 = new HashMap<>();
        columnStatisticsData3.put("a", new CatalogColumnStatisticsDataLong(2L, 2L, 1L, 0L));
        columnStatisticsData3.put("b", new CatalogColumnStatisticsDataLong(2L, 3L, 2L, 0L));
        columnStatisticsData3.put("c", new CatalogColumnStatisticsDataLong(1L, 2L, 2L, 0L));
        assertPartitionStatistics(
                path2, "a=2", 2L, new CatalogColumnStatistics(columnStatisticsData3));
    }

    @Test
    public void testPartitionTableWithFullPartitionPath() throws Exception {
        tEnv.executeSql(
                "analyze table PartitionTable partition(e=2, a=5) compute statistics for all columns");
        ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "PartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(-1L, -1, -1L, -1L));
        assertPartitionStatistics(path, "e=1,a=1", -1L);
        assertPartitionStatistics(path, "e=1,a=2", -1L);
        assertPartitionStatistics(path, "e=1,a=4", -1L);
        assertPartitionStatistics(path, "e=1,a=5", -1L);
        assertPartitionStatistics(path, "e=2,a=2", -1L);
        assertPartitionStatistics(path, "e=2,a=3", -1L);
        assertPartitionStatistics(path, "e=2,a=4", -1L);
        assertPartitionStatistics(path, "e=3,a=3", -1L);
        assertPartitionStatistics(path, "e=3,a=5", -1L);

        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData = new HashMap<>();
        columnStatisticsData.put("a", new CatalogColumnStatisticsDataLong(5L, 5L, 1L, 0L));
        columnStatisticsData.put("b", new CatalogColumnStatisticsDataLong(14L, 15L, 2L, 0L));
        columnStatisticsData.put("c", new CatalogColumnStatisticsDataLong(13L, 14L, 2L, 0L));
        columnStatisticsData.put("d", new CatalogColumnStatisticsDataString(3L, 3.0, 2L, 0L));
        columnStatisticsData.put("e", new CatalogColumnStatisticsDataLong(2L, 2L, 1L, 0L));
        assertPartitionStatistics(
                path, "e=2,a=5", 2L, new CatalogColumnStatistics(columnStatisticsData));
    }

    @Test
    public void testPartitionTableWithPartialPartitionPath() throws Exception {
        tEnv.executeSql("analyze table PartitionTable partition(e=2, a) compute statistics");
        ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "PartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(-1L, -1, -1L, -1L));
        assertPartitionStatistics(path, "e=1,a=1", -1L);
        assertPartitionStatistics(path, "e=1,a=2", -1L);
        assertPartitionStatistics(path, "e=1,a=4", -1L);
        assertPartitionStatistics(path, "e=1,a=5", -1L);
        assertPartitionStatistics(path, "e=2,a=2", 1L);
        assertPartitionStatistics(path, "e=2,a=3", 2L);
        assertPartitionStatistics(path, "e=2,a=4", 2L);
        assertPartitionStatistics(path, "e=2,a=5", 2L);
        assertPartitionStatistics(path, "e=3,a=3", -1L);
        assertPartitionStatistics(path, "e=3,a=5", -1L);
    }

    @Test
    public void testPartitionTableAnalyzePartialColumnsWithSomeColumnsHaveColumnStats()
            throws Exception {
        // If some columns have table column stats, analyze table for partial columns will merge
        // these exist columns stats instead of covering it.
        // Adding column stats to partial columns.
        tEnv.executeSql(
                "analyze table PartitionTable partition(e=2, a=5) compute statistics for columns a, b, c");
        ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "PartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(-1L, -1, -1L, -1L));
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData = new HashMap<>();
        columnStatisticsData.put("a", new CatalogColumnStatisticsDataLong(5L, 5L, 1L, 0L));
        columnStatisticsData.put("b", new CatalogColumnStatisticsDataLong(14L, 15L, 2L, 0L));
        columnStatisticsData.put("c", new CatalogColumnStatisticsDataLong(13L, 14L, 2L, 0L));
        assertPartitionStatistics(
                path, "e=2,a=5", 2L, new CatalogColumnStatistics(columnStatisticsData));

        tEnv.executeSql(
                "analyze table PartitionTable partition(e=2, a=5) compute statistics for columns c, d");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(-1L, -1, -1L, -1L));
        columnStatisticsData.put("d", new CatalogColumnStatisticsDataString(3L, 3.0, 2L, 0L));
        assertPartitionStatistics(
                path, "e=2,a=5", 2L, new CatalogColumnStatistics(columnStatisticsData));
    }

    @Test
    public void testPartitionTableAnalyzePartialPartitionWithSomePartitionHaveColumnStats()
            throws Exception {
        // For different partitions, their column stats are isolated and should not affect each
        // other.
        // Adding column stats to one partition.
        tEnv.executeSql(
                "analyze table PartitionTable partition(e=2, a=5) compute statistics for columns a, b, c");
        ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "PartitionTable");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(-1L, -1, -1L, -1L));
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData1 = new HashMap<>();
        columnStatisticsData1.put("a", new CatalogColumnStatisticsDataLong(5L, 5L, 1L, 0L));
        columnStatisticsData1.put("b", new CatalogColumnStatisticsDataLong(14L, 15L, 2L, 0L));
        columnStatisticsData1.put("c", new CatalogColumnStatisticsDataLong(13L, 14L, 2L, 0L));
        assertPartitionStatistics(
                path, "e=2,a=5", 2L, new CatalogColumnStatistics(columnStatisticsData1));

        // Adding column stats to another partition.
        tEnv.executeSql(
                "analyze table PartitionTable partition(e=2, a=4) compute statistics for columns a, d");
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableStatistics(path))
                .isEqualTo(new CatalogTableStatistics(-1L, -1, -1L, -1L));
        // origin analyze partition.
        assertPartitionStatistics(
                path, "e=2,a=5", 2L, new CatalogColumnStatistics(columnStatisticsData1));
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData2 = new HashMap<>();
        columnStatisticsData2.put("a", new CatalogColumnStatisticsDataLong(4L, 4L, 1L, 0L));
        columnStatisticsData2.put("d", new CatalogColumnStatisticsDataString(3L, 3.0, 2L, 0L));
        // new analyze partition.
        assertPartitionStatistics(
                path, "e=2,a=4", 2L, new CatalogColumnStatistics(columnStatisticsData2));
    }

    private void assertPartitionStatistics(ObjectPath path, String partitionSpec, long rowCount)
            throws Exception {
        CatalogPartitionSpec spec = createCatalogPartitionSpec(partitionSpec);
        assertThat(
                        tEnv.getCatalog(tEnv.getCurrentCatalog())
                                .get()
                                .getPartitionStatistics(path, spec))
                .isEqualTo(new CatalogTableStatistics(rowCount, -1, -1L, -1L));
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path))
                .isEqualTo(new CatalogColumnStatistics(new HashMap<>()));
        assertThat(
                        tEnv.getCatalog(tEnv.getCurrentCatalog())
                                .get()
                                .getPartitionColumnStatistics(path, spec))
                .isEqualTo(new CatalogColumnStatistics(new HashMap<>()));
    }

    private void assertPartitionStatistics(
            ObjectPath path,
            String partitionSpec,
            long rowCount,
            CatalogColumnStatistics columnStats)
            throws Exception {
        CatalogPartitionSpec spec = createCatalogPartitionSpec(partitionSpec);
        assertThat(
                        tEnv.getCatalog(tEnv.getCurrentCatalog())
                                .get()
                                .getPartitionStatistics(path, spec))
                .isEqualTo(new CatalogTableStatistics(rowCount, -1, -1L, -1L));
        assertThat(tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTableColumnStatistics(path))
                .isEqualTo(new CatalogColumnStatistics(new HashMap<>()));
        assertThat(
                        tEnv.getCatalog(tEnv.getCurrentCatalog())
                                .get()
                                .getPartitionColumnStatistics(path, spec))
                .isEqualTo(columnStats);
    }
}
