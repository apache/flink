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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions;
import org.apache.flink.table.filesystem.FileSystemLookupFunction;
import org.apache.flink.table.filesystem.PartitionFetcher;
import org.apache.flink.table.filesystem.PartitionReader;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.STREAMING_SOURCE_PARTITION_INCLUDE;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.STREAMING_SOURCE_PARTITION_ORDER;
import static org.junit.Assert.assertEquals;

/** Test lookup join of hive tables. */
public class HiveLookupJoinITCase {

    private static TableEnvironment tableEnv;
    private static HiveCatalog hiveCatalog;

    @BeforeClass
    public static void setup() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        // create probe table
        TestCollectionTableFactory.initData(
                Arrays.asList(
                        Row.of(1, "a"),
                        Row.of(1, "c"),
                        Row.of(2, "b"),
                        Row.of(2, "c"),
                        Row.of(3, "c"),
                        Row.of(4, "d")));
        tableEnv.executeSql(
                "create table default_catalog.default_database.probe (x int,y string, p as proctime()) "
                        + "with ('connector'='COLLECTION','is-bounded' = 'false')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // create the hive non-partitioned table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_table (x int, y string, z int) tblproperties ('%s'='5min')",
                        FileSystemConnectorOptions.LOOKUP_JOIN_CACHE_TTL.key()));

        // create the hive partitioned table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_partition_table (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ('%s'='5min')",
                        FileSystemConnectorOptions.LOOKUP_JOIN_CACHE_TTL.key()));

        // create the hive partitioned table
        tableEnv.executeSql(
                String.format(
                        "create table partition_table (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ('%s' = 'true', '%s' = 'latest', '%s' = 'partition-name', '%s'='2h')",
                        STREAMING_SOURCE_ENABLE.key(),
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        STREAMING_SOURCE_PARTITION_ORDER.key(),
                        STREAMING_SOURCE_MONITOR_INTERVAL.key()));

        // create the hive partitioned table3 which uses default 'partition-name'.
        tableEnv.executeSql(
                String.format(
                        "create table partition_table_1 (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ('%s' = 'true', '%s' = 'latest', '%s'='120min')",
                        FileSystemConnectorOptions.STREAMING_SOURCE_ENABLE.key(),
                        FileSystemConnectorOptions.STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        FileSystemConnectorOptions.STREAMING_SOURCE_MONITOR_INTERVAL.key()));

        // create the hive partitioned table3 which uses 'partition-time'.
        tableEnv.executeSql(
                String.format(
                        "create table partition_table_2 (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ("
                                + "'%s' = 'true',"
                                + " '%s' = 'latest',"
                                + " '%s' = '12h',"
                                + " '%s' = 'partition-time', "
                                + " '%s' = 'default',"
                                + " '%s' = '$pt_year-$pt_mon-$pt_day 00:00:00')",
                        STREAMING_SOURCE_ENABLE.key(),
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        STREAMING_SOURCE_MONITOR_INTERVAL.key(),
                        STREAMING_SOURCE_PARTITION_ORDER.key(),
                        PARTITION_TIME_EXTRACTOR_KIND.key(),
                        PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key()));

        // create the hive partitioned table3 which uses 'create-time'.
        tableEnv.executeSql(
                String.format(
                        "create table partition_table_3 (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ("
                                + " '%s' = 'true',"
                                + " '%s' = 'latest',"
                                + " '%s' = 'create-time')",
                        STREAMING_SOURCE_ENABLE.key(),
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        STREAMING_SOURCE_PARTITION_ORDER.key()));
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    }

    @Test
    public void testLookupOptions() throws Exception {
        FileSystemLookupFunction<HiveTablePartition> lookupFunction1 =
                getLookupFunction("bounded_table");
        FileSystemLookupFunction<HiveTablePartition> lookupFunction2 =
                getLookupFunction("partition_table");
        lookupFunction1.open(null);
        lookupFunction2.open(null);

        // verify lookup cache TTL option is properly configured
        assertEquals(Duration.ofMinutes(5), lookupFunction1.getReloadInterval());
        assertEquals(Duration.ofMinutes(120), lookupFunction2.getReloadInterval());
    }

    @Test
    public void testPartitionFetcherAndReader() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        batchEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        batchEnv.useCatalog(hiveCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite partition_table values "
                                + "(1,'a',08,2019,'08','01'),"
                                + "(1,'a',10,2020,'08','31'),"
                                + "(2,'a',21,2020,'08','31'),"
                                + "(2,'b',22,2020,'08','31'),"
                                + "(3,'c',33,2020,'09','31')")
                .await();
        FileSystemLookupFunction<HiveTablePartition> lookupFunction =
                getLookupFunction("partition_table");
        lookupFunction.open(null);

        PartitionFetcher<HiveTablePartition> fetcher = lookupFunction.getPartitionFetcher();
        PartitionFetcher.Context<HiveTablePartition> context = lookupFunction.getFetcherContext();
        List<HiveTablePartition> partitions = fetcher.fetch(context);
        // fetch latest partition by partition-name
        assertEquals(1, partitions.size());

        PartitionReader<HiveTablePartition, RowData> reader = lookupFunction.getPartitionReader();
        reader.open(partitions);

        List<RowData> res = new ArrayList<>();
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of(hiveCatalog.getName(), "default", "partition_table");
        CatalogTable catalogTable =
                (CatalogTable) hiveCatalog.getTable(tableIdentifier.toObjectPath());
        GenericRowData reuse = new GenericRowData(catalogTable.getSchema().getFieldCount());
        TypeSerializer<RowData> serializer =
                InternalSerializers.create(
                        catalogTable.getSchema().toRowDataType().getLogicalType());

        RowData row;
        while ((row = reader.read(reuse)) != null) {
            res.add(serializer.copy(row));
        }
        res.sort(Comparator.comparingInt(o -> o.getInt(0)));
        assertEquals("[+I(3,c,33,2020,09,31)]", res.toString());
    }

    @Test
    public void testLookupJoinBoundedTable() throws Exception {
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(
                        "insert into bounded_table values (1,'a',10),(2,'a',21),(2,'b',22),(3,'c',33)")
                .await();
        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z from "
                                        + " default_catalog.default_database.probe as p "
                                        + " join bounded_table for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertEquals("[+I[1, a, 10], +I[2, b, 22], +I[3, c, 33]]", results.toString());
    }

    @Test
    public void testLookupJoinBoundedPartitionedTable() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        batchEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        batchEnv.useCatalog(hiveCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite bounded_partition_table values "
                                + "(1,'a',08,2019,'08','01'),"
                                + "(1,'a',10,2020,'08','31'),"
                                + "(2,'a',21,2020,'08','31'),"
                                + "(2,'b',22,2020,'08','31')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " default_catalog.default_database.probe as p"
                                        + " join bounded_partition_table for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertEquals(
                "[+I[1, a, 8, 2019, 08, 01], +I[1, a, 10, 2020, 08, 31], +I[2, b, 22, 2020, 08, 31]]",
                results.toString());
    }

    @Test
    public void testLookupJoinPartitionedTable() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        batchEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        batchEnv.useCatalog(hiveCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite partition_table_1 values "
                                + "(1,'a',08,2019,'09','01'),"
                                + "(1,'a',10,2020,'09','31'),"
                                + "(2,'a',21,2020,'09','31'),"
                                + "(2,'b',22,2020,'09','31'),"
                                + "(3,'c',33,2020,'09','31'),"
                                + "(1,'a',101,2020,'08','01'),"
                                + "(2,'a',121,2020,'08','01'),"
                                + "(2,'b',122,2020,'08','01')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " default_catalog.default_database.probe as p"
                                        + " join partition_table_1 for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertEquals(
                "[+I[1, a, 10, 2020, 09, 31], +I[2, b, 22, 2020, 09, 31], +I[3, c, 33, 2020, 09, 31]]",
                results.toString());
    }

    @Test
    public void testLookupJoinPartitionedTableWithPartitionTime() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        batchEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        batchEnv.useCatalog(hiveCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite partition_table_2 values "
                                + "(1,'a',08,2020,'08','01'),"
                                + "(1,'a',10,2020,'08','31'),"
                                + "(2,'a',21,2019,'08','31'),"
                                + "(2,'b',22,2020,'08','31'),"
                                + "(3,'c',33,2017,'08','31'),"
                                + "(1,'a',101,2017,'09','01'),"
                                + "(2,'a',121,2019,'09','01'),"
                                + "(2,'b',122,2019,'09','01')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " default_catalog.default_database.probe as p"
                                        + " join partition_table_2 for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertEquals(
                "[+I[1, a, 10, 2020, 08, 31], +I[2, b, 22, 2020, 08, 31]]", results.toString());
    }

    @Test
    public void testLookupJoinPartitionedTableWithCreateTime() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        batchEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        batchEnv.useCatalog(hiveCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite partition_table_3 values "
                                + "(1,'a',08,2020,'month1','01'),"
                                + "(1,'a',10,2020,'month2','02'),"
                                + "(2,'a',21,2020,'month1','02'),"
                                + "(2,'b',22,2020,'month3','20'),"
                                + "(3,'c',22,2020,'month3','20'),"
                                + "(3,'c',33,2017,'08','31'),"
                                + "(1,'a',101,2017,'09','01'),"
                                + "(2,'a',121,2019,'09','01'),"
                                + "(2,'b',122,2019,'09','01')")
                .await();

        // inert a new partition
        batchEnv.executeSql(
                        "insert overwrite partition_table_3 values "
                                + "(1,'a',101,2020,'08','01'),"
                                + "(2,'a',121,2020,'08','01'),"
                                + "(2,'b',122,2020,'08','01')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " default_catalog.default_database.probe as p"
                                        + " join partition_table_3 for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertEquals(
                "[+I[1, a, 101, 2020, 08, 01], +I[2, b, 122, 2020, 08, 01]]", results.toString());
    }

    private FileSystemLookupFunction<HiveTablePartition> getLookupFunction(String tableName)
            throws Exception {
        TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of(hiveCatalog.getName(), "default", tableName);
        CatalogTable catalogTable =
                (CatalogTable) hiveCatalog.getTable(tableIdentifier.toObjectPath());
        HiveLookupTableSource hiveTableSource =
                (HiveLookupTableSource)
                        FactoryUtil.createTableSource(
                                hiveCatalog,
                                tableIdentifier,
                                tableEnvInternal
                                        .getCatalogManager()
                                        .resolveCatalogTable(catalogTable),
                                tableEnv.getConfig().getConfiguration(),
                                Thread.currentThread().getContextClassLoader(),
                                false);
        FileSystemLookupFunction<HiveTablePartition> lookupFunction =
                (FileSystemLookupFunction<HiveTablePartition>)
                        hiveTableSource.getLookupFunction(new int[][] {{0}});
        return lookupFunction;
    }

    @AfterClass
    public static void tearDown() {
        tableEnv.executeSql("drop table bounded_table");
        tableEnv.executeSql("drop table bounded_partition_table");
        tableEnv.executeSql("drop table partition_table");
        tableEnv.executeSql("drop table partition_table_1");
        tableEnv.executeSql("drop table partition_table_2");
        tableEnv.executeSql("drop table partition_table_3");

        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }
}
