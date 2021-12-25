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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.junit.Assert.assertEquals;

/** Tests for {@link HiveSource}. */
public class HiveSourceITCase {

    private static HiveCatalog hiveCatalog;

    @BeforeClass
    public static void setup() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
    }

    @AfterClass
    public static void tearDown() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @Test
    public void testRegularRead() throws Exception {
        // test non-partitioned table
        ObjectPath tablePath = new ObjectPath("default", "tbl1");
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CONNECTOR.key(), IDENTIFIER);
        hiveCatalog.createTable(
                tablePath,
                new CatalogTableImpl(
                        TableSchema.builder().field("i", DataTypes.INT()).build(),
                        tableOptions,
                        null),
                false);
        HiveTestUtils.createTextTableInserter(
                        hiveCatalog, tablePath.getDatabaseName(), tablePath.getObjectName())
                .addRow(new Object[] {1})
                .addRow(new Object[] {2})
                .commit();

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        HiveSource<RowData> hiveSource =
                new HiveSourceBuilder(
                                new JobConf(hiveCatalog.getHiveConf()),
                                new Configuration(),
                                HiveShimLoader.getHiveVersion(),
                                tablePath.getDatabaseName(),
                                tablePath.getObjectName(),
                                Collections.emptyMap())
                        .buildWithDefaultBulkFormat();
        List<RowData> results =
                CollectionUtil.iteratorToList(
                        streamEnv
                                .fromSource(
                                        hiveSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "HiveSource-tbl1")
                                .executeAndCollect());
        assertEquals(2, results.size());
        assertEquals(1, results.get(0).getInt(0));
        assertEquals(2, results.get(1).getInt(0));
        hiveCatalog.dropTable(tablePath, false);

        // test partitioned table
        tablePath = new ObjectPath("default", "tbl2");
        hiveCatalog.createTable(
                tablePath,
                new CatalogTableImpl(
                        TableSchema.builder()
                                .field("i", DataTypes.INT())
                                .field("p", DataTypes.STRING())
                                .build(),
                        Collections.singletonList("p"),
                        tableOptions,
                        null),
                false);
        HiveTestUtils.createTextTableInserter(
                        hiveCatalog, tablePath.getDatabaseName(), tablePath.getObjectName())
                .addRow(new Object[] {1})
                .addRow(new Object[] {2})
                .commit("p='a'");
        hiveSource =
                new HiveSourceBuilder(
                                new JobConf(hiveCatalog.getHiveConf()),
                                new Configuration(),
                                HiveShimLoader.getHiveVersion(),
                                tablePath.getDatabaseName(),
                                tablePath.getObjectName(),
                                Collections.emptyMap())
                        .setLimit(1L)
                        .buildWithDefaultBulkFormat();
        results =
                CollectionUtil.iteratorToList(
                        streamEnv
                                .fromSource(
                                        hiveSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "HiveSource-tbl2")
                                .executeAndCollect());
        assertEquals(1, results.size());
        assertEquals(1, results.get(0).getInt(0));
        assertEquals("a", results.get(0).getString(1).toString());

        HiveTestUtils.createTextTableInserter(
                        hiveCatalog, tablePath.getDatabaseName(), tablePath.getObjectName())
                .addRow(new Object[] {3})
                .commit("p='b'");
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("p", "b");
        hiveSource =
                new HiveSourceBuilder(
                                new JobConf(hiveCatalog.getHiveConf()),
                                new Configuration(),
                                null,
                                tablePath.getDatabaseName(),
                                tablePath.getObjectName(),
                                Collections.emptyMap())
                        .setPartitions(
                                Collections.singletonList(
                                        HiveTablePartition.ofPartition(
                                                hiveCatalog.getHiveConf(),
                                                hiveCatalog.getHiveVersion(),
                                                tablePath.getDatabaseName(),
                                                tablePath.getObjectName(),
                                                spec)))
                        .buildWithDefaultBulkFormat();
        results =
                CollectionUtil.iteratorToList(
                        streamEnv
                                .fromSource(
                                        hiveSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "HiveSource-tbl2")
                                .executeAndCollect());
        assertEquals(1, results.size());
        assertEquals(3, results.get(0).getInt(0));
        assertEquals("b", results.get(0).getString(1).toString());

        hiveCatalog.dropTable(tablePath, false);
    }
}
