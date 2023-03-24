/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive.read;

import org.apache.flink.connectors.hive.HiveOptions;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for HiveInputFormatPartitionReader. */
public class HiveInputFormatPartitionReaderITCase {

    @Test
    public void testReadMultipleSplits() throws Exception {
        HiveCatalog hiveCatalog = HiveTestUtils.createHiveCatalog();
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());

        if (!HiveShimLoader.getHiveVersion().startsWith("2.0")) {
            testReadFormat(tableEnv, hiveCatalog, "orc");
        }
        testReadFormat(tableEnv, hiveCatalog, "parquet");
    }

    private void testReadFormat(TableEnvironment tableEnv, HiveCatalog hiveCatalog, String format)
            throws Exception {
        String tableName = prepareData(tableEnv, format);
        ObjectPath tablePath = new ObjectPath("default", tableName);
        Schema schema = hiveCatalog.getTable(tablePath).getUnresolvedSchema();
        // create partition reader
        HiveInputFormatPartitionReader partitionReader =
                new HiveInputFormatPartitionReader(
                        HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.defaultValue(),
                        new JobConf(hiveCatalog.getHiveConf()),
                        hiveCatalog.getHiveVersion(),
                        tablePath,
                        schema.getColumns().stream()
                                .map(HiveTestUtils::getType)
                                .toArray(DataType[]::new),
                        schema.getColumns().stream()
                                .map(Schema.UnresolvedColumn::getName)
                                .toArray(String[]::new),
                        Collections.emptyList(),
                        null,
                        false);
        Table hiveTable = hiveCatalog.getHiveTable(tablePath);
        // create HiveTablePartition to read from
        HiveTablePartition tablePartition =
                new HiveTablePartition(
                        hiveTable.getSd(),
                        HiveReflectionUtils.getTableMetadata(
                                HiveShimLoader.loadHiveShim(hiveCatalog.getHiveVersion()),
                                hiveTable));
        partitionReader.open(Collections.singletonList(tablePartition));
        GenericRowData reuse = new GenericRowData(schema.getColumns().size());
        int count = 0;
        // this follows the way the partition reader is used during lookup join
        while (partitionReader.read(reuse) != null) {
            count++;
        }
        assertThat(count)
                .isEqualTo(
                        CollectionUtil.iteratorToList(
                                        tableEnv.executeSql("select * from " + tableName).collect())
                                .size());
    }

    private String prepareData(TableEnvironment tableEnv, String format) throws Exception {
        String tableName = format + "_table";
        tableEnv.executeSql(
                String.format("create table %s (i int,s string) stored as %s", tableName, format));
        tableEnv.executeSql(String.format("insert into %s values (1,'a')", tableName)).await();
        tableEnv.executeSql(String.format("insert into %s values (2,'b')", tableName)).await();
        return tableName;
    }
}
