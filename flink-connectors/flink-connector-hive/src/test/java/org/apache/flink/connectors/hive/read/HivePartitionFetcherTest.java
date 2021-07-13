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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions.PartitionOrder;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.STREAMING_SOURCE_PARTITION_ORDER;
import static org.junit.Assert.assertEquals;

/** Tests for hive partition fetch implementations. */
public class HivePartitionFetcherTest {

    @Test
    public void testIgnoreNonExistPartition() throws Exception {
        // it's possible a partition path exists but the partition is not added to HMS, e.g. the
        // partition is still being loaded, or the path is simply misplaced
        // make sure the fetch can ignore such paths
        HiveCatalog hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();

        // create test table
        String[] fieldNames = new String[] {"i", "date"};
        DataType[] fieldTypes = new DataType[] {DataTypes.INT(), DataTypes.STRING()};
        TableSchema schema = TableSchema.builder().fields(fieldNames, fieldTypes).build();
        List<String> partitionKeys = Collections.singletonList("date");
        Map<String, String> options = new HashMap<>();
        options.put("connector", "hive");
        CatalogTable catalogTable = new CatalogTableImpl(schema, partitionKeys, options, null);
        ObjectPath tablePath = new ObjectPath("default", "test");
        hiveCatalog.createTable(tablePath, catalogTable, false);

        // add a valid partition path
        Table hiveTable = hiveCatalog.getHiveTable(tablePath);
        Path path = new Path(hiveTable.getSd().getLocation(), "date=2021-06-18");
        FileSystem fs = path.getFileSystem(hiveCatalog.getHiveConf());
        fs.mkdirs(path);

        // test partition-time order
        Configuration flinkConf = new Configuration();
        flinkConf.set(STREAMING_SOURCE_PARTITION_ORDER, PartitionOrder.PARTITION_TIME);
        HiveShim hiveShim = HiveShimLoader.loadHiveShim(hiveCatalog.getHiveVersion());
        JobConfWrapper jobConfWrapper = new JobConfWrapper(new JobConf(hiveCatalog.getHiveConf()));
        String defaultPartName = "__HIVE_DEFAULT_PARTITION__";
        MyHivePartitionFetcherContext fetcherContext =
                new MyHivePartitionFetcherContext(
                        tablePath,
                        hiveShim,
                        jobConfWrapper,
                        partitionKeys,
                        fieldTypes,
                        fieldNames,
                        flinkConf,
                        defaultPartName);
        fetcherContext.open();
        assertEquals(0, fetcherContext.getComparablePartitionValueList().size());

        // test create-time order
        flinkConf.set(STREAMING_SOURCE_PARTITION_ORDER, PartitionOrder.CREATE_TIME);
        fetcherContext =
                new MyHivePartitionFetcherContext(
                        tablePath,
                        hiveShim,
                        jobConfWrapper,
                        partitionKeys,
                        fieldTypes,
                        fieldNames,
                        flinkConf,
                        defaultPartName);
        fetcherContext.open();
        assertEquals(0, fetcherContext.getComparablePartitionValueList().size());

        // test partition-name order
        flinkConf.set(STREAMING_SOURCE_PARTITION_ORDER, PartitionOrder.PARTITION_NAME);
        fetcherContext =
                new MyHivePartitionFetcherContext(
                        tablePath,
                        hiveShim,
                        jobConfWrapper,
                        partitionKeys,
                        fieldTypes,
                        fieldNames,
                        flinkConf,
                        defaultPartName);
        fetcherContext.open();
        assertEquals(0, fetcherContext.getComparablePartitionValueList().size());
    }

    private static class MyHivePartitionFetcherContext
            extends HivePartitionFetcherContextBase<Partition> {

        private static final long serialVersionUID = 1L;

        public MyHivePartitionFetcherContext(
                ObjectPath tablePath,
                HiveShim hiveShim,
                JobConfWrapper confWrapper,
                List<String> partitionKeys,
                DataType[] fieldTypes,
                String[] fieldNames,
                Configuration configuration,
                String defaultPartitionName) {
            super(
                    tablePath,
                    hiveShim,
                    confWrapper,
                    partitionKeys,
                    fieldTypes,
                    fieldNames,
                    configuration,
                    defaultPartitionName);
        }

        @Override
        public Optional<Partition> getPartition(List<String> partValues) throws Exception {
            return Optional.empty();
        }
    }
}
