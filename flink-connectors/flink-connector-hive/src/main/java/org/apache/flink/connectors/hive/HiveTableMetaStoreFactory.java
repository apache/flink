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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * Hive {@link TableMetaStoreFactory}, use {@link HiveMetastoreClientWrapper} to communicate with
 * hive meta store.
 */
public class HiveTableMetaStoreFactory implements TableMetaStoreFactory {

    private static final long serialVersionUID = 1L;

    private final JobConfWrapper conf;
    private final String hiveVersion;
    private final String database;
    private final String tableName;

    HiveTableMetaStoreFactory(JobConf conf, String hiveVersion, String database, String tableName) {
        this.conf = new JobConfWrapper(conf);
        this.hiveVersion = hiveVersion;
        this.database = database;
        this.tableName = tableName;
    }

    @Override
    public HiveTableMetaStore createTableMetaStore() throws Exception {
        return new HiveTableMetaStore();
    }

    private class HiveTableMetaStore implements TableMetaStore {

        private HiveMetastoreClientWrapper client;
        private StorageDescriptor sd;

        private HiveTableMetaStore() throws TException {
            client =
                    HiveMetastoreClientFactory.create(
                            new HiveConf(conf.conf(), HiveConf.class), hiveVersion);
            sd = client.getTable(database, tableName).getSd();
        }

        @Override
        public Path getLocationPath() {
            return new Path(sd.getLocation());
        }

        @Override
        public Optional<Path> getPartition(LinkedHashMap<String, String> partSpec)
                throws Exception {
            try {
                return Optional.of(
                        new Path(
                                client.getPartition(
                                                database,
                                                tableName,
                                                new ArrayList<>(partSpec.values()))
                                        .getSd()
                                        .getLocation()));
            } catch (NoSuchObjectException ignore) {
                return Optional.empty();
            }
        }

        @Override
        public void createOrAlterPartition(
                LinkedHashMap<String, String> partitionSpec, Path partitionPath) throws Exception {
            Partition partition;
            try {
                partition =
                        client.getPartition(
                                database, tableName, new ArrayList<>(partitionSpec.values()));
            } catch (NoSuchObjectException e) {
                createPartition(partitionSpec, partitionPath);
                return;
            }
            alterPartition(partitionSpec, partitionPath, partition);
        }

        private void createPartition(LinkedHashMap<String, String> partSpec, Path path)
                throws Exception {
            StorageDescriptor newSd = new StorageDescriptor(sd);
            newSd.setLocation(path.toString());
            Partition partition =
                    HiveTableUtil.createHivePartition(
                            database,
                            tableName,
                            new ArrayList<>(partSpec.values()),
                            newSd,
                            new HashMap<>());
            partition.setValues(new ArrayList<>(partSpec.values()));
            client.add_partition(partition);
        }

        private void alterPartition(
                LinkedHashMap<String, String> partitionSpec,
                Path partitionPath,
                Partition currentPartition)
                throws Exception {
            StorageDescriptor partSD = currentPartition.getSd();
            // the following logic copied from Hive::alterPartitionSpecInMemory
            partSD.setOutputFormat(sd.getOutputFormat());
            partSD.setInputFormat(sd.getInputFormat());
            partSD.getSerdeInfo().setSerializationLib(sd.getSerdeInfo().getSerializationLib());
            partSD.getSerdeInfo().setParameters(sd.getSerdeInfo().getParameters());
            partSD.setBucketCols(sd.getBucketCols());
            partSD.setNumBuckets(sd.getNumBuckets());
            partSD.setSortCols(sd.getSortCols());
            partSD.setLocation(partitionPath.toString());
            client.alter_partition(database, tableName, currentPartition);
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
