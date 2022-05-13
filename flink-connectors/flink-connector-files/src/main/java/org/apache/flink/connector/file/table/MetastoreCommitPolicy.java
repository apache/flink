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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.table.TableMetaStoreFactory.TableMetaStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

/**
 * Partition commit policy to update metastore.
 *
 * <p>If this is for file system table, the metastore is a empty implemantation. If this is for hive
 * table, the metastore is for connecting to hive metastore.
 */
@Internal
public class MetastoreCommitPolicy implements PartitionCommitPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(MetastoreCommitPolicy.class);

    private TableMetaStore metaStore;

    public void setMetastore(TableMetaStore metaStore) {
        this.metaStore = metaStore;
    }

    @Override
    public void commit(Context context) throws Exception {
        LinkedHashMap<String, String> partitionSpec = context.partitionSpec();
        metaStore
                .getPartition(partitionSpec)
                .ifPresent(
                        path ->
                                LOG.warn(
                                        "The partition {} has existed before current commit,"
                                                + " the path is {}, this partition will be altered instead of being created",
                                        partitionSpec,
                                        path));
        metaStore.createOrAlterPartition(partitionSpec, context.partitionPath());
        LOG.info("Committed partition {} to metastore", partitionSpec);
    }
}
