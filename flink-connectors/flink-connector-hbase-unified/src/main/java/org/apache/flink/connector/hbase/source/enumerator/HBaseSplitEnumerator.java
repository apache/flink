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

package org.apache.flink.connector.hbase.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.source.HBaseSourceOptions;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * The SourceEnumerator is responsible for work discovery and assignment of splits. The
 * HBaseSourceEnumerator does the following:
 *
 * <ol>
 *   <li>Connect to HBase to discover all column families of the table that is being watched.
 *   <li>Equally distribute column families to HBaseSourceSplits depending on the parallelism. One
 *       {@link HBaseSourceSplit} will be created per parallel reader. Each {@link HBaseSourceSplit}
 *       can cover multiple column families.
 * </ol>
 */
@Internal
public class HBaseSplitEnumerator
        implements SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSplitEnumerator.class);

    private final SplitEnumeratorContext<HBaseSourceSplit> context;
    private final Queue<HBaseSourceSplit> remainingSplits;
    private final String table;
    private final byte[] serializedHBaseConfig;
    private final String host;

    public HBaseSplitEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> context,
            byte[] serializedHBaseConfig,
            Configuration sourceConfiguration) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>();
        this.host = sourceConfiguration.get(HBaseSourceOptions.HOST_NAME);
        this.table = sourceConfiguration.get(HBaseSourceOptions.TABLE_NAME);
        this.serializedHBaseConfig = serializedHBaseConfig;
        LOG.debug("Constructed with {} remaining splits", remainingSplits.size());
    }

    @Override
    public void start() {
        org.apache.hadoop.conf.Configuration hbaseConfiguration =
                HBaseConfigurationUtil.deserializeConfiguration(this.serializedHBaseConfig, null);
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
                Admin admin = connection.getAdmin()) {
            ColumnFamilyDescriptor[] colFamDes =
                    admin.getDescriptor(TableName.valueOf(this.table)).getColumnFamilies();
            List<HBaseSourceSplit> splits = new ArrayList<>();
            List<ArrayList<String>> parallelInstances =
                    new ArrayList<>(context.currentParallelism());

            for (int i = 0; i < context.currentParallelism(); i++) {
                parallelInstances.add(new ArrayList<>());
            }
            int index = 0;
            for (ColumnFamilyDescriptor colFamDe : colFamDes) {
                parallelInstances.get(index).add(colFamDe.getNameAsString());
                index = (index + 1) % parallelInstances.size();
            }

            parallelInstances.forEach(
                    (list) -> {
                        if (!list.isEmpty()) {
                            splits.add(new HBaseSourceSplit(list.get(0), host, list));
                        }
                    });
            addSplits(splits);
        } catch (IOException e) {
            throw new RuntimeException("could not start HBaseSplitEnumerator", e);
        }
    }

    @Override
    public void close() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final HBaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<HBaseSourceSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("adding reader with id {}", subtaskId);
        HBaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Collection<HBaseSourceSplit> snapshotState() {
        return remainingSplits;
    }

    public void addSplits(Collection<HBaseSourceSplit> splits) {
        remainingSplits.addAll(splits);
    }
}
