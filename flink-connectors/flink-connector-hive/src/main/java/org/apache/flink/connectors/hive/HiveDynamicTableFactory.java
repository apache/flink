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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.util.JobConfUtils;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.util.Set;

import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.STREAMING_SOURCE_PARTITION_INCLUDE;

/** A dynamic table factory implementation for Hive catalog. */
public class HiveDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private final HiveConf hiveConf;

    public HiveDynamicTableFactory(HiveConf hiveConf) {
        this.hiveConf = hiveConf;
    }

    @Override
    public String factoryIdentifier() {
        throw new UnsupportedOperationException("Hive factory is only work for catalog.");
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        throw new UnsupportedOperationException("Hive factory is only work for catalog.");
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        throw new UnsupportedOperationException("Hive factory is only work for catalog.");
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        boolean isHiveTable = HiveCatalog.isHiveTable(context.getCatalogTable().getOptions());

        // we don't support temporary hive tables yet
        if (isHiveTable && !context.isTemporary()) {
            Integer configuredParallelism =
                    Configuration.fromMap(context.getCatalogTable().getOptions())
                            .get(FileSystemConnectorOptions.SINK_PARALLELISM);
            JobConf jobConf = JobConfUtils.createJobConfWithCredentials(hiveConf);
            return new HiveTableSink(
                    context.getConfiguration(),
                    jobConf,
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    configuredParallelism);
        } else {
            return FactoryUtil.createTableSink(
                    null, // we already in the factory of catalog
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        boolean isHiveTable = HiveCatalog.isHiveTable(context.getCatalogTable().getOptions());

        // we don't support temporary hive tables yet
        if (isHiveTable && !context.isTemporary()) {
            CatalogTable catalogTable = Preconditions.checkNotNull(context.getCatalogTable());

            boolean isStreamingSource =
                    Boolean.parseBoolean(
                            catalogTable
                                    .getOptions()
                                    .getOrDefault(
                                            STREAMING_SOURCE_ENABLE.key(),
                                            STREAMING_SOURCE_ENABLE.defaultValue().toString()));

            boolean includeAllPartition =
                    STREAMING_SOURCE_PARTITION_INCLUDE
                            .defaultValue()
                            .equals(
                                    catalogTable
                                            .getOptions()
                                            .getOrDefault(
                                                    STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                                                    STREAMING_SOURCE_PARTITION_INCLUDE
                                                            .defaultValue()));
            JobConf jobConf = JobConfUtils.createJobConfWithCredentials(hiveConf);
            // hive table source that has not lookup ability
            if (isStreamingSource && includeAllPartition) {
                return new HiveTableSource(
                        jobConf,
                        context.getConfiguration(),
                        context.getObjectIdentifier().toObjectPath(),
                        catalogTable);
            } else {
                // hive table source that has scan and lookup ability
                return new HiveLookupTableSource(
                        jobConf,
                        context.getConfiguration(),
                        context.getObjectIdentifier().toObjectPath(),
                        catalogTable);
            }

        } else {
            return FactoryUtil.createTableSource(
                    null, // we already in the factory of catalog
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }
    }
}
