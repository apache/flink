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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connectors.hive.util.JobConfUtils;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalogLock;
import org.apache.flink.table.connector.RequireCatalogLock;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.util.Set;

import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_PARTITION_INCLUDE;
import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_READ_PARTITION_WITH_SUBDIRECTORY_ENABLED;

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
        final boolean isHiveTable = HiveCatalog.isHiveTable(context.getCatalogTable().getOptions());

        // we don't support temporary hive tables yet
        if (!isHiveTable || context.isTemporary()) {
            DynamicTableSink sink =
                    FactoryUtil.createDynamicTableSink(
                            null,
                            context.getObjectIdentifier(),
                            context.getCatalogTable(),
                            context.getConfiguration(),
                            context.getClassLoader(),
                            context.isTemporary());
            if (sink instanceof RequireCatalogLock) {
                ((RequireCatalogLock) sink).setLockFactory(HiveCatalogLock.createFactory(hiveConf));
            }
            return sink;
        }

        final Integer configuredSinkParallelism =
                Configuration.fromMap(context.getCatalogTable().getOptions())
                        .get(FileSystemConnectorOptions.SINK_PARALLELISM);
        final JobConf jobConf = JobConfUtils.createJobConfWithCredentials(hiveConf);
        return new HiveTableSink(
                context.getConfiguration(),
                jobConf,
                context.getObjectIdentifier(),
                context.getCatalogTable(),
                configuredSinkParallelism);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final ReadableConfig configuration =
                Configuration.fromMap(context.getCatalogTable().getOptions());

        final boolean isHiveTable = HiveCatalog.isHiveTable(context.getCatalogTable().getOptions());

        // we don't support temporary hive tables yet
        if (!isHiveTable || context.isTemporary()) {
            DynamicTableSource source =
                    FactoryUtil.createDynamicTableSource(
                            null,
                            context.getObjectIdentifier(),
                            context.getCatalogTable(),
                            context.getConfiguration(),
                            context.getClassLoader(),
                            context.isTemporary());
            if (source instanceof RequireCatalogLock) {
                ((RequireCatalogLock) source)
                        .setLockFactory(HiveCatalogLock.createFactory(hiveConf));
            }
            return source;
        }

        final ResolvedCatalogTable catalogTable =
                Preconditions.checkNotNull(context.getCatalogTable());

        final boolean isStreamingSource = configuration.get(STREAMING_SOURCE_ENABLE);
        final boolean includeAllPartition =
                STREAMING_SOURCE_PARTITION_INCLUDE
                        .defaultValue()
                        .equals(configuration.get(STREAMING_SOURCE_PARTITION_INCLUDE));
        final JobConf jobConf = JobConfUtils.createJobConfWithCredentials(hiveConf);
        boolean readSubDirectory =
                context.getConfiguration()
                        .get(TABLE_EXEC_HIVE_READ_PARTITION_WITH_SUBDIRECTORY_ENABLED);
        // set whether to read directory recursively
        jobConf.set(FileInputFormat.INPUT_DIR_RECURSIVE, String.valueOf(readSubDirectory));

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
    }
}
