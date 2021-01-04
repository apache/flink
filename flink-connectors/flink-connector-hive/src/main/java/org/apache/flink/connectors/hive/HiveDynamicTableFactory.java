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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.catalog.config.CatalogConfig.IS_GENERIC;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_INCLUDE;

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

    private static CatalogTable removeIsGenericFlag(Context context) {
        Map<String, String> newOptions = new HashMap<>(context.getCatalogTable().getOptions());
        boolean isGeneric = Boolean.parseBoolean(newOptions.remove(IS_GENERIC));
        // temporary table doesn't have the IS_GENERIC flag but we still consider it generic
        if (!isGeneric && !context.isTemporary()) {
            throw new ValidationException(
                    "Hive dynamic table factory now only work for generic table.");
        }
        return context.getCatalogTable().copy(newOptions);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        boolean isGeneric =
                Boolean.parseBoolean(
                        context.getCatalogTable().getOptions().get(CatalogConfig.IS_GENERIC));

        // temporary table doesn't have the IS_GENERIC flag but we still consider it generic
        if (!isGeneric && !context.isTemporary()) {
            return new HiveTableSink(
                    context.getConfiguration(),
                    new JobConf(hiveConf),
                    context.getObjectIdentifier(),
                    context.getCatalogTable());
        } else {
            return FactoryUtil.createTableSink(
                    null, // we already in the factory of catalog
                    context.getObjectIdentifier(),
                    removeIsGenericFlag(context),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        boolean isGeneric =
                Boolean.parseBoolean(
                        context.getCatalogTable().getOptions().get(CatalogConfig.IS_GENERIC));

        // temporary table doesn't have the IS_GENERIC flag but we still consider it generic
        if (!isGeneric && !context.isTemporary()) {
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
            // hive table source that has not lookup ability
            if (isStreamingSource && includeAllPartition) {
                return new HiveTableSource(
                        new JobConf(hiveConf),
                        context.getConfiguration(),
                        context.getObjectIdentifier().toObjectPath(),
                        catalogTable);
            } else {
                // hive table source that has scan and lookup ability
                return new HiveLookupTableSource(
                        new JobConf(hiveConf),
                        context.getConfiguration(),
                        context.getObjectIdentifier().toObjectPath(),
                        catalogTable);
            }

        } else {
            return FactoryUtil.createTableSource(
                    null, // we already in the factory of catalog
                    context.getObjectIdentifier(),
                    removeIsGenericFlag(context),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }
    }
}
