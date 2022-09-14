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

package org.apache.flink.connector.hbase1;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase1.sink.HBaseDynamicTableSink;
import org.apache.flink.connector.hbase1.source.HBaseDynamicTableSource;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import org.apache.hadoop.conf.Configuration;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.LOOKUP_ASYNC;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.NULL_STRING_LITERAL;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_MAX_SIZE;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.ZOOKEEPER_QUORUM;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.ZOOKEEPER_ZNODE_PARENT;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptionsUtil.getHBaseConfiguration;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptionsUtil.getHBaseWriteOptions;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptionsUtil.validatePrimaryKey;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/** HBase connector factory. */
@Internal
public class HBase1DynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "hbase-1.4";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        validatePrimaryKey(context.getPhysicalRowDataType(), context.getPrimaryKeyIndexes());

        String tableName = tableOptions.get(TABLE_NAME);
        Configuration hbaseClientConf = getHBaseConfiguration(tableOptions);
        String nullStringLiteral = tableOptions.get(NULL_STRING_LITERAL);
        HBaseTableSchema hbaseSchema =
                HBaseTableSchema.fromDataType(context.getPhysicalRowDataType());
        LookupCache cache = null;

        // Backward compatible to legacy caching options
        if (tableOptions.get(LOOKUP_CACHE_MAX_ROWS) > 0
                && tableOptions.get(LOOKUP_CACHE_TTL).compareTo(Duration.ZERO) > 0) {
            cache =
                    DefaultLookupCache.newBuilder()
                            .maximumSize(tableOptions.get(LOOKUP_CACHE_MAX_ROWS))
                            .expireAfterWrite(tableOptions.get(LOOKUP_CACHE_TTL))
                            .build();
        }

        if (tableOptions
                .get(LookupOptions.CACHE_TYPE)
                .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        }

        return new HBaseDynamicTableSource(
                hbaseClientConf,
                tableName,
                hbaseSchema,
                nullStringLiteral,
                tableOptions.get(LookupOptions.MAX_RETRIES),
                cache);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        validatePrimaryKey(context.getPhysicalRowDataType(), context.getPrimaryKeyIndexes());

        String tableName = tableOptions.get(TABLE_NAME);
        Configuration hbaseConf = getHBaseConfiguration(tableOptions);
        HBaseWriteOptions hBaseWriteOptions = getHBaseWriteOptions(tableOptions);
        String nullStringLiteral = tableOptions.get(NULL_STRING_LITERAL);
        HBaseTableSchema hbaseSchema =
                HBaseTableSchema.fromDataType(context.getPhysicalRowDataType());

        return new HBaseDynamicTableSink(
                tableName, hbaseSchema, hbaseConf, hBaseWriteOptions, nullStringLiteral);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TABLE_NAME);
        set.add(ZOOKEEPER_QUORUM);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(ZOOKEEPER_ZNODE_PARENT);
        set.add(NULL_STRING_LITERAL);
        set.add(SINK_BUFFER_FLUSH_MAX_SIZE);
        set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        set.add(SINK_BUFFER_FLUSH_INTERVAL);
        set.add(SINK_PARALLELISM);
        set.add(LOOKUP_ASYNC);
        set.add(LOOKUP_CACHE_MAX_ROWS);
        set.add(LOOKUP_CACHE_TTL);
        set.add(LOOKUP_MAX_RETRIES);
        set.add(LookupOptions.CACHE_TYPE);
        set.add(LookupOptions.MAX_RETRIES);
        set.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS);
        set.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE);
        set.add(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        set.add(LookupOptions.PARTIAL_CACHE_MAX_ROWS);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        TABLE_NAME,
                        ZOOKEEPER_ZNODE_PARENT,
                        ZOOKEEPER_QUORUM,
                        NULL_STRING_LITERAL,
                        SINK_BUFFER_FLUSH_MAX_SIZE,
                        SINK_BUFFER_FLUSH_MAX_ROWS,
                        SINK_BUFFER_FLUSH_INTERVAL,
                        LOOKUP_CACHE_MAX_ROWS,
                        LOOKUP_CACHE_TTL,
                        LOOKUP_MAX_RETRIES)
                .collect(Collectors.toSet());
    }
}
