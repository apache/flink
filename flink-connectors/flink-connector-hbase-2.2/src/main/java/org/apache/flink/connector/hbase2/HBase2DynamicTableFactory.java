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

package org.apache.flink.connector.hbase2;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.hbase.options.HBaseLookupOptions;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase2.sink.HBaseDynamicTableSink;
import org.apache.flink.connector.hbase2.source.HBaseDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.hbase.options.HBaseOptions.LOOKUP_ASYNC;
import static org.apache.flink.connector.hbase.options.HBaseOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.hbase.options.HBaseOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.hbase.options.HBaseOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.hbase.options.HBaseOptions.NULL_STRING_LITERAL;
import static org.apache.flink.connector.hbase.options.HBaseOptions.PROPERTIES_PREFIX;
import static org.apache.flink.connector.hbase.options.HBaseOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.hbase.options.HBaseOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.hbase.options.HBaseOptions.SINK_BUFFER_FLUSH_MAX_SIZE;
import static org.apache.flink.connector.hbase.options.HBaseOptions.TABLE_NAME;
import static org.apache.flink.connector.hbase.options.HBaseOptions.ZOOKEEPER_QUORUM;
import static org.apache.flink.connector.hbase.options.HBaseOptions.ZOOKEEPER_ZNODE_PARENT;
import static org.apache.flink.connector.hbase.options.HBaseOptions.getHBaseConfiguration;
import static org.apache.flink.connector.hbase.options.HBaseOptions.getHBaseLookupOptions;
import static org.apache.flink.connector.hbase.options.HBaseOptions.getHBaseWriteOptions;
import static org.apache.flink.connector.hbase.options.HBaseOptions.validatePrimaryKey;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/** HBase connector factory. */
public class HBase2DynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "hbase-2.2";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        TableSchema tableSchema = context.getCatalogTable().getSchema();
        Map<String, String> options = context.getCatalogTable().getOptions();

        validatePrimaryKey(tableSchema);

        String tableName = tableOptions.get(TABLE_NAME);
        Configuration hbaseConf = getHBaseConfiguration(options);
        HBaseLookupOptions lookupOptions = getHBaseLookupOptions(tableOptions);
        String nullStringLiteral = tableOptions.get(NULL_STRING_LITERAL);
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(tableSchema);

        return new HBaseDynamicTableSource(
                hbaseConf, tableName, hbaseSchema, nullStringLiteral, lookupOptions);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        TableSchema tableSchema = context.getCatalogTable().getSchema();
        Map<String, String> options = context.getCatalogTable().getOptions();

        validatePrimaryKey(tableSchema);

        String tableName = tableOptions.get(TABLE_NAME);
        Configuration hbaseConf = getHBaseConfiguration(options);
        HBaseWriteOptions hBaseWriteOptions = getHBaseWriteOptions(tableOptions);
        String nullStringLiteral = tableOptions.get(NULL_STRING_LITERAL);
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(tableSchema);

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
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(ZOOKEEPER_ZNODE_PARENT);
        set.add(ZOOKEEPER_QUORUM);
        set.add(NULL_STRING_LITERAL);
        set.add(SINK_BUFFER_FLUSH_MAX_SIZE);
        set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        set.add(SINK_BUFFER_FLUSH_INTERVAL);
        set.add(SINK_PARALLELISM);
        set.add(LOOKUP_ASYNC);
        set.add(LOOKUP_CACHE_MAX_ROWS);
        set.add(LOOKUP_CACHE_TTL);
        set.add(LOOKUP_MAX_RETRIES);
        return set;
    }
}
