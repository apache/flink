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
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
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
import org.apache.hadoop.hbase.HConstants;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/** HBase connector factory. */
public class HBase2DynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "hbase-2.2";

    private static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of HBase table to connect.");

    private static final ConfigOption<String> ZOOKEEPER_QUORUM =
            ConfigOptions.key("zookeeper.quorum")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The HBase Zookeeper quorum.");

    private static final ConfigOption<String> ZOOKEEPER_ZNODE_PARENT =
            ConfigOptions.key("zookeeper.znode.parent")
                    .stringType()
                    .defaultValue("/hbase")
                    .withDescription("The root dir in Zookeeper for HBase cluster.");

    private static final ConfigOption<String> NULL_STRING_LITERAL =
            ConfigOptions.key("null-string-literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Representation for null values for string fields. HBase source and "
                                    + "sink encodes/decodes empty bytes as null values for all types except string type.");

    private static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "Writing option, maximum size in memory of buffered rows for each "
                                    + "writing request. This can improve performance for writing data to HBase database, "
                                    + "but may increase the latency. Can be set to '0' to disable it. ");

    private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Writing option, maximum number of rows to buffer for each writing request. "
                                    + "This can improve performance for writing data to HBase database, but may increase the latency. "
                                    + "Can be set to '0' to disable it.");

    private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "Writing option, the interval to flush any buffered rows. "
                                    + "This can improve performance for writing data to HBase database, but may increase the latency. "
                                    + "Can be set to '0' to disable it. Note, both 'sink.buffer-flush.max-size' and 'sink.buffer-flush.max-rows' "
                                    + "can be set to '0' with the flush interval set allowing for complete async processing of buffered actions.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        validatePrimaryKey(tableSchema);

        String hTableName = helper.getOptions().get(TABLE_NAME);

        String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(tableSchema);

        return new HBaseDynamicTableSource(
                getHbaseConf(helper), hTableName, hbaseSchema, nullStringLiteral);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        validatePrimaryKey(tableSchema);

        String hTableName = helper.getOptions().get(TABLE_NAME);

        HBaseWriteOptions.Builder writeBuilder = HBaseWriteOptions.builder();
        writeBuilder.setBufferFlushMaxSizeInBytes(
                helper.getOptions().get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes());
        writeBuilder.setBufferFlushIntervalMillis(
                helper.getOptions().get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        writeBuilder.setBufferFlushMaxRows(helper.getOptions().get(SINK_BUFFER_FLUSH_MAX_ROWS));
        writeBuilder.setParallelism(helper.getOptions().getOptional(SINK_PARALLELISM).orElse(null));
        String nullStringLiteral = helper.getOptions().get(NULL_STRING_LITERAL);
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(tableSchema);

        return new HBaseDynamicTableSink(
                hTableName,
                hbaseSchema,
                getHbaseConf(helper),
                writeBuilder.build(),
                nullStringLiteral);
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
        return set;
    }

    // ------------------------------------------------------------------------------------------

    /**
     * Checks that the HBase table have row key defined. A row key is defined as an atomic type, and
     * column families and qualifiers are defined as ROW type. There shouldn't be multiple atomic
     * type columns in the schema. The PRIMARY KEY constraint is optional, if exist, the primary key
     * constraint must be defined on the single row key field.
     */
    private static void validatePrimaryKey(TableSchema schema) {
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(schema);
        if (!hbaseSchema.getRowKeyName().isPresent()) {
            throw new IllegalArgumentException(
                    "HBase table requires to define a row key field. "
                            + "A row key field is defined as an atomic type, "
                            + "column families and qualifiers are defined as ROW type.");
        }
        schema.getPrimaryKey()
                .ifPresent(
                        k -> {
                            if (k.getColumns().size() > 1) {
                                throw new IllegalArgumentException(
                                        "HBase table doesn't support a primary Key on multiple columns. "
                                                + "The primary key of HBase table must be defined on row key field.");
                            }
                            if (!hbaseSchema.getRowKeyName().get().equals(k.getColumns().get(0))) {
                                throw new IllegalArgumentException(
                                        "Primary key of HBase table must be defined on the row key field. "
                                                + "A row key field is defined as an atomic type, "
                                                + "column families and qualifiers are defined as ROW type.");
                            }
                        });
    }

    private static Configuration getHbaseConf(TableFactoryHelper helper) {
        Configuration hbaseClientConf = HBaseConfigurationUtil.createHBaseConf();
        hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, helper.getOptions().get(ZOOKEEPER_QUORUM));
        hbaseClientConf.set(
                HConstants.ZOOKEEPER_ZNODE_PARENT, helper.getOptions().get(ZOOKEEPER_ZNODE_PARENT));

        return hbaseClientConf;
    }
}
