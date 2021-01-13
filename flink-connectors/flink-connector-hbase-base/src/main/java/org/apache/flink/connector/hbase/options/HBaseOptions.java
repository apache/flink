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

package org.apache.flink.connector.hbase.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_ZK_NODE_PARENT;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** Common Options for HBase. */
@Internal
public class HBaseOptions implements Serializable {

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of HBase table to connect.");

    public static final ConfigOption<String> ZOOKEEPER_QUORUM =
            ConfigOptions.key("zookeeper.quorum")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The HBase Zookeeper quorum.");

    public static final ConfigOption<String> ZOOKEEPER_ZNODE_PARENT =
            ConfigOptions.key("zookeeper.znode.parent")
                    .stringType()
                    .defaultValue("/hbase")
                    .withDescription("The root dir in Zookeeper for HBase cluster.");

    public static final ConfigOption<String> NULL_STRING_LITERAL =
            ConfigOptions.key("null-string-literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Representation for null values for string fields. HBase source and "
                                    + "sink encodes/decodes empty bytes as null values for all types except string type.");

    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "Writing option, maximum size in memory of buffered rows for each "
                                    + "writing request. This can improve performance for writing data to HBase database, "
                                    + "but may increase the latency. Can be set to '0' to disable it. ");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Writing option, maximum number of rows to buffer for each writing request. "
                                    + "This can improve performance for writing data to HBase database, but may increase the latency. "
                                    + "Can be set to '0' to disable it.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "Writing option, the interval to flush any buffered rows. "
                                    + "This can improve performance for writing data to HBase database, but may increase the latency. "
                                    + "Can be set to '0' to disable it. Note, both 'sink.buffer-flush.max-size' and 'sink.buffer-flush.max-rows' "
                                    + "can be set to '0' with the flush interval set allowing for complete async processing of buffered actions.");

    // Prefix for HBase specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /**
     * Checks that the HBase table have row key defined. A row key is defined as an atomic type, and
     * column families and qualifiers are defined as ROW type. There shouldn't be multiple atomic
     * type columns in the schema. The PRIMARY KEY constraint is optional, if exist, the primary key
     * constraint must be defined on the single row key field.
     */
    public static void validatePrimaryKey(TableSchema schema) {
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

    /**
     * Options for validate HBase source table , The reason for separate source and sink is to
     * facilitate future expansion of parameters.
     *
     * @param tableOptions
     */
    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateTableName(tableOptions);
        validateZookeeperQuorum(tableOptions);
    }

    /**
     * Options for validate HBase sink table.
     *
     * @param tableOptions
     */
    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateTableName(tableOptions);
        validateZookeeperQuorum(tableOptions);
    }

    private static void validateTableName(ReadableConfig tableOptions) {
        Optional<String> tableName = tableOptions.getOptional(TABLE_NAME);
        if (!tableName.isPresent()) {
            throw new ValidationException("Option 'table-name' must be set.");
        }
    }

    private static void validateZookeeperQuorum(ReadableConfig tableOptions) {
        Optional<String> zookeeperQuorum = tableOptions.getOptional(ZOOKEEPER_QUORUM);
        if (!zookeeperQuorum.isPresent()) {
            throw new ValidationException("Option 'zookeeper.quorum' must be set.");
        }
    }

    public static String getTableName(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TABLE_NAME).orElse(null);
    }

    private static String getZookeeperQuorum(ReadableConfig tableOptions) {
        return tableOptions.getOptional(ZOOKEEPER_QUORUM).orElse(null);
    }

    private static String getZookeeperNodeParent(ReadableConfig tableOptions) {
        return tableOptions.get(ZOOKEEPER_ZNODE_PARENT);
    }

    public static String getNullStringLiteral(ReadableConfig tableOptions) {
        return tableOptions.get(NULL_STRING_LITERAL);
    }

    public static HBaseWriteOptions getHBaseWriteOptions(ReadableConfig tableOptions) {
        HBaseWriteOptions.Builder builder = HBaseWriteOptions.builder();
        builder.setBufferFlushIntervalMillis(
                tableOptions.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.setBufferFlushMaxRows(tableOptions.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.setBufferFlushMaxSizeInBytes(
                tableOptions.get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes());
        builder.setParallelism(tableOptions.getOptional(SINK_PARALLELISM).orElse(null));
        return builder.build();
    }

    /**
     * config HBase Configuration.
     *
     * @param tableOptions table option
     * @param options properties option
     * @return
     */
    public static Configuration getHBaseConfiguration(
            ReadableConfig tableOptions, Map<String, String> options) {
        // create default configuration from current runtime env (`hbase-site.xml` in classpath)
        // first,
        Configuration hbaseClientConf = HBaseConfigurationUtil.getHBaseConfiguration();
        hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, getZookeeperQuorum(tableOptions));
        hbaseClientConf.set(
                HConstants.ZOOKEEPER_ZNODE_PARENT, getZookeeperNodeParent(tableOptions));
        // add HBase properties
        final Properties properties = getHBaseClientProperties(options);
        properties.forEach((k, v) -> hbaseClientConf.set(k.toString(), v.toString()));
        return hbaseClientConf;
    }

    // get HBase table properties
    private static Properties getHBaseClientProperties(Map<String, String> tableOptions) {
        final Properties hbaseProperties = new Properties();

        if (containsHBaseClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                hbaseProperties.put(subKey, value);
                            });
        }
        return hbaseProperties;
    }

    /** Returns wether the table options contains HBase client properties or not. 'properties'. */
    private static boolean containsHBaseClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    public static Configuration getHBaseConf(DescriptorProperties descriptorProperties) {
        Configuration hbaseClientConf = HBaseConfigurationUtil.createHBaseConf();
        descriptorProperties
                .getOptionalString(CONNECTOR_ZK_QUORUM)
                .ifPresent(zkQ -> hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, zkQ));

        descriptorProperties
                .getOptionalString(CONNECTOR_ZK_NODE_PARENT)
                .ifPresent(v -> hbaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, v));

        // add HBase properties
        descriptorProperties
                .getPropertiesWithPrefix(CONNECTOR_PROPERTIES)
                .forEach(hbaseClientConf::set);
        return hbaseClientConf;
    }
}
