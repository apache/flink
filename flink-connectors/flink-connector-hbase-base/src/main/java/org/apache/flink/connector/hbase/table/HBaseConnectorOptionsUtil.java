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

package org.apache.flink.connector.hbase.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_BUFFER_FLUSH_MAX_SIZE;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.ZOOKEEPER_QUORUM;
import static org.apache.flink.connector.hbase.table.HBaseConnectorOptions.ZOOKEEPER_ZNODE_PARENT;

/** Utilities for {@link HBaseConnectorOptions}. */
@Internal
public class HBaseConnectorOptionsUtil {

    /** Prefix for HBase specific properties. */
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
    public static void validatePrimaryKey(DataType dataType, int[] primaryKeyIndexes) {
        HBaseTableSchema hbaseSchema = HBaseTableSchema.fromDataType(dataType);
        if (!hbaseSchema.getRowKeyName().isPresent()) {
            throw new IllegalArgumentException(
                    "HBase table requires to define a row key field. "
                            + "A row key field is defined as an atomic type, "
                            + "column families and qualifiers are defined as ROW type.");
        }
        if (primaryKeyIndexes.length == 0) {
            return;
        }
        if (primaryKeyIndexes.length > 1) {
            throw new IllegalArgumentException(
                    "HBase table doesn't support a primary Key on multiple columns. "
                            + "The primary key of HBase table must be defined on row key field.");
        }
        if (!hbaseSchema
                .getRowKeyName()
                .get()
                .equals(DataType.getFieldNames(dataType).get(primaryKeyIndexes[0]))) {
            throw new IllegalArgumentException(
                    "Primary key of HBase table must be defined on the row key field. "
                            + "A row key field is defined as an atomic type, "
                            + "column families and qualifiers are defined as ROW type.");
        }
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

    /** config HBase Configuration. */
    public static Configuration getHBaseConfiguration(ReadableConfig tableOptions) {
        // create default configuration from current runtime env (`hbase-site.xml` in classpath)
        // first,
        Configuration hbaseClientConf = HBaseConfigurationUtil.getHBaseConfiguration();
        hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, tableOptions.get(ZOOKEEPER_QUORUM));
        hbaseClientConf.set(
                HConstants.ZOOKEEPER_ZNODE_PARENT, tableOptions.get(ZOOKEEPER_ZNODE_PARENT));
        // add HBase properties
        final Properties properties =
                getHBaseClientProperties(
                        ((org.apache.flink.configuration.Configuration) tableOptions).toMap());
        properties.forEach((k, v) -> hbaseClientConf.set(k.toString(), v.toString()));
        return hbaseClientConf;
    }

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

    /** Returns whether the table options contains HBase client properties or not. 'properties'. */
    private static boolean containsHBaseClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    private HBaseConnectorOptionsUtil() {}
}
