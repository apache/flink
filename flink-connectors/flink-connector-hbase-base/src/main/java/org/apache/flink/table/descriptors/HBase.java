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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_ZK_NODE_PARENT;
import static org.apache.flink.table.descriptors.AbstractHBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

/** Connector descriptor for Apache HBase. */
@PublicEvolving
public class HBase extends ConnectorDescriptor {
    private DescriptorProperties properties = new DescriptorProperties();
    private Map<String, String> hbaseProperties;

    public HBase() {
        super(CONNECTOR_TYPE_VALUE_HBASE, 1, false);
    }

    /**
     * Set the Apache HBase version to be used. Required.
     *
     * @param version HBase version. E.g., "1.4.3".
     */
    public HBase version(String version) {
        properties.putString(CONNECTOR_VERSION, version);
        return this;
    }

    /**
     * Set the HBase table name, Required.
     *
     * @param tableName Name of HBase table. E.g., "testNamespace:testTable", "testDefaultTable"
     */
    public HBase tableName(String tableName) {
        properties.putString(CONNECTOR_TABLE_NAME, tableName);
        return this;
    }

    /**
     * Set the zookeeper quorum address to connect the HBase cluster. Required.
     *
     * @param zookeeperQuorum zookeeper quorum address to connect the HBase cluster. E.g.,
     *     "localhost:2181,localhost:2182,localhost:2183".
     */
    public HBase zookeeperQuorum(String zookeeperQuorum) {
        properties.putString(CONNECTOR_ZK_QUORUM, zookeeperQuorum);
        return this;
    }

    /**
     * Set the zookeeper node parent path of HBase cluster. Default to use "/hbase", Optional.
     *
     * @param zookeeperNodeParent zookeeper node path of hbase cluster. E.g,
     *     "/hbase/example-root-znode".
     */
    public HBase zookeeperNodeParent(String zookeeperNodeParent) {
        properties.putString(CONNECTOR_ZK_NODE_PARENT, zookeeperNodeParent);
        return this;
    }

    /**
     * Set threshold when to flush buffered request based on the memory byte size of rows currently
     * added. Default to <code>2mb</code>. Optional.
     *
     * @param maxSize the maximum size (using the syntax of {@link MemorySize}).
     */
    public HBase writeBufferFlushMaxSize(String maxSize) {
        properties.putMemorySize(
                CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE,
                MemorySize.parse(maxSize, MemorySize.MemoryUnit.BYTES));
        return this;
    }

    /**
     * Set threshold when to flush buffered request based on the number of rows currently added.
     * Defaults to not set, i.e. won't flush based on the number of buffered rows. Optional.
     *
     * @param writeBufferFlushMaxRows number of added rows when begin the request flushing.
     */
    public HBase writeBufferFlushMaxRows(int writeBufferFlushMaxRows) {
        properties.putInt(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS, writeBufferFlushMaxRows);
        return this;
    }

    /**
     * Set an interval when to flushing buffered requesting if the interval passes, in milliseconds.
     * Defaults to not set, i.e. won't flush based on flush interval. Optional.
     *
     * @param interval flush interval. The string should be in format "{length value}{time unit
     *     label}" E.g, "123ms", "1 s", If no time unit label is specified, it will be considered as
     *     milliseconds. For more details about the format, please see {@link
     *     TimeUtils#parseDuration(String)}}.
     */
    public HBase writeBufferFlushInterval(String interval) {
        properties.putString(CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL, interval);
        return this;
    }

    /**
     * Sets the configuration properties for HBase Configuration. Resets previously set properties.
     *
     * @param properties The configuration properties for HBase Configuration.
     */
    public HBase properties(Properties properties) {
        Preconditions.checkNotNull(properties);
        if (this.hbaseProperties == null) {
            this.hbaseProperties = new HashMap<>();
        }
        this.hbaseProperties.clear();
        properties.forEach((k, v) -> this.hbaseProperties.put((String) k, (String) v));
        return this;
    }

    /**
     * Adds a configuration property for HBase Configuration.
     *
     * @param key property key for the HBase Configuration
     * @param value property value for the HBase Configuration
     */
    public HBase property(String key, String value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        if (this.hbaseProperties == null) {
            this.hbaseProperties = new HashMap<>();
        }
        hbaseProperties.put(key, value);
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        if (this.hbaseProperties != null) {
            this.hbaseProperties.forEach(
                    (key, value) -> {
                        if (!properties.containsKey(CONNECTOR_PROPERTIES + '.' + key)) {
                            properties.putString(CONNECTOR_PROPERTIES + '.' + key, value);
                        }
                    });
        }
        return properties.asMap();
    }
}
