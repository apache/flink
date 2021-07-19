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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class to create an {@link HBaseSink}. The following example shows how to create an
 * HBaseSink that writes Long values to HBase.
 *
 * <pre>{@code
 * HBaseSink<Long> hbaseSink = HBaseSink.builder()
 *          .setTableName(tableName)
 *          .setSinkSerializer(new HBaseLongSerializer())
 *          .setHBaseConfiguration(hbaseConfig)
 *          .build();
 * }</pre>
 *
 * <p>Here is an example for the Serializer:
 *
 * <pre>{@code
 * static class HBaseLongSerializer implements HBaseSinkSerializer<Long> {
 *     @Override
 *     public HBaseEvent serialize(Long event) {
 *         return HBaseEvent.putWith(                  // or deleteWith()
 *                 event.toString(),                   // rowId
 *                 "exampleColumnFamily",              // column family
 *                 "exampleQualifier",                 // qualifier
 *                 Bytes.toBytes(event.toString()));   // payload
 *     }
 * }
 * }</pre>
 *
 * <p>A SerializationSchema is always required, as well as a table name to write to and an
 * HBaseConfiguration.
 *
 * <p>By default each HBaseWriter has a queue limit of 1000 entries used for batching. This can be
 * changed with {@link #setQueueLimit(int)}. The maximum allowed latency of an event can be set by
 * {@link #setMaxLatencyMs(int)}. After the specified elements will be sent to HBase no matter how
 * many elements are currently in the batching queue.
 */
public class HBaseSinkBuilder<IN> {

    private static final ConfigOption<?>[] REQUIRED_CONFIGS = {HBaseSinkOptions.TABLE_NAME};
    private final Configuration sinkConfiguration;
    private org.apache.hadoop.conf.Configuration hbaseConfiguration;
    private HBaseSinkSerializer<IN> sinkSerializer;

    protected HBaseSinkBuilder() {
        this.sinkSerializer = null;
        this.hbaseConfiguration = null;
        this.sinkConfiguration = new Configuration();
    }

    public HBaseSinkBuilder<IN> setTableName(String tableName) {
        sinkConfiguration.setString(HBaseSinkOptions.TABLE_NAME, tableName);
        return this;
    }

    public <T extends IN> HBaseSinkBuilder<T> setSinkSerializer(
            HBaseSinkSerializer<T> sinkSerializer) {
        final HBaseSinkBuilder<T> sinkBuilder = (HBaseSinkBuilder<T>) this;
        sinkBuilder.sinkSerializer = sinkSerializer;
        return sinkBuilder;
    }

    public HBaseSinkBuilder<IN> setHBaseConfiguration(
            org.apache.hadoop.conf.Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
        return this;
    }

    public HBaseSinkBuilder<IN> setQueueLimit(int queueLimit) {
        sinkConfiguration.setInteger(HBaseSinkOptions.QUEUE_LIMIT, queueLimit);
        return this;
    }

    public HBaseSinkBuilder<IN> setMaxLatencyMs(int maxLatencyMs) {
        sinkConfiguration.setInteger(HBaseSinkOptions.MAX_LATENCY, maxLatencyMs);
        return this;
    }

    public HBaseSink<IN> build() {
        sanityCheck();
        return new HBaseSink<>(sinkSerializer, hbaseConfiguration, sinkConfiguration);
    }

    private void sanityCheck() {
        for (ConfigOption<?> requiredConfig : REQUIRED_CONFIGS) {
            checkNotNull(
                    sinkConfiguration.get(requiredConfig),
                    String.format("Config option %s is required but not provided", requiredConfig));
        }

        checkNotNull(sinkSerializer, "No sink serializer was specified.");
        checkNotNull(hbaseConfiguration, "No hbase configuration was specified.");
    }
}
