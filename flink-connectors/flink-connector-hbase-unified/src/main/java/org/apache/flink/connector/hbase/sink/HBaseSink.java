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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.sink.writer.HBaseWriter;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * A Sink Connector for HBase. Please use an {@link HBaseSinkBuilder} to construct a {@link
 * HBaseSink}. As HBase does not support transactions, this sink does not guarantee exactly-once,
 * but at-least-once.
 *
 * <p>The following example shows how to create an HBaseSink that writes Long values to HBase.
 *
 * <pre>{@code
 * HBaseSink<Long> hbaseSink =
 *      HBaseSink.builder()
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
 * @see HBaseSinkBuilder HBaseSinkBuilder for more details on creation
 * @see HBaseSinkSerializer HBaseSinkSerializer for more details on serialization
 */
public class HBaseSink<IN> implements Sink<IN, Void, Mutation, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);

    private final HBaseSinkSerializer<IN> sinkSerializer;
    private final byte[] serializedHBaseConfig;
    private final Configuration sinkConfiguration;

    HBaseSink(
            HBaseSinkSerializer<IN> sinkSerializer,
            org.apache.hadoop.conf.Configuration hbaseConfiguration,
            Configuration sinkConfiguration) {
        this.sinkSerializer = sinkSerializer;
        this.serializedHBaseConfig =
                HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration);
        this.sinkConfiguration = sinkConfiguration;
        LOG.debug("constructed sink");
    }

    public static <IN> HBaseSinkBuilder<IN> builder() {
        return new HBaseSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN, Void, Mutation> createWriter(InitContext context, List<Mutation> states) {
        return new HBaseWriter<>(
                context, states, sinkSerializer, serializedHBaseConfig, sinkConfiguration);
    }

    @Override
    public Optional<Committer<Void>> createCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Mutation>> getWriterStateSerializer() {
        return Optional.of(new HBaseSinkMutationSerializer());
    }
}
