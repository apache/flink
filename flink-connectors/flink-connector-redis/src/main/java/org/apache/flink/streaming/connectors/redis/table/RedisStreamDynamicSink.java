/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.redis.FlinkRedisStreamProducer;
import org.apache.flink.streaming.connectors.redis.MapConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** Redis-Stream-backed {@link DynamicTableSink}. */
@Internal
public class RedisStreamDynamicSink implements DynamicTableSink, SupportsPartitioning {

    private static final ChangelogMode CHANGELOG_MODE = ChangelogMode.insertOnly();

    /** The Redis Stream to write to. */
    private final String streamKey;

    /** Properties for the Redis (Stream) producer. */
    private final Properties producerProperties;

    private final MapConverter<RowData> converter;

    public RedisStreamDynamicSink(String streamKey, Properties producerProperties, MapConverter<RowData> converter) {
        this.streamKey = Preconditions.checkNotNull(streamKey, "Redis Stream key name must not be null");
        this.producerProperties = Preconditions.checkNotNull(producerProperties,
                "Properties for the Flink Redis producer must not be null");
        this.converter = Preconditions.checkNotNull(converter, "Converter must not be null");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return CHANGELOG_MODE;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        FlinkRedisStreamProducer<RowData> redisStreamProducer =
                new FlinkRedisStreamProducer<>(converter, streamKey, producerProperties);

        return SinkFunctionProvider.of(redisStreamProducer);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisStreamDynamicSink(streamKey, producerProperties, converter);
    }

    @Override
    public String asSummaryString() {
        return "RedisStream";
    }

    // --------------------------------------------------------------------------------------------
    // SupportsPartitioning
    // --------------------------------------------------------------------------------------------

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // nothing to do
    }

    // --------------------------------------------------------------------------------------------
    // Value semantics for equals and hashCode
    // --------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisStreamDynamicSink that = (RedisStreamDynamicSink) o;
        return Objects.equals(streamKey, that.streamKey)
                && Objects.equals(producerProperties, that.producerProperties)
                && Objects.equals(converter, that.converter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamKey, producerProperties, converter);
    }
}
