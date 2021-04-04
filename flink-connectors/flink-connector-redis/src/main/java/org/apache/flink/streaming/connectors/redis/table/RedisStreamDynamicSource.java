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

import org.apache.flink.streaming.connectors.redis.AbstractRedisStreamConsumer;
import org.apache.flink.streaming.connectors.redis.DataConverter;
import org.apache.flink.streaming.connectors.redis.RedisStreamConsumer;
import org.apache.flink.streaming.connectors.redis.RedisStreamGroupConsumer;
import org.apache.flink.streaming.connectors.redis.config.StartupMode;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import redis.clients.jedis.StreamEntryID;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 *
 */
public class RedisStreamDynamicSource implements ScanTableSource {

    private static final ChangelogMode CHANGELOG_MODE = ChangelogMode.insertOnly();

    private final Properties config;

    private final Optional<String> groupName;

    private final Optional<String> consumerName;

    /**
     * The Redis key to consume.
     */
    private final String streamKey;

    /**
     * The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}).
     */
    private final StartupMode startupMode;

    /**
     * Specific startup offset; only relevant when startup mode is
     * {@link StartupMode#SPECIFIC_OFFSETS}.
     */
    private StreamEntryID streamEntryId;

    /**
     * Specific startup timestamp; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.
     */
    private Long timestamp;

    private final DataConverter<RowData> converter;

    private RedisStreamDynamicSource(
            Properties config,
            Optional<String> groupName,
            Optional<String> consumerName,
            String streamKey,
            StartupMode startupMode,
            DataConverter<RowData> rowMapConverter) {
        this.config = config;
        this.streamKey = Preconditions.checkNotNull(streamKey, "Key must not be null.");
        this.groupName = groupName;
        this.consumerName = consumerName;
        this.converter = Preconditions.checkNotNull(rowMapConverter, "RowMapConverter must not be null.");
        this.startupMode = Preconditions.checkNotNull(startupMode, "StartupMode must not be null.");
    }

    public RedisStreamDynamicSource(
            Properties config,
            String streamKey,
            StreamEntryID streamEntryId,
            DataConverter<RowData> rowMapConverter) {
        this(config, Optional.empty(), Optional.empty(), streamKey, StartupMode.SPECIFIC_OFFSETS, rowMapConverter);
        this.streamEntryId = streamEntryId;
    }

    public RedisStreamDynamicSource(
            Properties config,
            String streamKey,
            Long timestamp,
            DataConverter<RowData> rowMapConverter) {
        this(config, Optional.empty(), Optional.empty(), streamKey, StartupMode.TIMESTAMP, rowMapConverter);
        this.timestamp = timestamp;
    }

    public RedisStreamDynamicSource(
            Properties config,
            String streamKey,
            StartupMode startupMode,
            DataConverter<RowData> rowMapConverter) {
        this(config, Optional.empty(), Optional.empty(), streamKey, startupMode, rowMapConverter);
    }

    public RedisStreamDynamicSource(
            Properties config,
            String groupName,
            String consumerName,
            String streamKey,
            StreamEntryID streamEntryId,
            DataConverter<RowData> rowMapConverter) {
        this(config, Optional.of(groupName), Optional.of(consumerName), streamKey, StartupMode.SPECIFIC_OFFSETS, rowMapConverter);
        this.streamEntryId = streamEntryId;
    }

    public RedisStreamDynamicSource(
            Properties config,
            String groupName,
            String consumerName,
            String streamKey,
            Long timestamp,
            DataConverter<RowData> rowMapConverter) {
        this(config, Optional.of(groupName), Optional.of(consumerName), streamKey, StartupMode.TIMESTAMP, rowMapConverter);
        this.timestamp = timestamp;
    }

    public RedisStreamDynamicSource(
            Properties config,
            String groupName,
            String consumerName,
            String streamKey,
            StartupMode startupMode,
            DataConverter<RowData> rowMapConverter) {
        this(config, Optional.of(groupName), Optional.of(consumerName), streamKey, startupMode, rowMapConverter);
    }

    public RedisStreamDynamicSource(
            Properties config,
            String groupName,
            String consumerName,
            String streamKey,
            DataConverter<RowData> rowMapConverter) {
        this(config, Optional.of(groupName), Optional.of(consumerName), streamKey, StartupMode.GROUP_OFFSETS, rowMapConverter);
    }

    @Override
    public DynamicTableSource copy() {
        if (groupName.isPresent()) {
            switch (startupMode) {
                case GROUP_OFFSETS:
                    return new RedisStreamDynamicSource(config, groupName.get(), consumerName.get(),
                            streamKey, converter);
                case TIMESTAMP:
                    return new RedisStreamDynamicSource(config, groupName.get(), consumerName.get(),
                            streamKey, timestamp, converter);
                case SPECIFIC_OFFSETS:
                    return new RedisStreamDynamicSource(config, groupName.get(), consumerName.get(),
                            streamKey, streamEntryId, converter);
                default:
                    return new RedisStreamDynamicSource(config, groupName.get(), consumerName.get(),
                            streamKey, startupMode, converter);
            }
        } else {
            switch (startupMode) {
                case TIMESTAMP:
                    return new RedisStreamDynamicSource(config, streamKey, timestamp, converter);
                case SPECIFIC_OFFSETS:
                    return new RedisStreamDynamicSource(config, streamKey, streamEntryId, converter);
                default:
                    return new RedisStreamDynamicSource(config, streamKey, startupMode, converter);
            }
        }
    }

    @Override
    public String asSummaryString() {
        return "RedisStream";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RedisStreamDynamicSource that = (RedisStreamDynamicSource) o;
        return Objects.equals(config, that.config)
                && Objects.equals(groupName, that.groupName)
                && Objects.equals(consumerName, that.consumerName)
                && Objects.equals(streamKey, that.streamKey)
                && startupMode == that.startupMode
                && Objects.equals(streamEntryId, that.streamEntryId)
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(converter, that.converter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                config,
                groupName,
                consumerName,
                streamKey,
                startupMode,
                streamEntryId,
                timestamp,
                converter);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return CHANGELOG_MODE;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        AbstractRedisStreamConsumer<RowData> redisStreamConsumer = createRedisConsumer();

        return SourceFunctionProvider.of(redisStreamConsumer, false);
    }

    protected AbstractRedisStreamConsumer<RowData> createRedisConsumer() {
        if (groupName.isPresent()) {
            switch (startupMode) {
                case GROUP_OFFSETS:
                    return new RedisStreamGroupConsumer<>(groupName.get(), consumerName.get(),
                            converter, streamKey, config);
                case TIMESTAMP:
                    return new RedisStreamGroupConsumer<>(groupName.get(), consumerName.get(),
                            converter, new String[]{streamKey}, new Long[]{timestamp}, config);
                case SPECIFIC_OFFSETS:
                    return new RedisStreamGroupConsumer<>(groupName.get(), consumerName.get(),
                            converter, new String[]{streamKey}, new StreamEntryID[]{streamEntryId}, config);
                default:
                    return new RedisStreamGroupConsumer<>(groupName.get(), consumerName.get(),
                            startupMode, converter, new String[]{streamKey}, config);
            }
        } else {
            switch (startupMode) {
                case TIMESTAMP:
                    return new RedisStreamConsumer<>(converter,
                            new String[]{streamKey}, new Long[]{timestamp}, config);
                case SPECIFIC_OFFSETS:
                    return new RedisStreamConsumer<>(converter,
                            new String[]{streamKey}, new StreamEntryID[]{streamEntryId}, config);
                default:
                    return new RedisStreamConsumer<>(config, startupMode, converter, streamKey);
            }
        }

    }
}
