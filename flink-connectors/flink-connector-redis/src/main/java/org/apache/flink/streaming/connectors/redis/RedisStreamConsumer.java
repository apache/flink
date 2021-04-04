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

package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.streaming.connectors.redis.config.StartupMode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

/**
 *
 * @param <T>
 */
public class RedisStreamConsumer<T> extends AbstractRedisStreamConsumer<T> {

    private final DataConverter<T> dataConverter;

    public RedisStreamConsumer(Properties configProps, StartupMode startupMode,
                               DataConverter<T> dataConverter, String... streamKeys) {
        super(startupMode, streamKeys, configProps);
        this.dataConverter = dataConverter;
    }

    public RedisStreamConsumer(DataConverter<T> dataConverter, String[] streamKeys, Long[] timestamps, Properties configProps) {
        super(streamKeys, timestamps, configProps);
        this.dataConverter = dataConverter;
    }

    public RedisStreamConsumer(DataConverter<T> dataConverter, String[] streamKeys, StreamEntryID[] streamIds, Properties configProps) {
        super(streamKeys, streamIds, configProps);
        this.dataConverter = dataConverter;
    }

    @Override
    protected List<Entry<String, List<StreamEntry>>> read(Jedis jedis) {
        return jedis.xread(1, 0L, streamEntryIds);
    }

    @Override
    protected void collect(SourceContext<T> sourceContext, String streamKey, StreamEntry streamEntry) {
        sourceContext.collect(dataConverter.toData(streamEntry.getFields()));
    }
}
