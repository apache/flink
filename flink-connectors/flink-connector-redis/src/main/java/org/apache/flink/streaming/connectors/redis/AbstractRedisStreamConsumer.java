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

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 *
 * @param <T>
 */
public abstract class AbstractRedisStreamConsumer<T> extends RedisConsumerBase<T> {

    protected final Entry<String, StreamEntryID>[] streamEntryIds;
    private final Map<String, Integer> keyIndex = new HashMap<>();

    public AbstractRedisStreamConsumer(StartupMode startupMode, String[] streamKeys, Properties configProps) {
        super(Arrays.asList(streamKeys), configProps);
        final StreamEntryID streamEntryID;
        switch (startupMode) {
            case EARLIEST:
                streamEntryID = new StreamEntryID();
                break;
            case LATEST:
                streamEntryID = StreamEntryID.LAST_ENTRY;
                break;
            case GROUP_OFFSETS:
                streamEntryID = StreamEntryID.UNRECEIVED_ENTRY;
                break;
            case SPECIFIC_OFFSETS:
                throw new RuntimeException("Use the constructor with 'StreamEntryID[] streamIds' as param");
            case TIMESTAMP:
                throw new RuntimeException("Use the constructor with 'Long[] timestamps' param");
            default:
                throw new IllegalStateException();
        }
        this.streamEntryIds = prepareStreamEntryIds(streamKeys, streamEntryID);
        initializeKeyIndex();
    }

    public AbstractRedisStreamConsumer(String[] streamKeys, Long[] timestamps, Properties configProps) {
        this(streamKeys, streamEntryIds(timestamps), configProps);
    }

    public AbstractRedisStreamConsumer(String[] streamKeys, StreamEntryID[] streamIds, Properties configProps) {
        this(prepareStreamEntryIds(streamKeys, streamIds), configProps);
    }

    private AbstractRedisStreamConsumer(Entry<String, StreamEntryID>[] streamIds, Properties configProps) {
        super(null, configProps);
        this.streamEntryIds = streamIds;
        initializeKeyIndex();
    }

    @Override
    protected final boolean readAndCollect(Jedis jedis, List<String> streamKeys, SourceContext<T> sourceContext) {
        boolean anyEntry = false;
        List<Entry<String, List<StreamEntry>>> response = read(jedis);
        if (response != null) {
            for (Entry<String, List<StreamEntry>> streamEntries : response) {
                String streamKey = streamEntries.getKey();
                for (StreamEntry entry : streamEntries.getValue()) {
                    anyEntry = true;
                    collect(sourceContext, streamKey, entry);
                    updateIdForKey(streamKey, entry.getID());
                }
            }
        }
        return anyEntry;
    }

    protected abstract List<Entry<String, List<StreamEntry>>> read(Jedis jedis);

    protected abstract void collect(SourceContext<T> sourceContext, String streamKey, StreamEntry streamEntry);

    protected void updateIdForKey(String streamKey, StreamEntryID streamEntryID) {
        int index = keyIndex.get(streamKey);
        if (this.streamEntryIds[index].getValue().toString().equals(">")) {
            // skip
        } else {
            this.streamEntryIds[index].setValue(streamEntryID);
        }
    }

    private void initializeKeyIndex() {
        int index = 0;
        for (Entry<String, StreamEntryID> streamEntryId : streamEntryIds) {
            keyIndex.put(streamEntryId.getKey(), index++);
        }
    }

    private static Entry<String, StreamEntryID>[] prepareStreamEntryIds(String[] streamKeys, StreamEntryID streamId) {
        Entry<?, ?>[] streams = new Entry<?, ?>[streamKeys.length];
        for (int i = 0; i < streamKeys.length; i++) {
            streams[i] = new SimpleEntry<>(streamKeys[i], streamId);
        }
        return (Entry<String, StreamEntryID>[]) streams;
    }

    private static Entry<String, StreamEntryID>[] prepareStreamEntryIds(String[] streamKeys, StreamEntryID[] streamIds) {
        Entry<?, ?>[] streams = new Entry<?, ?>[streamKeys.length];
        for (int i = 0; i < streamKeys.length; i++) {
            streams[i] = new SimpleEntry<>(streamKeys[i], streamIds[i]);
        }
        return (Entry<String, StreamEntryID>[]) streams;
    }

    private static StreamEntryID[] streamEntryIds(Long[] timestamps) {
        StreamEntryID[] entryIds = new StreamEntryID[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            entryIds[i] = new StreamEntryID(timestamps[i], 0L);
        }
        return entryIds;
    }
}
