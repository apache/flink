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

package org.apache.flink.streaming.connectors.redis.util;

import org.apache.flink.streaming.connectors.redis.config.StartupMode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

/** */
public final class ConsumerGroupUtils {

    public static void createConsumerGroup(
            Jedis jedis,
            String streamKey,
            String groupName,
            StreamEntryID streamEntryID,
            boolean createStreamIfAbsent) {
        jedis.xgroupCreate(streamKey, groupName, streamEntryID, createStreamIfAbsent);
    }
    //
    //    public static void createConsumerGroup(Jedis jedis, String streamKey, String groupName,
    // long timestamp, boolean createStreamIfAbsent) {
    //        createConsumerGroup(jedis, streamKey, groupName, new StreamEntryID(timestamp, 0L),
    // createStreamIfAbsent);
    //    }

    public static void createConsumerGroup(
            Jedis jedis,
            String streamKey,
            String groupName,
            StartupMode startupMode,
            boolean createStreamIfAbsent) {
        final StreamEntryID streamEntryID;
        switch (startupMode) {
            case EARLIEST:
                streamEntryID = new StreamEntryID();
                break;
            case LATEST:
                streamEntryID = StreamEntryID.LAST_ENTRY;
                break;
            case SPECIFIC_OFFSETS:
                throw new RuntimeException("Use the method with StreamEntryID as param");
            case TIMESTAMP:
                throw new RuntimeException("Use the method with 'long timestamp' param");
            default:
                throw new RuntimeException(
                        "Consumer Group cannot be initialized from " + startupMode);
        }
        createConsumerGroup(jedis, streamKey, groupName, streamEntryID, createStreamIfAbsent);
    }
}
