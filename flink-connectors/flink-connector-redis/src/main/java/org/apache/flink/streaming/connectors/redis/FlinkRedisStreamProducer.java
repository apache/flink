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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

import java.util.Map;
import java.util.Properties;

/**
 *
 * @param <OUT>
 */
public class FlinkRedisStreamProducer<OUT> extends FlinkRedisProducerBase<OUT> {

    private final MapConverter<OUT> mapConverter;

    public FlinkRedisStreamProducer(MapConverter<OUT> mapConverter, String streamKey, Properties configProps) {
        super(streamKey, configProps);
        this.mapConverter = mapConverter;
    }

    @Override
    public void invoke(Jedis jedis, String streamKey, OUT value, Context context) throws Exception {
        // code
        Map<String, String> map = mapConverter.toMap(value);
        jedis.xadd(streamKey, StreamEntryID.NEW_ENTRY, map);
    }

}
