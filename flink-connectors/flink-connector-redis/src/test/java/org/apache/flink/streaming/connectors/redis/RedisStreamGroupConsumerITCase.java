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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.config.StartupMode;
import org.apache.flink.streaming.connectors.redis.internal.SchemalessDataRowToMap;
import org.apache.flink.streaming.connectors.redis.util.ConsumerGroupUtils;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/** */
public class RedisStreamGroupConsumerITCase extends RedisITCaseBase {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RedisStreamGroupConsumerITCase.class);

    private static final int NUM_THREADS = 3;
    private static final int NUM_ELEMENTS = 20;
    private static final int NUM_FIELDS = 3;

    private static final String REDIS_KEY = "TEST_KEY";
    private static final String GROUP_NAME = "FlinkGroup";

    StreamExecutionEnvironment env;
    private static final AtomicInteger[] count = new AtomicInteger[NUM_THREADS];

    @Before
    @Override
    public void setUp() {
        super.setUp();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        for (int t = 0; t < NUM_THREADS; t++) {
            count[t] = new AtomicInteger();
        }
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void redisGroupConsumer() throws Exception {
        ConsumerGroupUtils.createConsumerGroup(
                jedis, REDIS_KEY, GROUP_NAME, StartupMode.EARLIEST, true);
        populate(jedis);

        for (int t = 0; t < NUM_THREADS; t++) {
            DataStreamSource<Row> source =
                    env.addSource(
                            new RedisStreamGroupConsumer<>(
                                    GROUP_NAME,
                                    "consumer" + t,
                                    new SchemalessDataRowToMap(),
                                    REDIS_KEY,
                                    getDefaultConfigProperties()));
            source.setParallelism(1);

            final int index = t;
            source.addSink(
                    new SinkFunction<>() {
                        @Override
                        public void invoke(Row value, Context context) throws Exception {
                            count[index].incrementAndGet();
                        }
                    });
        }

        env.execute("Test Redis Consumer Group");

        int total = 0;
        for (int t = 0; t < NUM_THREADS; t++) {
            if (count[t].get() == 0) {
                LOGGER.warn("consumer{} could not read any entry.", t);
            }
            total += count[t].get();
        }
        assertEquals(NUM_ELEMENTS, total);
    }

    private static void populate(Jedis iJedis) {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            HashMap<String, String> map = new HashMap<>();
            for (int j = 0; j < NUM_FIELDS; j++) {
                map.put("f" + j, "" + j);
            }
            iJedis.xadd(REDIS_KEY, StreamEntryID.NEW_ENTRY, map);
        }
    }
}
