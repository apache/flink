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
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/** */
public class RedisStreamConsumerITCase extends RedisITCaseBase {

    private static final int NUM_ELEMENTS = 4;
    private static final int NUM_FIELDS = 3;
    private static final String REDIS_KEY = "TEST_KEY";

    private StreamExecutionEnvironment env;

    private static final AtomicInteger count = new AtomicInteger();

    @BeforeClass
    public static void prepare() {
        start();
    }

    @AfterClass
    public static void cleanUp() {
        stop();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        count.set(0);
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void redisConsumer() throws Exception {
        populate(jedis);

        DataStreamSource<Row> source =
                env.addSource(
                        new RedisStreamConsumer<>(
                                getConfigProperties(),
                                StartupMode.EARLIEST,
                                new SchemalessDataRowToMap(),
                                REDIS_KEY));
        source.setParallelism(1);

        SinkFunction<Row> sink =
                new SinkFunction<Row>() {
                    @Override
                    public void invoke(Row value, Context context) throws Exception {
                        count.incrementAndGet();
                    }
                };
        source.addSink(sink);

        env.execute("Test Redis Row Consumer");

        assertEquals(NUM_ELEMENTS, count.get());
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
