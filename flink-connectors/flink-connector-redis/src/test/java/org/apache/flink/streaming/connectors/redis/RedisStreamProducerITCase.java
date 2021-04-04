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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.internal.SchemalessDataRowToMap;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class RedisStreamProducerITCase extends RedisITCaseBase {

    private static final int NUM_ELEMENTS = 4;
    private static final int NUM_FIELDS = 3;
    private static final String REDIS_KEY = "TEST_KEY";

    private StreamExecutionEnvironment env;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void redisProducer() throws Exception {
        DataStreamSource<Row> source = env.addSource(new TestRowSourceFunction());

        SinkFunction<Row> producer = new FlinkRedisStreamProducer<>(new SchemalessDataRowToMap(),
                REDIS_KEY, getDefaultConfigProperties());
        source.addSink(producer);

        env.execute("Test Redis Stream Producer");

        assertEquals(NUM_ELEMENTS, jedis.xlen(REDIS_KEY).intValue());
    }

    private static class TestRowSourceFunction implements SourceFunction<Row> {

        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {

            for (int i = 0; i < NUM_ELEMENTS && running; i++) {
                ArrayList<Object> list = new ArrayList<>(NUM_FIELDS);
                for (int j = 0; j < NUM_FIELDS; j++) {
                    list.add("f" + j);
                    list.add("" + j);
                }
                ctx.collect(Row.of(list.toArray()));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
