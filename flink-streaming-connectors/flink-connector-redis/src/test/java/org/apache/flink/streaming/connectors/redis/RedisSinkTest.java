/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisPool;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RedisSinkTest extends StreamingMultipleProgramsTestBase {

	private static final int NUM_ELEMENTS = 20;
	private static final int REDIS_PORT = 6379;
	private static final String REDIS_HOST = "127.0.0.1";
	private static final String REDIS_CHANNEL = "CHANNEL";

	private static RedisServer redisServer;
	private static final List<String> sourceList = new ArrayList<>();
	private Thread sinkThread;
	private PubSub pubSub;

    @BeforeClass
    public static void createRedisServer() throws IOException, InterruptedException {
        redisServer = new RedisServer(REDIS_PORT);
        redisServer.start();
    }

    @Before
    public void before() throws Exception {
        pubSub = new PubSub();
        sinkThread = new Thread(new Subscribe(pubSub));
    }

    @Test
    public void redisSinkTest() throws Exception {
        sinkThread.start();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.addSource(new TestSourceFunction());
        RedisSink<String> redisSink = new RedisSink<>(REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, new SimpleStringSchema());
        source.addSink(redisSink);

        env.execute("Redis Sink Test");

        assertEquals(NUM_ELEMENTS, sourceList.size());
    }

    @After
    public void after() throws Exception {
        pubSub.unsubscribe();
        sinkThread.join();
        sourceList.clear();
    }

    @AfterClass
    public static void stopRedisServer(){
        redisServer.stop();
    }

    private class Subscribe implements Runnable {
        private PubSub localPubSub;
        private Subscribe(PubSub pubSub){
            this.localPubSub = pubSub;
        }

        @Override
        public void run() {
            JedisPool pool = new JedisPool(REDIS_HOST, REDIS_PORT);
            pool.getResource().subscribe(localPubSub, REDIS_CHANNEL.getBytes());
        }
    }

    private static class TestSourceFunction implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < NUM_ELEMENTS && running; i++) {
                ctx.collect("message #" + i);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class PubSub extends BinaryJedisPubSub{

        @Override
        public void onMessage(byte[] channel, byte[] message) {
            String msg = new SimpleStringSchema().deserialize(message);
            sourceList.add(msg);
        }

    }
}
