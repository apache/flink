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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import redis.clients.jedis.JedisPubSub;

import static org.junit.Assert.assertEquals;

public class RedisSinkPublishITCase extends RedisITCaseBase {

	private static final int NUM_ELEMENTS = 20;
	private static final String REDIS_CHANNEL = "CHANNEL";

	private static final List<String> sourceList = new ArrayList<>();
	private Thread sinkThread;
	private PubSub pubSub;

	@Before
	public void before() throws Exception {
		pubSub = new PubSub();
		sinkThread = new Thread(new Subscribe(pubSub));
	}

	@Test
	public void redisSinkTest() throws Exception {
		sinkThread.start();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
			.setHost(REDIS_HOST)
			.setPort(REDIS_PORT).build();
		DataStreamSource<Tuple2<String, String>> source = env.addSource(new TestSourceFunction());

		RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(jedisPoolConfig, new RedisTestMapper());

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

	private class Subscribe implements Runnable {
		private PubSub localPubSub;
		private Subscribe(PubSub pubSub){
			this.localPubSub = pubSub;
		}

		@Override
		public void run() {
			JedisPool pool = new JedisPool(REDIS_HOST, REDIS_PORT);
			pool.getResource().subscribe(localPubSub, REDIS_CHANNEL);
		}
	}

	private static class TestSourceFunction implements SourceFunction<Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
			for (int i = 0; i < NUM_ELEMENTS && running; i++) {
				ctx.collect(new Tuple2<>(REDIS_CHANNEL, "message #" + i));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static class PubSub extends JedisPubSub {

		@Override
		public void onMessage(String channel, String message) {
			sourceList.add(message);
		}

	}

	private static class RedisTestMapper implements RedisMapper<Tuple2<String, String>>{

		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.PUBLISH);
		}

		@Override
		public String getKeyFromData(Tuple2<String, String> data) {
			return data.f0;
		}

		@Override
		public String getValueFromData(Tuple2<String, String> data) {
			return data.f1;
		}
	}
}
