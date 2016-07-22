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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

public class RedisSinkTest extends TestLogger {

	@Test(expected=NullPointerException.class)
	public void shouldThrowNullPointExceptionIfDataMapperIsNull(){
		new RedisSink<>(new FlinkJedisClusterConfig.Builder().build(), null);
	}

	@Test(expected = NullPointerException.class)
	public void shouldThrowNullPointerExceptionIfCommandDescriptionIsNull(){
		new RedisSink<>(new FlinkJedisClusterConfig.Builder().build(), new TestMapper(null));
	}

	@Test(expected = NullPointerException.class)
	public void shouldThrowNullPointerExceptionIfConfigurationIsNull(){
		new RedisSink<>(null, new TestMapper(new RedisCommandDescription(RedisCommand.LPUSH)));
	}

	@Test
	public void testRedisDownBehavior() throws Exception {

		// create a wrong configuration so that open() fails.

		FlinkJedisPoolConfig wrongJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
			.setHost("127.0.0.1")
			.setPort(1234).build();

		testDownBehavior(wrongJedisPoolConfig);
	}

	@Test
	public void testRedisClusterDownBehavior() throws Exception {

		Set<InetSocketAddress> hosts = new HashSet<>();
		hosts.add(new InetSocketAddress("127.0.0.1", 1234));

		// create a wrong configuration so that open() fails.

		FlinkJedisClusterConfig wrongJedisClusterConfig = new FlinkJedisClusterConfig.Builder()
			.setNodes(hosts)
			.setTimeout(100)
			.setMaxIdle(1)
			.setMaxTotal(1)
			.setMinIdle(1).build();

		testDownBehavior(wrongJedisClusterConfig);
	}

	@Test
	public void testRedisSentinelDownBehavior() throws Exception {

		Set<String> hosts = new HashSet<>();
		hosts.add("localhost:55095");

		// create a wrong configuration so that open() fails.

		FlinkJedisSentinelConfig wrongJedisSentinelConfig = new FlinkJedisSentinelConfig.Builder()
			.setMasterName("master")
			.setSentinels(hosts)
			.build();

		testDownBehavior(wrongJedisSentinelConfig);
	}

	private void testDownBehavior(FlinkJedisConfigBase config) throws Exception {
		RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(config,
			new RedisSinkITCase.RedisCommandMapper(RedisCommand.SADD));

		try {
			redisSink.open(new Configuration());
		} catch (Exception e) {

			// search for nested JedisConnectionExceptions
			// because this is the expected behavior

			Throwable t = e;
			int depth = 0;
			while (!(t instanceof JedisConnectionException)) {
				t = t.getCause();
				if (t == null || depth++ == 20) {
					throw e;
				}
			}
		}
	}

	private class TestMapper implements RedisMapper<Tuple2<String, String>>{
		private RedisCommandDescription redisCommandDescription;

		public TestMapper(RedisCommandDescription redisCommandDescription){
			this.redisCommandDescription = redisCommandDescription;
		}
		@Override
		public RedisCommandDescription getCommandDescription() {
			return redisCommandDescription;
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
