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

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.embedded.RedisCluster;
import redis.embedded.util.JedisUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.apache.flink.util.NetUtils.getAvailablePort;

public class RedisSentinelClusterTest extends TestLogger {

	private static RedisCluster cluster;
	private static final String REDIS_MASTER = "master";
	private static final String TEST_KEY = "testKey";
	private static final String TEST_VALUE = "testValue";
	private static final List<Integer> sentinels = Arrays.asList(getAvailablePort(), getAvailablePort());
	private static final List<Integer> group1 = Arrays.asList(getAvailablePort(), getAvailablePort());

	private JedisSentinelPool jedisSentinelPool;
	private FlinkJedisSentinelConfig jedisSentinelConfig;

	@BeforeClass
	public static void setUpCluster(){
		cluster = RedisCluster.builder().sentinelPorts(sentinels).quorumSize(1)
			.serverPorts(group1).replicationGroup(REDIS_MASTER, 1)
			.build();
		cluster.start();
	}

	@Before
	public void setUp() {
		Set<String> hosts = JedisUtil.sentinelHosts(cluster);
		jedisSentinelConfig = new FlinkJedisSentinelConfig.Builder().setMasterName(REDIS_MASTER)
			.setSentinels(hosts).build();
		jedisSentinelPool = new JedisSentinelPool(jedisSentinelConfig.getMasterName(),
			jedisSentinelConfig.getSentinels());
	}

	@Test
	public void testRedisSentinelOperation() {
		RedisCommandsContainer redisContainer = RedisCommandsContainerBuilder.build(jedisSentinelConfig);
		Jedis jedis = null;
		try{
			jedis = jedisSentinelPool.getResource();
			redisContainer.set(TEST_KEY, TEST_VALUE);
			assertEquals(TEST_VALUE, jedis.get(TEST_KEY));
		}finally {
			if (jedis != null){
				jedis.close();
			}
		}
	}

	@After
	public void tearDown() throws IOException {
		if (jedisSentinelPool != null) {
			jedisSentinelPool.close();
		}
	}

	@AfterClass
	public static void tearDownCluster() throws IOException {
		if (!cluster.isActive()) {
			cluster.stop();
		}
	}
}
