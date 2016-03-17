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
package org.apache.flink.streaming.connectors.redis.common.container;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.JedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.JedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.JedisSentinelConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

public class RedisCommandsContainerBuilder {

	/**
	 * Builds container for single Redis environment.
	 * @param jedisPoolConfig configuration for JedisPool
	 * @return container for single Redis environment
	 */
	public static RedisCommandsContainer build(JedisPoolConfig jedisPoolConfig) {
		GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
		genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
		genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
		genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());

		JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, jedisPoolConfig.getHost(),
			jedisPoolConfig.getPort(), jedisPoolConfig.getTimeout(), jedisPoolConfig.getPassword(),
			jedisPoolConfig.getDatabase());
		return new RedisContainer(jedisPool);
	}

	/**
	 * Builds container for Redis Cluster environment.
	 * @param config configuration for JedisCluster
	 * @return container for Redis Cluster environment
	 */
	public static RedisCommandsContainer build(JedisClusterConfig config) {
		GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
		genericObjectPoolConfig.setMaxIdle(config.getMaxIdle());
		genericObjectPoolConfig.setMaxTotal(config.getMaxTotal());
		genericObjectPoolConfig.setMinIdle(config.getMinIdle());

		JedisCluster jedisCluster = new JedisCluster(config.getNodes(), config.getTimeout(),
			config.getMaxRedirections(), genericObjectPoolConfig);
		return new RedisClusterContainer(jedisCluster);
	}

	/**
	 * Builds container for Redis Sentinel environment.
	 * @param config configuration for JedisSentinel
	 * @return container for Redis sentinel environment
     */
	public static RedisCommandsContainer build(JedisSentinelConfig config) {
		GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
		genericObjectPoolConfig.setMaxIdle(config.getMaxIdle());
		genericObjectPoolConfig.setMaxTotal(config.getMaxTotal());
		genericObjectPoolConfig.setMinIdle(config.getMinIdle());

		JedisSentinelPool jedisSentinelPool =new JedisSentinelPool(config.getMasterName(), config.getSentinels(), genericObjectPoolConfig,
			config.getConnectionTimeout(), config.getSoTimeout(), config.getPassword(), config.getDatabase());
		return new RedisContainer(jedisSentinelPool);
	}
}
