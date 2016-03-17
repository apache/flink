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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.JedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.JedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.JedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataTypeDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import redis.clients.jedis.exceptions.JedisException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A sink that delivers data to a Redis channel using the Jedis client.
 *
 *  <p>
 * When creating the sink using first constructor {@link #RedisSink(JedisPoolConfig, RedisMapper)}
 * the sink will create connection using {@link redis.clients.jedis.JedisPool}.
 *
 * When using second constructor {@link #RedisSink(JedisSentinelConfig, RedisMapper)} the sink will create connection
 * using {@link redis.clients.jedis.JedisSentinelPool} to redis cluster. Use this if redis is
 * configured using sentinels else use the third constructor {@link #RedisSink(JedisClusterConfig, RedisMapper)}
 * which use {@link redis.clients.jedis.JedisCluster} to connect to redis cluster.
 *
 * <p>
 * Example:
 *
 * <pre>{@code
 *
 * 		public static class RedisExampleDataMapper implements RedisMapper<Tuple2<String, String>>{
 *			@Override
 *			public RedisDataTypeDescription getDataTypeDescription() {
 *				return new RedisDataTypeDescription(dataType, REDIS_ADDITIONAL_KEY);
 *			}
 *			@Override
 *			public String getKeyFromData(Tuple2 data) {
 *				return String.valueOf(data.f0);
 *			}
 *
 *			@Override
 *			public String getValueFromData(Tuple2 data) {
 *				return String.valueOf(data.f1);
 *			}
 *		}
 *		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
 *		.setHost(REDIS_HOST)
 *		.setPort(REDIS_PORT).build();
 *      new RedisSink<String>(jedisPoolConfig, new RedisExampleDataMapper());
 * }</pre>
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class RedisSink<IN> extends RichSinkFunction<IN>{

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

	private String additionalKey;
	private RedisMapper<IN> redisSinkMapper;
	private RedisDataType redisDataType;

	private JedisPoolConfig jedisPoolConfig;
	private JedisSentinelConfig jedisSentinelConfig;
	private JedisClusterConfig jedisClusterConfig;

	private RedisCommandsContainer redisCommandsContainer;

	/**
	 *	Creates a new RedisSink that connects to the Redis Server
	 *
	 * @param jedisPoolConfig The configuration of {@link JedisPoolConfig}
	 * @param redisSinkMapper This used for generate redis command and key value from incoming elements
	 */
	public RedisSink(JedisPoolConfig jedisPoolConfig, RedisMapper<IN> redisSinkMapper){
		Preconditions.checkNotNull(jedisPoolConfig, "Redis connection pool config should not be Null");
		Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");

		this.redisSinkMapper = redisSinkMapper;
		this.jedisPoolConfig = jedisPoolConfig;
		RedisDataTypeDescription dataTypeDescription = redisSinkMapper.getDataTypeDescription();
		this.redisDataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();

	}

	/**
	 * Creates a new RedisSink that connects to the Redis Sentinels
	 * @param jedisSentinelConfig The configuration of {@link JedisSentinelConfig}
	 * @param redisSinkMapper This used for generate redis command and key value from incoming elements
	 */
	public RedisSink(JedisSentinelConfig jedisSentinelConfig, RedisMapper<IN> redisSinkMapper){
		Preconditions.checkNotNull(jedisSentinelConfig, "Redis Sentinel connection pool config should not be Null");
		Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");

		this.redisSinkMapper = redisSinkMapper;
		this.jedisSentinelConfig = jedisSentinelConfig;
		RedisDataTypeDescription dataTypeDescription = redisSinkMapper.getDataTypeDescription();
		this.redisDataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();

	}

	/**
	 * Creates a new RedisSink that connects to the Redis Cluster
	 * @param jedisClusterConfig The configuration of {@link JedisClusterConfig}
	 * @param redisSinkMapper This used for generate redis command and key value from incoming elements
	 */
	public RedisSink(JedisClusterConfig jedisClusterConfig, RedisMapper<IN> redisSinkMapper){
		Preconditions.checkNotNull(jedisClusterConfig, "Redis cluster config should not be Null");
		Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");

		this.redisSinkMapper = redisSinkMapper;
		this.jedisClusterConfig = jedisClusterConfig;
		RedisDataTypeDescription dataTypeDescription = redisSinkMapper.getDataTypeDescription();
		this.redisDataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();

	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Redis channel.
	 *
	 * @param input The incoming data
	 */
	@Override
	public void invoke(IN input) throws Exception {
		String key = redisSinkMapper.getKeyFromData(input);
		String value = redisSinkMapper.getValueFromData(input);

		switch (redisDataType) {
			case HASH:
				this.redisCommandsContainer.hset(this.additionalKey, key, value);
				break;

			case LIST:
				this.redisCommandsContainer.rpush(key, value);
				break;

			case SET:
				this.redisCommandsContainer.sadd(key, value);
				break;

			case PUBSUB:
				this.redisCommandsContainer.publish(key, value);
				break;

			case STRING:
				this.redisCommandsContainer.set(key, value);
				break;

			case HYPER_LOG_LOG:
				this.redisCommandsContainer.pfadd(key, value);
				break;

			case SORTED_SET:
				this.redisCommandsContainer.zadd(this.additionalKey, value, key);
				break;

			default:
				throw new IllegalArgumentException("Cannot process such data type: " + redisDataType);
		}

	}

	/**
	 * Initializes the connection to Redis by either cluster or sentinels or single server
	 * @throws Exception
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		if (jedisPoolConfig != null) {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisPoolConfig);
		} else if (jedisClusterConfig != null) {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisClusterConfig);
		} else if (jedisSentinelConfig != null){
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisSentinelConfig);
		} else {
			throw new IllegalArgumentException("Jedis configuration not found");
		}
	}

	/**
	 * Closes commands container
	 * @throws Exception
	 */
	@Override
	public void close() throws Exception {
		if (redisCommandsContainer != null){
			try {
				redisCommandsContainer.close();
			} catch (JedisException e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("failed to close Redis Commands Container");
				}
				throw new RuntimeException("Error while closing Commands Container with error message {}", e);
			}
		}
	}
}
