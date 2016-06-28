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
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A sink that delivers data to a Redis channel using the Jedis client.
 * <p>When creating the sink using first constructor {@link #RedisSink(FlinkJedisPoolConfig, RedisMapper)}
 * the sink will create connection using {@link redis.clients.jedis.JedisPool}.
 * <p>When using second constructor {@link #RedisSink(FlinkJedisSentinelConfig, RedisMapper)} the sink will create connection
 * using {@link redis.clients.jedis.JedisSentinelPool} to Redis cluster. Use this if Redis is
 * configured using sentinels else use the third constructor {@link #RedisSink(FlinkJedisClusterConfig, RedisMapper)}
 * which use {@link redis.clients.jedis.JedisCluster} to connect to Redis cluster.
 *
 * <p>Example:
 *
 * <pre>
 *{@code
 *public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {
 *
 *	private RedisCommand redisCommand;
 *
 *	public RedisAdditionalDataMapper(RedisCommand redisCommand){
 *		this.redisCommand = redisCommand;
 *	}
 *	public RedisDataTypeDescription getDataTypeDescription() {
 *		return new RedisDataTypeDescription(redisCommand, REDIS_ADDITIONAL_KEY);
 *	}
 *	public String getKeyFromData(Tuple2<String, String> data) {
 *		return data.f0;
 *	}
 *	public String getValueFromData(Tuple2<String, String> data) {
 *		return data.f1;
 *	}
 *}
 *JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
 *    .setHost(REDIS_HOST).setPort(REDIS_PORT).build();
 *new RedisSink<String>(jedisPoolConfig, new RedisExampleDataMapper(RedisCommand.LPUSH));
 *}</pre>
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class RedisSink<IN> extends RichSinkFunction<IN> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

	/**
	 * This additional key needed for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}.
	 * Other {@link RedisDataType} works only with two variable i.e. name of the list and value to be added.
	 * But for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET} we need three variables.
	 * <p>For {@link RedisDataType#HASH} we need hash name, hash key and element.
	 * {@code additionalKey} used as hash name for {@link RedisDataType#HASH}
	 * <p>For {@link RedisDataType#SORTED_SET} we need set name, the element and it's score.
	 * {@code additionalKey} used as set name for {@link RedisDataType#SORTED_SET}
	 */
	private String additionalKey;
	private RedisMapper<IN> redisSinkMapper;
	private RedisCommand redisCommand;

	private FlinkJedisPoolConfig jedisPoolConfig;
	private FlinkJedisSentinelConfig jedisSentinelConfig;
	private FlinkJedisClusterConfig jedisClusterConfig;

	private RedisCommandsContainer redisCommandsContainer;

	/**
	 * Creates a new {@link RedisSink} that connects to the Redis Server.
	 *
	 * @param jedisPoolConfig The configuration of {@link FlinkJedisPoolConfig}
	 * @param redisSinkMapper This is used to generate Redis command and key value from incoming elements.
	 */
	public RedisSink(FlinkJedisPoolConfig jedisPoolConfig, RedisMapper<IN> redisSinkMapper) {
		Preconditions.checkNotNull(jedisPoolConfig, "Redis connection pool config should not be null");
		this.jedisPoolConfig = jedisPoolConfig;
		init(redisSinkMapper);
	}

	/**
	 * Creates a new {@link RedisSink} that connects to the Redis Sentinels.
	 *
	 * @param jedisSentinelConfig The configuration of {@link FlinkJedisSentinelConfig}
	 * @param redisSinkMapper This used for generate redis command and key value from incoming elements
	 */
	public RedisSink(FlinkJedisSentinelConfig jedisSentinelConfig, RedisMapper<IN> redisSinkMapper) {
		Preconditions.checkNotNull(jedisSentinelConfig, "Redis Sentinel connection pool config should not be Null");
		this.jedisSentinelConfig = jedisSentinelConfig;
		init(redisSinkMapper);
	}

	/**
	 * Creates a new {@link RedisSink} that connects to the Redis Cluster.
	 *
	 * @param jedisClusterConfig The configuration of {@link FlinkJedisClusterConfig}
	 * @param redisSinkMapper This used for generate redis command and key value from incoming elements
	 */
	public RedisSink(FlinkJedisClusterConfig jedisClusterConfig, RedisMapper<IN> redisSinkMapper) {
		Preconditions.checkNotNull(jedisClusterConfig, "Redis cluster config should not be Null");
		this.jedisClusterConfig = jedisClusterConfig;
		init(redisSinkMapper);

	}

	private void init(RedisMapper<IN> redisSinkMapper){
		Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");
		Preconditions.checkNotNull(redisSinkMapper.getDataTypeDescription(), "Redis Mapper data type description can not be null");
		this.redisSinkMapper = redisSinkMapper;
		RedisCommandDescription dataTypeDescription = redisSinkMapper.getDataTypeDescription();
		this.redisCommand = dataTypeDescription.getCommand();
		this.additionalKey = dataTypeDescription.getAdditionalKey();
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Redis channel.
	 * Depending on the specified Redis data type (see {@link RedisDataType}),
	 * a different Redis command will be applied.
	 * Available commands are HSET, RPUSH, SADD, PUBLISH, SET, PFADD, and ZADD.
	 *
	 * @param input The incoming data
	 */
	@Override
	public void invoke(IN input) throws Exception {
		String key = redisSinkMapper.getKeyFromData(input);
		String value = redisSinkMapper.getValueFromData(input);

		switch (redisCommand) {
			case RPUSH:
				this.redisCommandsContainer.rpush(key, value);
				break;
			case LPUSH:
				this.redisCommandsContainer.lpush(key, value);
				break;
			case SADD:
				this.redisCommandsContainer.sadd(key, value);
				break;
			case SET:
				this.redisCommandsContainer.set(key, value);
				break;
			case PFADD:
				this.redisCommandsContainer.pfadd(key, value);
				break;
			case PUBLISH:
				this.redisCommandsContainer.publish(key, value);
				break;
			case ZADD:
				this.redisCommandsContainer.zadd(this.additionalKey, value, key);
				break;
			case HSET:
				this.redisCommandsContainer.hset(this.additionalKey, key, value);
				break;
			default:
				throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
		}
	}

	/**
	 * Initializes the connection to Redis by either cluster or sentinels or single server.
	 *
	 * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
	@Override
	public void open(Configuration parameters) throws Exception {
		if (jedisPoolConfig != null) {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisPoolConfig);
		} else if (jedisClusterConfig != null) {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisClusterConfig);
		} else if (jedisSentinelConfig != null) {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisSentinelConfig);
		} else {
			throw new IllegalArgumentException("Jedis configuration not found");
		}
	}

	/**
	 * Closes commands container.
	 * @throws IOException if command container is unable to close.
	 */
	@Override
	public void close() throws IOException {
		if (redisCommandsContainer != null) {
			redisCommandsContainer.close();
		}
	}
}
