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

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * Redis command container if we want to connect to a single Redis server or to Redis sentinels
 * If want to connect to a single Redis server, please use the first constructor {@link #RedisContainer(JedisPool)}.
 * If want to connect to a Redis sentinels, Please use the second constructor {@link #RedisContainer(JedisSentinelPool)}
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);

	private final JedisPool jedisPool;
	private final JedisSentinelPool jedisSentinelPool;


	/**
	 * Use this constructor if to connect with single Redis server.
	 *
	 * @param jedisPool JedisPool which actually manages Jedis instances
	 */
	public RedisContainer(JedisPool jedisPool) {
		Preconditions.checkNotNull(jedisPool, "Jedis Pool can not be null");
		this.jedisPool = jedisPool;
		this.jedisSentinelPool = null;
	}

	/**
	 * Use this constructor if Redis environment is clustered with sentinels.
	 *
	 * @param sentinelPool SentinelPool which actually manages Jedis instances
	 */
	public RedisContainer(final JedisSentinelPool sentinelPool) {
		Preconditions.checkNotNull(sentinelPool, "Jedis Sentinel Pool can not be null");
		this.jedisPool = null;
		this.jedisSentinelPool = sentinelPool;
	}

	/**
	 * Closes the Jedis instances.
	 */
	@Override
	public void close() throws IOException {
		if (this.jedisPool != null) {
			this.jedisPool.close();
		}
		if (this.jedisSentinelPool != null) {
			this.jedisSentinelPool.close();
		}
	}

	@Override
	public void hset(final String key, final String hashField, final String value) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.hset(key, hashField, value);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command HSET to key {} and hashField {} error message {}",
					key, hashField, e.getMessage());
			}
			throw e;
		} finally {
			releaseInstance(jedis);
		}
	}

	@Override
	public void rpush(final String listName, final String value) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.rpush(listName, value);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command RPUSH to list {} error message {}",
					listName, e.getMessage());
			}
			throw e;
		} finally {
			releaseInstance(jedis);
		}
	}

	@Override
	public void lpush(String listName, String value) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.lpush(listName, value);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command LUSH to list {} error message {}",
					listName, e.getMessage());
			}
			throw e;
		} finally {
			releaseInstance(jedis);
		}
	}

	@Override
	public void sadd(final String setName, final String value) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.sadd(setName, value);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command RPUSH to set {} error message {}",
					setName, e.getMessage());
			}
			throw e;
		} finally {
			releaseInstance(jedis);
		}
	}

	@Override
	public void publish(final String channelName, final String message) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.publish(channelName, message);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command PUBLISH to channel {} error message {}",
					channelName, e.getMessage());
			}
			throw e;
		} finally {
			releaseInstance(jedis);
		}
	}

	@Override
	public void set(final String key, final String value) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.set(key, value);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command SET to key {} error message {}",
					key, e.getMessage());
			}
			throw e;
		} finally {
			releaseInstance(jedis);
		}
	}

	@Override
	public void pfadd(final String key, final String element) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.pfadd(key, element);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command PFADD to key {} error message {}",
					key, e.getMessage());
			}
			throw e;
		} finally {
			releaseInstance(jedis);
		}
	}

	@Override
	public void zadd(final String key, final String score, final String element) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.zadd(key, Double.valueOf(score), element);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command ZADD to set {} error message {}",
					key, e.getMessage());
			}
			throw e;
		} finally {
			releaseInstance(jedis);
		}
	}

	/**
	 * Returns Jedis instance from the pool.
	 *
	 * @return the Jedis instance
     */
	private Jedis getInstance() {
		if (jedisSentinelPool != null) {
			return jedisSentinelPool.getResource();
		} else {
			return jedisPool.getResource();
		}
	}

	/**
	 * Closes the jedis instance after finishing the command.
	 *
	 * @param jedis The jedis instance
     */
	private void releaseInstance(final Jedis jedis) {
		if (jedis == null) {
			return;
		}
		try {
			jedis.close();
		} catch (Exception e) {
			LOG.error("Failed to close (return) instance to pool", e);
		}
	}
}
