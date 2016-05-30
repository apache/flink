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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * Redis command container if we want to connect to a single Redis server or to Redis sentinels
 * If want to connect to a single Redis server, plz use the first constructor {@link #RedisContainer(JedisPool)}.
 * If want to connect to a Redis sentinels, Plz use the second constructor ${@link #RedisContainer(JedisSentinelPool)}
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);

	private JedisPool jedisPool;
	private JedisSentinelPool jedisSentinelPool;


	/**
	 * Use this constructor if to connect with single Redis server.
	 *
	 * @param jedisPool JedisPool which actually manages Jedis instances
	 */
	public RedisContainer(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	/**
	 * Use this constructor if Redis environment is clustered with sentinels.
	 *
	 * @param sentinelPool SentinelPool which actually manages Jedis instances
	 */
	public RedisContainer(final JedisSentinelPool sentinelPool) {
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

	/**
	 * Sets field in the hash stored at key to value.
	 * If key does not exist, a new key holding a hash is created.
	 * If field already exists in the hash, it is overwritten.
	 *
	 * @param hashName   Hash name
	 * @param key Hash field name
	 * @param value Hash value
	 */
	@Override
	public void hset(final String hashName, final String key, final String value) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.hset(hashName, key, value);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command HSET to key {} and field {} error message {}",
					key, key, e.getMessage());
			}
		} finally {
			returnInstance(jedis);
		}
	}

	/**
	 * Insert all the specified values at the tail of the list stored at key.
	 * If key does not exist, it is created as empty list before performing the push operation.
	 *
	 * @param listName Name of the List
	 * @param value    Value to be added
	 */
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
		} finally {
			returnInstance(jedis);
		}
	}

	/**
	 * Add the specified members to the set stored at key.
	 * Specified members that are already a member of this set are ignored.
	 * If key does not exist, a new set is created before adding the specified members.
	 *
	 * @param setName Name of the Set
	 * @param value   Value to be added
	 */
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
		} finally {
			returnInstance(jedis);
		}
	}

	/**
	 * Posts a message to the given channel
	 *
	 * @param channelName Name of the channel to which data will be published
	 * @param message     the message
	 */
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
		} finally {
			returnInstance(jedis);
		}
	}

	/**
	 * Set key to hold the string value. If key already holds a value, it is overwritten,
	 * regardless of its type. Any previous time to live associated with the key is
	 * discarded on successful SET operation.
	 *
	 * @param key   the key name in which value to be set
	 * @param value the value
	 */
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
		} finally {
			returnInstance(jedis);
		}
	}

	/**
	 * Adds all the element arguments to the HyperLogLog data structure
	 * stored at the variable name specified as first argument.
	 *
	 * @param key     The name of the key
	 * @param element the element
	 */
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
		} finally {
			returnInstance(jedis);
		}
	}

	/**
	 * Adds the specified member with the specified scores to the sorted set stored at key
	 *
	 * @param setName The name of the Sorted Set
	 * @param element element to be added
	 * @param score   Score of the element
	 */
	@Override
	public void zadd(final String setName, final String element, final String score) {
		Jedis jedis = null;
		try {
			jedis = getInstance();
			jedis.zadd(setName, Double.valueOf(score), element);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send Redis message with command ZADD to set {} error message {}",
					setName, e.getMessage());
			}
		} finally {
			returnInstance(jedis);
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
		} else if (jedisPool != null) {
			return jedisPool.getResource();
		} else {
			throw new IllegalArgumentException("Jedis Pool not found");
		}
	}

	/**
	 * Closes the jedis instance after finishing the command.
	 *
	 * @param jedis The jedis instance
     */
	private void returnInstance(final Jedis jedis) {
		if (jedis == null) {
			return;
		}
		try {
			jedis.close();
		} catch (Exception e) {
			LOG.error("Failed to close (return) instance to pool {}", e.getMessage());
		}
	}
}
