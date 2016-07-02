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
package org.apache.flink.streaming.connectors.redis.common.mapper;

/**
 * All available commands for Redis. Each command belongs to a {@link RedisDataType} group.
 */
public enum RedisCommand {

	/**
	 * Insert the specified value at the head of the list stored at key.
	 * If key does not exist, it is created as empty list before performing the push operations.
	 */
	LPUSH(RedisDataType.LIST),

	/**
	 * Insert the specified value at the tail of the list stored at key.
	 * If key does not exist, it is created as empty list before performing the push operation.
	 */
	RPUSH(RedisDataType.LIST),

	/**
	 * Add the specified member to the set stored at key.
	 * Specified member that is already a member of this set is ignored.
	 */
	SADD(RedisDataType.SET),

	/**
	 * Set key to hold the string value. If key already holds a value,
	 * it is overwritten, regardless of its type.
	 */
	SET(RedisDataType.STRING),

	/**
	 * Adds the element to the HyperLogLog data structure stored at the variable name specified as first argument.
	 */
	PFADD(RedisDataType.HYPER_LOG_LOG),

	/**
	 * Posts a message to the given channel.
	 */
	PUBLISH(RedisDataType.PUBSUB),

	/**
	 * Adds the specified members with the specified score to the sorted set stored at key.
	 */
	ZADD(RedisDataType.SORTED_SET),

	/**
	 * Sets field in the hash stored at key to value. If key does not exist,
	 * a new key holding a hash is created. If field already exists in the hash, it is overwritten.
	 */
	HSET(RedisDataType.HASH);

	/**
	 * The {@link RedisDataType} this command belongs to.
	 */
	private RedisDataType redisDataType;

	RedisCommand(RedisDataType redisDataType) {
		this.redisDataType = redisDataType;
	}


	/**
	 * The {@link RedisDataType} this command belongs to.
	 * @return the {@link RedisDataType}
	 */
	public RedisDataType getRedisDataType(){
		return redisDataType;
	}
}
