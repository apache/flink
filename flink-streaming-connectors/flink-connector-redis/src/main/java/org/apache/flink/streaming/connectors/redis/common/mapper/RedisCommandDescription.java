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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * The description of the command type. This must be passed while creating new {@link RedisMapper}.
 * <p>When creating descriptor for the group of {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET},
 * you need to use first constructor {@link #RedisCommandDescription(RedisCommand, String)}.
 * If the {@code additionalKey} is {@code null} it will throw {@code IllegalArgumentException}
 *
 * <p>When {@link RedisCommand} is not in group of {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}
 * you can use second constructor {@link #RedisCommandDescription(RedisCommand)}
 */
public class RedisCommandDescription implements Serializable {

	private static final long serialVersionUID = 1L;

	private RedisCommand redisCommand;

	/**
	 * This additional key is needed for the group {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}.
	 * Other {@link RedisDataType} works only with two variable i.e. name of the list and value to be added.
	 * But for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET} we need three variables.
	 * <p>For {@link RedisDataType#HASH} we need hash name, hash key and element.
	 * {@link #getAdditionalKey()} used as hash name for {@link RedisDataType#HASH}
	 * <p>For {@link RedisDataType#SORTED_SET} we need set name, the element and it's score.
	 * {@link #getAdditionalKey()} used as set name for {@link RedisDataType#SORTED_SET}
	 */
	private String additionalKey;

	/**
	 * Use this constructor when data type is {@link RedisDataType#HASH} or {@link RedisDataType#SORTED_SET}.
	 * If different data type is specified, {@code additionalKey} is ignored.
	 * @param redisCommand the redis command type {@link RedisCommand}
	 * @param additionalKey additional key for Hash and Sorted set data type
	 */
	public RedisCommandDescription(RedisCommand redisCommand, String additionalKey) {
		Preconditions.checkNotNull(redisCommand, "Redis command type can not be null");
		this.redisCommand = redisCommand;
		this.additionalKey = additionalKey;

		if (redisCommand.getRedisDataType() == RedisDataType.HASH ||
			redisCommand.getRedisDataType() == RedisDataType.SORTED_SET) {
			if (additionalKey == null) {
				throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
			}
		}
	}

	/**
	 * Use this constructor when command type is not in group {@link RedisDataType#HASH} or {@link RedisDataType#SORTED_SET}.
	 *
	 * @param redisCommand the redis data type {@link RedisCommand}
	 */
	public RedisCommandDescription(RedisCommand redisCommand) {
		this(redisCommand, null);
	}

	/**
	 * Returns the {@link RedisCommand}.
	 *
	 * @return the command type of the mapping
	 */
	public RedisCommand getCommand() {
		return redisCommand;
	}

	/**
	 * Returns the additional key if data type is {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}.
	 *
	 * @return the additional key
	 */
	public String getAdditionalKey() {
		return additionalKey;
	}
}
