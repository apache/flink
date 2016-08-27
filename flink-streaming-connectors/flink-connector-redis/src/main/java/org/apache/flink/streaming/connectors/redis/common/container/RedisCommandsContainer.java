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

import java.io.IOException;
import java.io.Serializable;

/**
 * The container for all available Redis commands.
 */
public interface RedisCommandsContainer extends Serializable {

	/**
	 * Open the Jedis container.
	 *
	 * @throws Exception if the instance can not be opened properly
	 */
	void open() throws Exception;

	/**
	 * Sets field in the hash stored at key to value.
	 * If key does not exist, a new key holding a hash is created.
	 * If field already exists in the hash, it is overwritten.
	 *
	 * @param key Hash name
	 * @param hashField Hash field
	 * @param value Hash value
	 */
	void hset(String key, String hashField, String value);

	/**
	 * Insert the specified value at the tail of the list stored at key.
	 * If key does not exist, it is created as empty list before performing the push operation.
	 *
	 * @param listName Name of the List
	 * @param value  Value to be added
	 */
	void rpush(String listName, String value);

	/**
	 * Insert the specified value at the head of the list stored at key.
	 * If key does not exist, it is created as empty list before performing the push operation.
	 *
	 * @param listName Name of the List
	 * @param value  Value to be added
	 */
	void lpush(String listName, String value);

	/**
	 * Add the specified member to the set stored at key.
	 * Specified members that are already a member of this set are ignored.
	 * If key does not exist, a new set is created before adding the specified members.
	 *
	 * @param setName Name of the Set
	 * @param value Value to be added
	 */
	void sadd(String setName, String value);

	/**
	 * Posts a message to the given channel.
	 *
	 * @param channelName Name of the channel to which data will be published
	 * @param message the message
	 */
	void publish(String channelName, String message);

	/**
	 * Set key to hold the string value. If key already holds a value, it is overwritten,
	 * regardless of its type. Any previous time to live associated with the key is
	 * discarded on successful SET operation.
	 *
	 * @param key the key name in which value to be set
	 * @param value the value
	 */
	void set(String key, String value);

	/**
	 * Adds all the element arguments to the HyperLogLog data structure
	 * stored at the variable name specified as first argument.
	 *
	 * @param key The name of the key
	 * @param element the element
	 */
	void pfadd(String key, String element);

	/**
	 * Adds the specified member with the specified scores to the sorted set stored at key.
	 *
	 * @param key The name of the Sorted Set
	 * @param score Score of the element
	 * @param element  element to be added
	 */
	void zadd(String key, String score, String element);

	/**
	 * Close the Jedis container.
	 *
	 * @throws IOException if the instance can not be closed properly
	 */
	void close() throws IOException;
}
