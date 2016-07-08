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
 * All available data type for Redis.
 */
public enum RedisDataType {

	/**
	 * Strings are the most basic kind of Redis value. Redis Strings are binary safe,
	 * this means that a Redis string can contain any kind of data, for instance a JPEG image or a serialized Ruby object.
	 * A String value can be at max 512 Megabytes in length.
	 */
	STRING,

	/**
	 * Redis Hashes are maps between string fields and string values.
	 */
	HASH,

	/**
	 * Redis Lists are simply lists of strings, sorted by insertion order.
	 */
	LIST,

	/**
	 * Redis Sets are an unordered collection of Strings.
	 */
	SET,

	/**
	 * Redis Sorted Sets are, similarly to Redis Sets, non repeating collections of Strings.
	 * The difference is that every member of a Sorted Set is associated with score,
	 * that is used in order to take the sorted set ordered, from the smallest to the greatest score.
	 * While members are unique, scores may be repeated.
	 */
	SORTED_SET,

	/**
	 * HyperLogLog is a probabilistic data structure used in order to count unique things.
	 */
	HYPER_LOG_LOG,

	/**
	 * Redis implementation of publish and subscribe paradigm. Published messages are characterized into channels,
	 * without knowledge of what (if any) subscribers there may be.
	 * Subscribers express interest in one or more channels, and only receive messages
	 * that are of interest, without knowledge of what (if any) publishers there are.
	 */
	PUBSUB
}
