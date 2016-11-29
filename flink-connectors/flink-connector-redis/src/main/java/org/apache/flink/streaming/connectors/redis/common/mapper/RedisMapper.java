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

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Function that creates the description how the input data should be mapped to redis type.
 *<p>Example:
 *<pre>{@code
 *private static class RedisTestMapper implements RedisMapper<Tuple2<String, String>> {
 *    public RedisDataTypeDescription getCommandDescription() {
 *        return new RedisDataTypeDescription(RedisCommand.PUBLISH);
 *    }
 *    public String getKeyFromData(Tuple2<String, String> data) {
 *        return data.f0;
 *    }
 *    public String getValueFromData(Tuple2<String, String> data) {
 *        return data.f1;
 *    }
 *}
 *}</pre>
 *
 * @param <T> The type of the element handled by this {@code RedisMapper}
 */
public interface RedisMapper<T> extends Function, Serializable {

	/**
	 * Returns descriptor which defines data type.
	 *
	 * @return data type descriptor
	 */
	RedisCommandDescription getCommandDescription();

	/**
	 * Extracts key from data.
	 *
	 * @param data source data
	 * @return key
	 */
	String getKeyFromData(T data);

	/**
	 * Extracts value from data.
	 *
	 * @param data source data
	 * @return value
	 */
	String getValueFromData(T data);
}
