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
 * The description of the data type. This must be passed while creating new {@link RedisMapper}.
 * <p>When creating descriptor for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET},
 * you need to use first constructor {@link #RedisDataTypeDescription(RedisDataType, String)}.
 * If the {@code additionalKey} is {@code null} it will throw {@code IllegalArgumentException}
 *
 * <p>When {@link RedisDataType} is not {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}
 * you can use second constructor {@link #RedisDataTypeDescription(RedisDataType)}
 */
public class RedisDataTypeDescription implements Serializable {

	private static final long serialVersionUID = 1L;

	private RedisDataType dataType;

	/**
	 * This additional key is needed for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}.
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
	 * @param dataType the redis data type {@link RedisDataType}
	 * @param additionalKey additional key for Hash and Sorted set data type
	 */
	public RedisDataTypeDescription(RedisDataType dataType, String additionalKey) {
		Preconditions.checkNotNull(dataType, "Redis data type can not be null");
		this.dataType = dataType;
		this.additionalKey = additionalKey;

		if (dataType == RedisDataType.HASH || dataType == RedisDataType.SORTED_SET) {
			if (additionalKey == null) {
				throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
			}
		}
	}

	/**
	 * Use this constructor when data type is not HASH or SORTED_SET.
	 *
	 * @param dataType the redis data type {@link RedisDataType}
	 */
	public RedisDataTypeDescription(RedisDataType dataType) {
		this(dataType, null);
	}

	/**
	 * Returns the {@link RedisDataType}.
	 *
	 * @return the data type of the mapping
	 */
	public RedisDataType getDataType() {
		return dataType;
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
