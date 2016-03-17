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

public class RedisDataTypeDescription implements Function, Serializable {

	private static final long serialVersionUID = 1L;

	private RedisDataType dataType;
	private String additionalKey;

	/**
	 * Use this constructor when data type is HASH or SORTED_SET
	 * @param dataType the redis data type {@link RedisDataType}
	 * @param additionalKey additional key for Hash and Sorted set data type
     */
	public RedisDataTypeDescription(RedisDataType dataType, String additionalKey) {
		this.dataType = dataType;
		this.additionalKey = additionalKey;

		if (dataType == RedisDataType.HASH || dataType == RedisDataType.SORTED_SET) {
			if (additionalKey == null) {
				throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
			}
		}
	}

	/**
	 * Use this constructor when data type is not HASH or SORTED_SET
	 * @param dataType the redis data type {@link RedisDataType}
     */
	public RedisDataTypeDescription(RedisDataType dataType) {
		this(dataType, null);
	}

	public RedisDataType getDataType() {
		return dataType;
	}

	public String getAdditionalKey() {
		return additionalKey;
	}
}
