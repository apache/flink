/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Json wrapper.
 */
public class JsonConverter {
	private ObjectMapper jsonMapper = new ObjectMapper()
		.setVisibility(PropertyAccessor.ALL, Visibility.NONE)
		.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

	public String toJson(Object src) {
		try {
			return jsonMapper.writeValueAsString(src);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Serialize object to json fail.", e);
		}
	}

	public <T> T fromJson(String json, Class<T> classOfT) {
		try {
			return jsonMapper.readValue(json, classOfT);
		} catch (IOException e) {
			throw new RuntimeException("Deserialize json to object fail. json: " + json, e);
		}
	}
}
