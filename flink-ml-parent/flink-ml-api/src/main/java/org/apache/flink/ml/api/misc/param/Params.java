/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.api.misc.param;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The map-like container class for parameter. This class is provided to unify the interaction with
 * parameters.
 */
@PublicEvolving
public class Params implements Serializable {
	private final Map<String, Object> paramMap = new HashMap<>();

	/**
	 * Returns the value of the specific parameter, or default value defined in the {@code info} if
	 * this Params doesn't contain the param.
	 *
	 * @param info the info of the specific parameter, usually with default value
	 * @param <V>  the type of the specific parameter
	 * @return the value of the specific parameter, or default value defined in the {@code info} if
	 * this Params doesn't contain the parameter
	 * @throws RuntimeException if the Params doesn't contains the specific parameter, while the
	 *                          param is not optional but has no default value in the {@code info}
	 */
	@SuppressWarnings("unchecked")
	public <V> V get(ParamInfo<V> info) {
		V value = (V) paramMap.getOrDefault(info.getName(), info.getDefaultValue());
		if (value == null && !info.isOptional() && !info.hasDefaultValue()) {
			throw new RuntimeException(info.getName() +
				" not exist which is not optional and don't have a default value");
		}
		return value;
	}

	/**
	 * Set the value of the specific parameter.
	 *
	 * @param info  the info of the specific parameter to set.
	 * @param value the value to be set to the specific parameter.
	 * @param <V>   the type of the specific parameter.
	 * @return the previous value of the specific parameter, or null if this Params didn't contain
	 * the parameter before
	 * @throws RuntimeException if the {@code info} has a validator and the {@code value} is
	 *                          evaluated as illegal by the validator
	 */
	public <V> Params set(ParamInfo<V> info, V value) {
		if (!info.isOptional() && value == null) {
			throw new RuntimeException(
				"Setting " + info.getName() + " as null while it's not a optional param");
		}
		if (value == null) {
			remove(info);
			return this;
		}

		if (info.getValidator() != null && !info.getValidator().validate(value)) {
			throw new RuntimeException(
				"Setting " + info.getName() + " as a invalid value:" + value);
		}
		paramMap.put(info.getName(), value);
		return this;
	}

	/**
	 * Removes the specific parameter from this Params.
	 *
	 * @param info the info of the specific parameter to remove
	 * @param <V>  the type of the specific parameter
	 */
	public <V> void remove(ParamInfo<V> info) {
		paramMap.remove(info.getName());
	}

	/**
	 * Creates and returns a deep clone of this Params.
	 *
	 * @return a deep clone of this Params
	 */
	public Params clone() {
		Params newParams = new Params();
		newParams.paramMap.putAll(this.paramMap);
		return newParams;
	}

	/**
	 * Returns a json containing all parameters in this Params. The json should be human-readable if
	 * possible.
	 *
	 * @return a json containing all parameters in this Params
	 */
	public String toJson() {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> stringMap = new HashMap<>();
		try {
			for (Map.Entry<String, Object> e : paramMap.entrySet()) {
				stringMap.put(e.getKey(), mapper.writeValueAsString(e.getValue()));
			}
			return mapper.writeValueAsString(stringMap);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize params to json", e);
		}
	}

	/**
	 * Restores the parameters from the given json. The parameters should be exactly the same with
	 * the one who was serialized to the input json after the restoration. The class mapping of the
	 * parameters in the json is required because it is hard to directly restore a param of a user
	 * defined type. Params will be treated as String if it doesn't exist in the {@code classMap}.
	 *
	 * @param json     the json String to restore from
	 * @param classMap the classes of the parameters contained in the json
	 */
	@SuppressWarnings("unchecked")
	public void loadJson(String json, Map<String, Class<?>> classMap) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			Map<String, String> m = mapper.readValue(json, Map.class);
			for (Map.Entry<String, String> e : m.entrySet()) {
				Class<?> valueClass = classMap.getOrDefault(e.getKey(), String.class);
				paramMap.put(e.getKey(), mapper.readValue(e.getValue(), valueClass));
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to deserialize json:" + json, e);
		}
	}
}
