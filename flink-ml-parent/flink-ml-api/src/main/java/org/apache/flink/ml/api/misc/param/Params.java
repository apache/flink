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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The map-like container class for parameter. This class is provided to unify the interaction with
 * parameters.
 */
@PublicEvolving
public class Params implements Serializable, Cloneable {
	private static final long serialVersionUID = 1L;

	private final Map<String, String> params;

	private transient ObjectMapper mapper;

	public Params() {
		this.params = new HashMap<>();
	}

	public int size() {
		return params.size();
	}

	public void clear() {
		params.clear();
	}

	public boolean isEmpty() {
		return params.isEmpty();
	}

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
	public <V> V get(ParamInfo<V> info) {
		Stream<V> paramValue = getParamNameAndAlias(info)
			.filter(this.params::containsKey)
			.map(x -> this.params.get(x))
			.map(x -> valueFromJson(x, info.getValueClass()))
			.limit(1);

		if (info.isOptional()) {
			if (info.hasDefaultValue()) {
				return paramValue.reduce(info.getDefaultValue(), (a, b) -> b);
			} else {
				return paramValue.collect(Collectors.collectingAndThen(Collectors.toList(),
					a -> {
						if (a.isEmpty()) {
							throw new RuntimeException("Not have defaultValue for parameter: " + info.getName());
						}
						return a.get(0);
					}));
			}
		}
		return paramValue.collect(Collectors.collectingAndThen(Collectors.toList(),
			a -> {
				if (a.isEmpty()) {
					throw new RuntimeException("Not have parameter: " + info.getName());
				}
				return a.get(0);
			}));
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
		if (info.getValidator() != null && !info.getValidator().validate(value)) {
			throw new RuntimeException(
				"Setting " + info.getName() + " as a invalid value:" + value);
		}
		params.put(info.getName(), valueToJson(value));
		return this;
	}

	/**
	 * Removes the specific parameter from this Params.
	 *
	 * @param info the info of the specific parameter to remove
	 * @param <V>  the type of the specific parameter
	 */
	public <V> void remove(ParamInfo<V> info) {
		params.remove(info.getName());
		for (String a : info.getAlias()) {
			params.remove(a);
		}
	}

	public <V> boolean contains(ParamInfo<V> paramInfo) {
		return params.containsKey(paramInfo.getName()) ||
			Arrays.stream(paramInfo.getAlias()).anyMatch(params::containsKey);
	}

	/**
	 * Creates and returns a deep clone of this Params.
	 *
	 * @return a json containing all parameters in this Params
	 */
	public String toJson() {
		try {
			return mapper.writeValueAsString(params);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize params to json", e);
		}
	}

	/**
	 * Restores the parameters from the given json. The parameters should be exactly the same with
	 * the one who was serialized to the input json after the restoration.
	 *
	 * @param json the json String to restore from
	 */
	@SuppressWarnings("unchecked")
	public void loadJson(String json) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> params;
		try {
			params = mapper.readValue(json, Map.class);
		} catch (IOException e) {
			throw new RuntimeException("Failed to deserialize json:" + json, e);
		}
		this.params.clear();
		this.params.putAll(params);
	}

	public static Params fromJson(String json) {
		Params params = new Params();
		params.loadJson(json);
		return params;
	}

	public Params merge(Params otherParams) {
		if (otherParams != null) {
			this.params.putAll(otherParams.params);
		}
		return this;
	}

	@Override
	public Params clone() {
		Params newParams = new Params();
		newParams.params.putAll(this.params);
		return newParams;
	}

	private void assertMapperInited() {
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
	}

	private String valueToJson(Object value) {
		assertMapperInited();
		try {
			if (value == null) {
				return null;
			}
			return mapper.writeValueAsString(value);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize to json:" + value, e);
		}
	}

	private <T> T valueFromJson(String json, Class<T> clazz) {
		assertMapperInited();
		try {
			if (json == null) {
				return null;
			}
			return mapper.readValue(json, clazz);
		} catch (IOException e) {
			throw new RuntimeException("Failed to deserialize json:" + json, e);
		}
	}

	private <V> Stream <String> getParamNameAndAlias(
		ParamInfo <V> paramInfo) {
		Stream <String> paramNames = Stream.of(paramInfo.getName());
		if (null != paramInfo.getAlias() && paramInfo.getAlias().length > 0) {
			paramNames = Stream.concat(paramNames, Arrays.stream(paramInfo.getAlias())).sequential();
		}
		return paramNames;
	}
}
