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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The map-like container class for parameter. This class is provided to unify the interaction with
 * parameters.
 */
@PublicEvolving
public class Params implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    /**
     * A mapping from param name to its value.
     *
     * <p>The value is stored in map using json format.
     */
    private final Map<String, String> params;

    private transient ObjectMapper mapper;

    public Params() {
        this.params = new HashMap<>();
    }

    /**
     * Return the number of params.
     *
     * @return Return the number of params.
     */
    public int size() {
        return params.size();
    }

    /** Removes all of the params. The params will be empty after this call returns. */
    public void clear() {
        params.clear();
    }

    /**
     * Returns <tt>true</tt> if this params contains no mappings.
     *
     * @return <tt>true</tt> if this map contains no mappings
     */
    public boolean isEmpty() {
        return params.isEmpty();
    }

    /**
     * Returns the value of the specific parameter, or default value defined in the {@code info} if
     * this Params doesn't have a value set for the parameter. An exception will be thrown in the
     * following cases because no value could be found for the specified parameter.
     *
     * <ul>
     *   <li>Non-optional parameter: no value is defined in this params for a non-optional
     *       parameter.
     *   <li>Optional parameter: no value is defined in this params and no default value is defined.
     * </ul>
     *
     * @param info the info of the specific parameter, usually with default value
     * @param <V> the type of the specific parameter
     * @return the value of the specific parameter, or default value defined in the {@code info} if
     *     this Params doesn't contain the parameter
     * @throws IllegalArgumentException if no value can be found for specified parameter
     */
    public <V> V get(ParamInfo<V> info) {
        String value = null;
        String usedParamName = null;
        for (String nameOrAlias : getParamNameAndAlias(info)) {
            if (params.containsKey(nameOrAlias)) {
                if (usedParamName != null) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Duplicate parameters of %s and %s",
                                    usedParamName, nameOrAlias));
                }
                usedParamName = nameOrAlias;
                value = params.get(nameOrAlias);
            }
        }

        if (usedParamName != null) {
            // The param value was set by the user.
            return valueFromJson(value, info.getValueClass());
        } else {
            // The param value was not set by the user.
            if (!info.isOptional()) {
                throw new IllegalArgumentException(
                        "Missing non-optional parameter " + info.getName());
            } else if (!info.hasDefaultValue()) {
                throw new IllegalArgumentException(
                        "Cannot find default value for optional parameter " + info.getName());
            }
            return info.getDefaultValue();
        }
    }

    /**
     * Set the value of the specific parameter.
     *
     * @param info the info of the specific parameter to set.
     * @param value the value to be set to the specific parameter.
     * @param <V> the type of the specific parameter.
     * @return the previous value of the specific parameter, or null if this Params didn't contain
     *     the parameter before
     * @throws RuntimeException if the {@code info} has a validator and the {@code value} is
     *     evaluated as illegal by the validator
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
     * @param <V> the type of the specific parameter
     */
    public <V> void remove(ParamInfo<V> info) {
        params.remove(info.getName());
        for (String a : info.getAlias()) {
            params.remove(a);
        }
    }

    /**
     * Check whether this params has a value set for the given {@code info}.
     *
     * @return <tt>true</tt> if this params has a value set for the specified {@code info}, false
     *     otherwise.
     */
    public <V> boolean contains(ParamInfo<V> info) {
        return params.containsKey(info.getName())
                || Arrays.stream(info.getAlias()).anyMatch(params::containsKey);
    }

    /**
     * Returns a json containing all parameters in this Params. The json should be human-readable if
     * possible.
     *
     * @return a json containing all parameters in this Params
     */
    public String toJson() {
        assertMapperInited();
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
        assertMapperInited();
        Map<String, String> params;
        try {
            params = mapper.readValue(json, Map.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize json:" + json, e);
        }
        this.params.putAll(params);
    }

    /**
     * Factory method for constructing params.
     *
     * @param json the json string to load
     * @return the {@code Params} loaded from the json string.
     */
    public static Params fromJson(String json) {
        Params params = new Params();
        params.loadJson(json);
        return params;
    }

    /**
     * Merge other params into this.
     *
     * @param otherParams other params
     * @return this
     */
    public Params merge(Params otherParams) {
        if (otherParams != null) {
            this.params.putAll(otherParams.params);
        }
        return this;
    }

    /**
     * Creates and returns a deep clone of this Params.
     *
     * @return a deep clone of this Params
     */
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

    private <V> List<String> getParamNameAndAlias(ParamInfo<V> info) {
        List<String> paramNames = new ArrayList<>(info.getAlias().length + 1);
        paramNames.add(info.getName());
        paramNames.addAll(Arrays.asList(info.getAlias()));
        return paramNames;
    }
}
