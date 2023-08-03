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

package org.apache.flink.streaming.examples.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.Utils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class provides simple utility methods for parsing program arguments into a map. The key of
 * map is the argument key. The value of map is the list of argument values of the same argument
 * key.
 *
 * <p><strong>Example arguments:</strong> --key1 value1 --key2 value2 -key3 value3 --multi
 * multiValue1 --multi multiValue2
 */
public class ParameterTool extends ExecutionConfig.GlobalJobParameters {
    private static final long serialVersionUID = 1L;

    private static final String NO_VALUE_KEY = "__NO_VALUE_KEY";

    /**
     * Returns {@link ParameterTool} for the given program arguments.
     *
     * @param args Input array arguments
     * @return A {@link ParameterTool}
     */
    public static ParameterTool fromArgs(String[] args) {
        final Map<String, Collection<String>> map =
                CollectionUtil.newHashMapWithExpectedSize(args.length / 2);

        int i = 0;
        while (i < args.length) {
            final String key = Utils.getKeyFromArgs(args, i);

            i += 1; // try to find the value

            map.putIfAbsent(key, new ArrayList<>());
            if (i >= args.length) {
                map.get(key).add(NO_VALUE_KEY);
            } else if (NumberUtils.isCreatable(args[i])) {
                map.get(key).add(args[i]);
                i += 1;
            } else if (args[i].startsWith("--") || args[i].startsWith("-")) {
                // the argument cannot be a negative number because we checked earlier
                // -> the next argument is a parameter name
                map.get(key).add(NO_VALUE_KEY);
            } else {
                map.get(key).add(args[i]);
                i += 1;
            }
        }
        return new ParameterTool(map);
    }

    private final Map<String, Collection<String>> data;

    private ParameterTool(Map<String, Collection<String>> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data));
    }

    // ------------------ Get data from the util ----------------

    /** Returns the String value for the given key. The value should only have one item. */
    public String get(String key) {
        if (!data.containsKey(key)) {
            return null;
        }
        Preconditions.checkState(
                data.get(key).size() == 1, "Key %s should has only one value.", key);
        return (String) data.get(key).toArray()[0];
    }

    /**
     * Returns the String value for the given key. If the key does not exist it will return the
     * given default value.
     */
    public String get(String key, String defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return value;
        }
    }

    /**
     * Returns the Long value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not a Long.
     */
    public long getLong(String key, long defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Long.parseLong(value);
        }
    }

    /**
     * Returns the Boolean value for the given key. If the key does not exists it will return the
     * default value given. The method returns whether the string of the value is "true" ignoring
     * cases.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Boolean.parseBoolean(value);
        }
    }

    /**
     * Returns the Double value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not a Double.
     */
    public double getDouble(String key, double defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Double.parseDouble(value);
        }
    }

    /**
     * Returns the Integer value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not an Integer.
     */
    public int getInt(String key, int defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    /**
     * Returns the Integer value for the given key. The method fails if the key does not exist or
     * the value is not an Integer.
     */
    public int getInt(String key) {
        String value = get(key);
        if (value == null) {
            throw new RuntimeException("No data for required key '" + key + "'");
        }
        return Integer.parseInt(value);
    }

    /** Check if value is set. */
    public boolean has(String value) {
        return data.containsKey(value);
    }

    /**
     * Returns the Collection of String values for the given key. If the key does not exist it will
     * throw a {@link RuntimeException}.
     */
    public Collection<String> getMultiParameterRequired(String key) {
        Collection<String> value = data.getOrDefault(key, null);
        if (value == null) {
            throw new RuntimeException("No data for required key '" + key + "'");
        }
        return value;
    }

    @Override
    public Map<String, String> toMap() {
        return data.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e -> {
                                    if (e.getValue().size() > 0) {
                                        return (String)
                                                e.getValue().toArray()[e.getValue().size() - 1];
                                    } else {
                                        return NO_VALUE_KEY;
                                    }
                                }));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParameterTool that = (ParameterTool) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
