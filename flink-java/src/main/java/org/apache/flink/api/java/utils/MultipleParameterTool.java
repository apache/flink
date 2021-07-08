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

package org.apache.flink.api.java.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.Utils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class provides simple utility methods for reading and parsing program arguments from
 * different sources. Multiple values parameter in args could be supported. For example, --multi
 * multiValue1 --multi multiValue2. If {@link MultipleParameterTool} object is used for
 * GlobalJobParameters, the last one of multiple values will be used. Navigate to {@link #toMap()}
 * for more information.
 */
@PublicEvolving
public class MultipleParameterTool extends AbstractParameterTool {
    private static final long serialVersionUID = 1L;

    // ------------------ Constructors ------------------------

    /**
     * Returns {@link MultipleParameterTool} for the given arguments. The arguments are keys
     * followed by values. Keys have to start with '-' or '--'
     *
     * <p><strong>Example arguments:</strong> --key1 value1 --key2 value2 -key3 value3 --multi
     * multiValue1 --multi multiValue2
     *
     * @param args Input array arguments
     * @return A {@link MultipleParameterTool}
     */
    public static MultipleParameterTool fromArgs(String[] args) {
        final Map<String, Collection<String>> map = new HashMap<>(args.length / 2);

        int i = 0;
        while (i < args.length) {
            final String key = Utils.getKeyFromArgs(args, i);

            i += 1; // try to find the value

            map.putIfAbsent(key, new ArrayList<>());
            if (i >= args.length) {
                map.get(key).add(NO_VALUE_KEY);
            } else if (NumberUtils.isNumber(args[i])) {
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

        return fromMultiMap(map);
    }

    /**
     * Returns {@link MultipleParameterTool} for the given multi map.
     *
     * @param multiMap A map of arguments. Key is String and value is a Collection.
     * @return A {@link MultipleParameterTool}
     */
    public static MultipleParameterTool fromMultiMap(Map<String, Collection<String>> multiMap) {
        Preconditions.checkNotNull(multiMap, "Unable to initialize from empty map");
        return new MultipleParameterTool(multiMap);
    }

    // ------------------ ParameterUtil  ------------------------
    protected final Map<String, Collection<String>> data;

    private MultipleParameterTool(Map<String, Collection<String>> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data));

        this.defaultData = new ConcurrentHashMap<>(data.size());

        this.unrequestedParameters =
                Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));

        unrequestedParameters.addAll(data.keySet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultipleParameterTool that = (MultipleParameterTool) o;
        return Objects.equals(data, that.data)
                && Objects.equals(defaultData, that.defaultData)
                && Objects.equals(unrequestedParameters, that.unrequestedParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, defaultData, unrequestedParameters);
    }

    // ------------------ Get data from the util ----------------

    /** Returns number of parameters in {@link ParameterTool}. */
    @Override
    public int getNumberOfParameters() {
        return data.size();
    }

    /**
     * Returns the String value for the given key. The value should only have one item. Use {@link
     * #getMultiParameter(String)} instead if want to get multiple values parameter. If the key does
     * not exist it will return null.
     */
    @Override
    public String get(String key) {
        addToDefaults(key, null);
        unrequestedParameters.remove(key);
        if (!data.containsKey(key)) {
            return null;
        }
        Preconditions.checkState(
                data.get(key).size() == 1, "Key %s should has only one value.", key);
        return (String) data.get(key).toArray()[0];
    }

    /** Check if value is set. */
    @Override
    public boolean has(String value) {
        addToDefaults(value, null);
        unrequestedParameters.remove(value);
        return data.containsKey(value);
    }

    /**
     * Returns the Collection of String values for the given key. If the key does not exist it will
     * return null.
     */
    public Collection<String> getMultiParameter(String key) {
        addToDefaults(key, null);
        unrequestedParameters.remove(key);
        return data.getOrDefault(key, null);
    }

    /**
     * Returns the Collection of String values for the given key. If the key does not exist it will
     * throw a {@link RuntimeException}.
     */
    public Collection<String> getMultiParameterRequired(String key) {
        addToDefaults(key, null);
        Collection<String> value = getMultiParameter(key);
        if (value == null) {
            throw new RuntimeException("No data for required key '" + key + "'");
        }
        return value;
    }

    // ------------------------- Export to different targets -------------------------

    /**
     * Return MultiMap of all the parameters processed by {@link MultipleParameterTool}.
     *
     * @return MultiMap of the {@link MultipleParameterTool}. Key is String and Value is a
     *     Collection of String.
     */
    public Map<String, Collection<String>> toMultiMap() {
        return data;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new MultipleParameterTool(this.data);
    }

    // ------------------------- Interaction with other ParameterUtils -------------------------

    /**
     * Merges two {@link MultipleParameterTool}.
     *
     * @param other Other {@link MultipleParameterTool} object
     * @return The Merged {@link MultipleParameterTool}
     */
    public MultipleParameterTool mergeWith(MultipleParameterTool other) {
        final Map<String, Collection<String>> resultData =
                new HashMap<>(data.size() + other.data.size());
        resultData.putAll(data);
        other.data.forEach(
                (key, value) -> {
                    resultData.putIfAbsent(key, new ArrayList<>());
                    resultData.get(key).addAll(value);
                });

        final MultipleParameterTool ret = new MultipleParameterTool(resultData);

        final HashSet<String> requestedParametersLeft = new HashSet<>(data.keySet());
        requestedParametersLeft.removeAll(unrequestedParameters);

        final HashSet<String> requestedParametersRight = new HashSet<>(other.data.keySet());
        requestedParametersRight.removeAll(other.unrequestedParameters);

        ret.unrequestedParameters.removeAll(requestedParametersLeft);
        ret.unrequestedParameters.removeAll(requestedParametersRight);

        return ret;
    }

    // ------------------------- ExecutionConfig.UserConfig interface -------------------------

    @Override
    public Map<String, String> toMap() {
        return getFlatMapOfData(data);
    }

    /**
     * Get the flat map of the multiple map data. If the key have multiple values, only the last one
     * will be used. This is also the current behavior when multiple parameters is specified for
     * {@link ParameterTool}.
     *
     * @param data multiple map of data.
     * @return flat map of data.
     */
    private static Map<String, String> getFlatMapOfData(Map<String, Collection<String>> data) {
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

    // ------------------------- Serialization ---------------------------------------------

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        defaultData = new ConcurrentHashMap<>(data.size());
        unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));
    }
}
