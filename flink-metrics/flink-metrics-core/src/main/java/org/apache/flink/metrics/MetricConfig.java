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

package org.apache.flink.metrics;

import org.apache.flink.annotation.Public;

import java.util.Properties;

/**
 * A properties class with added utility methods to extract primitives.
 *
 * <p>Values may be stored as strings via {@link #setProperty(String, String)} or as native Java
 * types via {@link #put(Object, Object)} (e.g., when Flink's YAML configuration parser stores
 * Integer, Long, or Boolean values directly). The getter methods handle both representations
 * transparently.
 */
@Public
public class MetricConfig extends Properties {

    public String getString(String key, String defaultValue) {
        return getProperty(key, defaultValue);
    }

    /**
     * Returns the value associated with the given key as an {@code int}.
     *
     * <p>If the value is a {@link Number}, its {@code intValue()} is returned directly. Otherwise,
     * the value's string representation is parsed via {@link Integer#parseInt(String)}.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value as an int.
     */
    public int getInteger(String key, int defaultValue) {
        final Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    /**
     * Returns the value associated with the given key as a {@code long}.
     *
     * <p>If the value is a {@link Number}, its {@code longValue()} is returned directly. Otherwise,
     * the value's string representation is parsed via {@link Long#parseLong(String)}.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value as a long.
     */
    public long getLong(String key, long defaultValue) {
        final Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    /**
     * Returns the value associated with the given key as a {@code float}.
     *
     * <p>If the value is a {@link Number}, its {@code floatValue()} is returned directly.
     * Otherwise, the value's string representation is parsed via {@link Float#parseFloat(String)}.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value as a float.
     */
    public float getFloat(String key, float defaultValue) {
        final Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString());
    }

    /**
     * Returns the value associated with the given key as a {@code double}.
     *
     * <p>If the value is a {@link Number}, its {@code doubleValue()} is returned directly.
     * Otherwise, the value's string representation is parsed via {@link
     * Double#parseDouble(String)}.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value as a double.
     */
    public double getDouble(String key, double defaultValue) {
        final Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    /**
     * Returns the value associated with the given key as a {@code boolean}.
     *
     * <p>If the value is a {@link Boolean}, it is returned directly. Otherwise, the value's string
     * representation is parsed via {@link Boolean#parseBoolean(String)}.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value as a boolean.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        final Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }
}
