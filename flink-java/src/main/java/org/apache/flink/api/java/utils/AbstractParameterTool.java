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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This class provides common utility methods of {@link ParameterTool} and {@link
 * MultipleParameterTool}.
 */
@Public
public abstract class AbstractParameterTool extends ExecutionConfig.GlobalJobParameters
        implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    protected static final String NO_VALUE_KEY = "__NO_VALUE_KEY";
    protected static final String DEFAULT_UNDEFINED = "<undefined>";

    // ------------------ ParameterUtil  ------------------------

    // data which is only used on the client and does not need to be transmitted
    protected transient Map<String, String> defaultData;
    protected transient Set<String> unrequestedParameters;

    /**
     * Returns the set of parameter names which have not been requested with {@link #has(String)} or
     * one of the {@code get} methods. Access to the map returned by {@link #toMap()} is not
     * tracked.
     */
    @PublicEvolving
    public Set<String> getUnrequestedParameters() {
        return Collections.unmodifiableSet(unrequestedParameters);
    }

    // ------------------ Get data from the util ----------------

    /** Returns number of parameters in {@link AbstractParameterTool}. */
    protected abstract int getNumberOfParameters();

    /**
     * Returns the String value for the given key. If the key does not exist it will return null.
     */
    protected abstract String get(String key);

    /** Check if value is set. */
    public abstract boolean has(String value);

    /**
     * Returns the String value for the given key. If the key does not exist it will throw a {@link
     * RuntimeException}.
     */
    public String getRequired(String key) {
        addToDefaults(key, null);
        String value = get(key);
        if (value == null) {
            throw new RuntimeException("No data for required key '" + key + "'");
        }
        return value;
    }

    /**
     * Returns the String value for the given key. If the key does not exist it will return the
     * given default value.
     */
    public String get(String key, String defaultValue) {
        addToDefaults(key, defaultValue);
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return value;
        }
    }

    // -------------- Integer

    /**
     * Returns the Integer value for the given key. The method fails if the key does not exist or
     * the value is not an Integer.
     */
    public int getInt(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Integer.parseInt(value);
    }

    /**
     * Returns the Integer value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not an Integer.
     */
    public int getInt(String key, int defaultValue) {
        addToDefaults(key, Integer.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    // -------------- LONG

    /** Returns the Long value for the given key. The method fails if the key does not exist. */
    public long getLong(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Long.parseLong(value);
    }

    /**
     * Returns the Long value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not a Long.
     */
    public long getLong(String key, long defaultValue) {
        addToDefaults(key, Long.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    // -------------- FLOAT

    /** Returns the Float value for the given key. The method fails if the key does not exist. */
    public float getFloat(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Float.valueOf(value);
    }

    /**
     * Returns the Float value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not a Float.
     */
    public float getFloat(String key, float defaultValue) {
        addToDefaults(key, Float.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Float.valueOf(value);
        }
    }

    // -------------- DOUBLE

    /** Returns the Double value for the given key. The method fails if the key does not exist. */
    public double getDouble(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Double.valueOf(value);
    }

    /**
     * Returns the Double value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not a Double.
     */
    public double getDouble(String key, double defaultValue) {
        addToDefaults(key, Double.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Double.valueOf(value);
        }
    }

    // -------------- BOOLEAN

    /** Returns the Boolean value for the given key. The method fails if the key does not exist. */
    public boolean getBoolean(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Boolean.valueOf(value);
    }

    /**
     * Returns the Boolean value for the given key. If the key does not exists it will return the
     * default value given. The method returns whether the string of the value is "true" ignoring
     * cases.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        addToDefaults(key, Boolean.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Boolean.valueOf(value);
        }
    }

    // -------------- SHORT

    /** Returns the Short value for the given key. The method fails if the key does not exist. */
    public short getShort(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Short.valueOf(value);
    }

    /**
     * Returns the Short value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not a Short.
     */
    public short getShort(String key, short defaultValue) {
        addToDefaults(key, Short.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Short.valueOf(value);
        }
    }

    // -------------- BYTE

    /** Returns the Byte value for the given key. The method fails if the key does not exist. */
    public byte getByte(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Byte.valueOf(value);
    }

    /**
     * Returns the Byte value for the given key. If the key does not exists it will return the
     * default value given. The method fails if the value is not a Byte.
     */
    public byte getByte(String key, byte defaultValue) {
        addToDefaults(key, Byte.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Byte.valueOf(value);
        }
    }

    // --------------- Internals

    protected void addToDefaults(String key, String value) {
        final String currentValue = defaultData.get(key);
        if (currentValue == null) {
            if (value == null) {
                value = DEFAULT_UNDEFINED;
            }
            defaultData.put(key, value);
        } else {
            // there is already an entry for this key. Check if the value is the undefined
            if (currentValue.equals(DEFAULT_UNDEFINED) && value != null) {
                // update key with better default value
                defaultData.put(key, value);
            }
        }
    }

    // ------------------------- Export to different targets -------------------------

    @Override
    protected abstract Object clone() throws CloneNotSupportedException;

    // ------------------------- ExecutionConfig.UserConfig interface -------------------------

    @Override
    public abstract Map<String, String> toMap();
}
