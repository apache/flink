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

import java.util.Properties;

/** A properties class with added utility method to extract primitives. */
public class MetricConfig extends Properties {

    public String getString(String key, String defaultValue) {
        return getProperty(key, defaultValue);
    }

    /**
     * Searches for the property with the specified key in this property list. If the key is not
     * found in this property list, the default property list, and its defaults, recursively, are
     * then checked. The method returns the default value argument if the property is not found.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value parsed as an int.
     */
    public int getInteger(String key, int defaultValue) {
        String argument = getProperty(key, null);
        return argument == null ? defaultValue : Integer.parseInt(argument);
    }

    /**
     * Searches for the property with the specified key in this property list. If the key is not
     * found in this property list, the default property list, and its defaults, recursively, are
     * then checked. The method returns the default value argument if the property is not found.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value parsed as a long.
     */
    public long getLong(String key, long defaultValue) {
        String argument = getProperty(key, null);
        return argument == null ? defaultValue : Long.parseLong(argument);
    }

    /**
     * Searches for the property with the specified key in this property list. If the key is not
     * found in this property list, the default property list, and its defaults, recursively, are
     * then checked. The method returns the default value argument if the property is not found.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value parsed as a float.
     */
    public float getFloat(String key, float defaultValue) {
        String argument = getProperty(key, null);
        return argument == null ? defaultValue : Float.parseFloat(argument);
    }

    /**
     * Searches for the property with the specified key in this property list. If the key is not
     * found in this property list, the default property list, and its defaults, recursively, are
     * then checked. The method returns the default value argument if the property is not found.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value parsed as a double.
     */
    public double getDouble(String key, double defaultValue) {
        String argument = getProperty(key, null);
        return argument == null ? defaultValue : Double.parseDouble(argument);
    }

    /**
     * Searches for the property with the specified key in this property list. If the key is not
     * found in this property list, the default property list, and its defaults, recursively, are
     * then checked. The method returns the default value argument if the property is not found.
     *
     * @param key the hashtable key.
     * @param defaultValue a default value.
     * @return the value in this property list with the specified key value parsed as a boolean.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        String argument = getProperty(key, null);
        return argument == null ? defaultValue : Boolean.parseBoolean(argument);
    }
}
