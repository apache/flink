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

package org.apache.flink.configuration;

/**
 * Utility class for creating various types of {@link ConfigOption}. This class provides static
 * methods to create {@link ConfigOption} instances with no default values.
 */
public class ConfigOptionUtils {

    /**
     * Creates a {@link ConfigOption} for Integer type.
     *
     * @param key the key for the configuration option
     * @return a {@link ConfigOption} of type Integer with no default value
     */
    public static ConfigOption<Integer> getIntegerConfigOption(String key) {
        return ConfigOptions.key(key).intType().noDefaultValue();
    }

    /**
     * Creates a {@link ConfigOption} for Long type.
     *
     * @param key the key for the configuration option
     * @return a {@link ConfigOption} of type Long with no default value
     */
    public static ConfigOption<Long> getLongConfigOption(String key) {
        return ConfigOptions.key(key).longType().noDefaultValue();
    }

    /**
     * Creates a {@link ConfigOption} for Float type.
     *
     * @param key the key for the configuration option
     * @return a {@link ConfigOption} of type Float with no default value
     */
    public static ConfigOption<Float> getFloatConfigOption(String key) {
        return ConfigOptions.key(key).floatType().noDefaultValue();
    }

    /**
     * Creates a {@link ConfigOption} for Double type.
     *
     * @param key the key for the configuration option
     * @return a {@link ConfigOption} of type Double with no default value
     */
    public static ConfigOption<Double> getDoubleConfigOption(String key) {
        return ConfigOptions.key(key).doubleType().noDefaultValue();
    }

    /**
     * Creates a {@link ConfigOption} for Boolean type.
     *
     * @param key the key for the configuration option
     * @return a {@link ConfigOption} of type Boolean with no default value
     */
    public static ConfigOption<Boolean> getBooleanConfigOption(String key) {
        return ConfigOptions.key(key).booleanType().noDefaultValue();
    }
}
