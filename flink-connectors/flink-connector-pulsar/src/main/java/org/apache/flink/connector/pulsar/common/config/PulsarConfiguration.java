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

package org.apache.flink.connector.pulsar.common.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

/**
 * An unmodifiable {@link Configuration} for Pulsar. We provide extra methods for building the
 * different Pulsar client instance.
 */
public abstract class PulsarConfiguration extends UnmodifiableConfiguration {
    private static final long serialVersionUID = 3050894147145572345L;

    /**
     * Creates a new PulsarConfiguration, which holds a copy of the given configuration that can't
     * be altered.
     *
     * @param config The configuration with the original contents.
     */
    protected PulsarConfiguration(Configuration config) {
        super(config);
    }

    /**
     * Get the option value by a prefix. We would return an empty map if the option doesn't exist.
     */
    public Map<String, String> getProperties(ConfigOption<Map<String, String>> option) {
        Map<String, String> properties = new HashMap<>();
        if (contains(option)) {
            Map<String, String> map = get(option);
            properties.putAll(map);
        }

        // Filter the sub config option. These options could be provided by SQL.
        String prefix = option.key() + ".";
        List<String> keys =
                keySet().stream()
                        .filter(key -> key.startsWith(prefix) && key.length() > prefix.length())
                        .collect(toList());

        // Put these config options' value into return result.
        for (String key : keys) {
            ConfigOption<String> o = ConfigOptions.key(key).stringType().noDefaultValue();
            String value = get(o);
            properties.put(key.substring(prefix.length()), value);
        }

        return properties;
    }

    /** Get an option value from the given config, convert it into the a new value instance. */
    public <F, T> T get(ConfigOption<F> option, Function<F, T> convertor) {
        F value = get(option);
        if (value != null) {
            return convertor.apply(value);
        } else {
            return null;
        }
    }

    /** Set the config option's value to a given builder. */
    public <T> void useOption(ConfigOption<T> option, Consumer<T> setter) {
        useOption(option, identity(), setter);
    }

    /**
     * Query the config option's value, convert it into a required type, set it to a given builder.
     */
    public <T, V> void useOption(
            ConfigOption<T> option, Function<T, V> convertor, Consumer<V> setter) {
        if (contains(option) || option.hasDefaultValue()) {
            V value = get(option, convertor);
            setter.accept(value);
        }
    }
}
