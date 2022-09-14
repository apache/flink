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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder for building the unmodifiable {@link Configuration} instance. Providing the common
 * validate logic for Pulsar source & sink.
 */
@Internal
public final class PulsarConfigBuilder {

    private final Configuration configuration = new Configuration();

    /** Validate if the config has a existed option. */
    public <T> boolean contains(ConfigOption<T> option) {
        return configuration.contains(option);
    }

    /**
     * Get an option-related config value. We would return the default config value defined in the
     * option if no value existed instead.
     *
     * @param key Config option instance.
     */
    public <T> T get(ConfigOption<T> key) {
        return configuration.get(key);
    }

    /**
     * Add a config option with a not null value. The config key shouldn't be duplicated.
     *
     * @param option Config option instance, contains key & type definition.
     * @param value The config value which shouldn't be null.
     */
    public <T> void set(ConfigOption<T> option, T value) {
        checkNotNull(option);
        checkNotNull(value);

        if (configuration.contains(option)) {
            T oldValue = configuration.get(option);
            checkArgument(
                    Objects.equals(oldValue, value),
                    "This option %s has been set to value %s.",
                    option.key(),
                    oldValue);
        } else {
            configuration.set(option, value);
        }
    }

    /**
     * Fill in a set of configs which shouldn't be duplicated.
     *
     * @param config A set of configs.
     */
    public void set(Configuration config) {
        Map<String, String> existedConfigs = configuration.toMap();
        List<String> duplicatedKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            if (existedConfigs.containsKey(key)) {
                String value2 = existedConfigs.get(key);
                if (!Objects.equals(value2, entry.getValue())) {
                    duplicatedKeys.add(key);
                }
            }
        }
        checkArgument(
                duplicatedKeys.isEmpty(),
                "Invalid configuration, these keys %s are already exist with different config value.",
                duplicatedKeys);
        configuration.addAll(config);
    }

    /**
     * Fill in a set of config properties which shouldn't be duplicated.
     *
     * @param properties A config which could be string type.
     */
    public void set(Properties properties) {
        properties.keySet().stream()
                .map(String::valueOf)
                .forEach(
                        key -> {
                            ConfigOption<String> option =
                                    ConfigOptions.key(key).stringType().noDefaultValue();
                            Object value = properties.get(key);

                            if (value != null) {
                                set(option, value.toString());
                            }
                        });
    }

    /**
     * Override the option with the given value. It will not check the existed option comparing to
     * {@link #set(ConfigOption, Object)}.
     */
    public <T> void override(ConfigOption<T> option, T value) {
        checkNotNull(option);
        checkNotNull(value);

        configuration.set(option, value);
    }

    /** Validate the current config instance and return a unmodifiable configuration. */
    public <T extends PulsarConfiguration> T build(
            PulsarConfigValidator validator, Function<Configuration, T> constructor) {
        validator.validate(configuration);
        return constructor.apply(configuration);
    }
}
