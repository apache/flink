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

package org.apache.flink.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.python.util.PythonDependencyUtils;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.util.Preconditions;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.python.PythonOptions.PYTHON_LOOPBACK_SERVER_ADDRESS;

/** Configurations for the Python job which are used at run time. */
@Internal
public class PythonConfig implements ReadableConfig {

    private static final List<ConfigOption<?>> PYTHON_CONFIG_OPTIONS;

    static {
        PYTHON_CONFIG_OPTIONS =
                new ArrayList<>(ConfigUtils.getAllConfigOptions(PythonOptions.class));
    }

    /**
     * Configuration adopted from the outer layer, e.g. flink-conf.yaml, command line arguments,
     * TableConfig, etc.
     */
    private final ReadableConfig configuration;

    /**
     * Configuration generated in the dependency management mechanisms. See {@link
     * PythonDependencyUtils.PythonDependencyManager} for more details.
     */
    private final ReadableConfig pythonDependencyConfiguration;

    public PythonConfig(
            ReadableConfig configuration, ReadableConfig pythonDependencyConfiguration) {
        this.configuration = Preconditions.checkNotNull(configuration);
        this.pythonDependencyConfiguration =
                Preconditions.checkNotNull(pythonDependencyConfiguration);
    }

    @Override
    public <T> T get(ConfigOption<T> option) {
        return pythonDependencyConfiguration
                .getOptional(option)
                .orElseGet(() -> configuration.get(option));
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        final Optional<T> value = pythonDependencyConfiguration.getOptional(option);
        if (value.isPresent()) {
            return value;
        }
        return configuration.getOptional(option);
    }

    public Configuration toConfiguration() {
        final Configuration config = new Configuration();
        PYTHON_CONFIG_OPTIONS.forEach(
                option ->
                        getOptional((ConfigOption) option)
                                .ifPresent(v -> config.set((ConfigOption) option, v)));

        // prepare the job options
        Map<String, String> jobOptions = config.get(PythonOptions.PYTHON_JOB_OPTIONS);
        if (jobOptions == null) {
            jobOptions = new HashMap<>();
            config.set(PythonOptions.PYTHON_JOB_OPTIONS, jobOptions);
        }
        jobOptions.put("TABLE_LOCAL_TIME_ZONE", getLocalTimeZone(configuration).getId());
        if (config.contains(PYTHON_LOOPBACK_SERVER_ADDRESS)) {
            jobOptions.put(
                    "PYTHON_LOOPBACK_SERVER_ADDRESS", config.get(PYTHON_LOOPBACK_SERVER_ADDRESS));
        }

        return config;
    }

    /**
     * Returns the current session time zone id. It is used when converting to/from {@code TIMESTAMP
     * WITH LOCAL TIME ZONE}.
     *
     * @see org.apache.flink.table.types.logical.LocalZonedTimestampType
     */
    private static ZoneId getLocalTimeZone(ReadableConfig config) {
        String zone = config.get(TableConfigOptions.LOCAL_TIME_ZONE);
        return TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                ? ZoneId.systemDefault()
                : ZoneId.of(zone);
    }
}
