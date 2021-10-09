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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Optional;

/**
 * Configuration view that combines the API specific table configuration and the executor
 * configuration. The table configuration has precedence.
 */
@Internal
public final class PlannerConfiguration implements ReadableConfig {

    private final ReadableConfig tableConfig;

    private final ReadableConfig executorConfig;

    PlannerConfiguration(ReadableConfig tableConfig, ReadableConfig executorConfig) {
        this.tableConfig = tableConfig;
        this.executorConfig = executorConfig;
    }

    @Override
    public <T> T get(ConfigOption<T> option) {
        return tableConfig.getOptional(option).orElseGet(() -> executorConfig.get(option));
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        final Optional<T> tableValue = tableConfig.getOptional(option);
        if (tableValue.isPresent()) {
            return tableValue;
        }
        return executorConfig.getOptional(option);
    }
}
