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

package org.apache.flink.connector.base.table.sink.options;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncSinkConnectorOptions;
import org.apache.flink.connector.base.table.options.ConfigurationValidator;

import java.util.Properties;
import java.util.function.Predicate;

import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS;

/** Class for validating options in {@link AsyncSinkConnectorOptions}. */
@PublicEvolving
public class AsyncSinkConfigurationValidator implements ConfigurationValidator {
    protected final ReadableConfig tableOptions;

    public AsyncSinkConfigurationValidator(ReadableConfig tableOptions) {
        this.tableOptions = tableOptions;
    }

    @Override
    public Properties getValidatedConfigurations() {
        Properties asyncProps = new Properties();

        validatePositiveIntegerValue(MAX_BATCH_SIZE);
        tableOptions
                .getOptional(MAX_BATCH_SIZE)
                .ifPresent(val -> asyncProps.put(MAX_BATCH_SIZE.key(), val));
        validatePositiveLongValue(FLUSH_BUFFER_SIZE);
        tableOptions
                .getOptional(FLUSH_BUFFER_SIZE)
                .ifPresent(val -> asyncProps.put(FLUSH_BUFFER_SIZE.key(), val));
        validatePositiveIntegerValue(MAX_BUFFERED_REQUESTS);
        tableOptions
                .getOptional(MAX_BUFFERED_REQUESTS)
                .ifPresent(val -> asyncProps.put(MAX_BUFFERED_REQUESTS.key(), val));
        validatePositiveIntegerValue(MAX_IN_FLIGHT_REQUESTS);
        tableOptions
                .getOptional(MAX_IN_FLIGHT_REQUESTS)
                .ifPresent(val -> asyncProps.put(MAX_IN_FLIGHT_REQUESTS.key(), val));
        validatePositiveLongValue(FLUSH_BUFFER_TIMEOUT);
        tableOptions
                .getOptional(FLUSH_BUFFER_TIMEOUT)
                .ifPresent(val -> asyncProps.put(FLUSH_BUFFER_TIMEOUT.key(), val));

        return asyncProps;
    }

    private void validatePositiveIntegerValue(ConfigOption<Integer> option) {
        validateOptionValue(
                option,
                intVal -> intVal > 0,
                String.format("Invalid option %s. Must be a positive integer.", option.key()));
    }

    private void validatePositiveLongValue(ConfigOption<Long> option) {
        validateOptionValue(
                option,
                longVal -> longVal > 0L,
                String.format("Invalid option %s. Must be a positive integer.", option.key()));
    }

    private <T> void validateOptionValue(
            ConfigOption<T> option, Predicate<T> valueValidator, String errorMessage) {
        tableOptions
                .getOptional(option)
                .ifPresent(
                        val -> {
                            if (!valueValidator.test(val)) {
                                throw new IllegalArgumentException(errorMessage);
                            }
                        });
    }
}
