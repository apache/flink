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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncSinkConnectorOptions;
import org.apache.flink.connector.base.table.options.ConfigurationValidator;

import java.util.Properties;

/** Base class for validating options in {@link AsyncSinkConnectorOptions}. */
@PublicEvolving
public abstract class AsyncSinkConfigurationValidator implements ConfigurationValidator {
    protected final ReadableConfig tableOptions;

    public AsyncSinkConfigurationValidator(ReadableConfig tableOptions) {
        this.tableOptions = tableOptions;
    }

    @Override
    public Properties getValidatedConfigurations() {
        Properties asyncProps = new Properties();

        if (tableOptions.getOptional(AsyncSinkConnectorOptions.MAX_BATCH_SIZE).isPresent()) {
            if (tableOptions.getOptional(AsyncSinkConnectorOptions.MAX_BATCH_SIZE).get() > 0) {
                asyncProps.put(
                        AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key(),
                        tableOptions.getOptional(AsyncSinkConnectorOptions.MAX_BATCH_SIZE).get());
            } else {
                throw new IllegalArgumentException(
                        "Invalid option batch size. Must be a positive integer");
            }
        }

        if (tableOptions.getOptional(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE).isPresent()) {
            if (tableOptions.getOptional(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE).get() > 0) {
                asyncProps.put(
                        AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE.key(),
                        tableOptions
                                .getOptional(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE)
                                .get());
            } else {
                throw new IllegalArgumentException(
                        "Invalid option flush buffer size. Must be a positive integer");
            }
        }

        if (tableOptions.getOptional(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS).isPresent()) {
            if (tableOptions.getOptional(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS).get()
                    > 0) {
                asyncProps.put(
                        AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS.key(),
                        tableOptions
                                .getOptional(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS)
                                .get());
            } else {
                throw new IllegalArgumentException(
                        "Invalid option maximum buffered requests. Must be a positive integer");
            }
        }
        if (tableOptions
                .getOptional(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS)
                .isPresent()) {
            if (tableOptions.getOptional(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS).get()
                    > 0) {
                asyncProps.put(
                        AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key(),
                        tableOptions
                                .getOptional(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS)
                                .get());
            } else {
                throw new IllegalArgumentException(
                        "Invalid option maximum inflight requests. Must be a positive integer");
            }
        }

        if (tableOptions.getOptional(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT).isPresent()) {
            if (tableOptions.getOptional(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT).get()
                    > 0) {
                asyncProps.put(
                        AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key(),
                        tableOptions
                                .getOptional(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT)
                                .get());
            } else {
                throw new IllegalArgumentException(
                        "Invalid option buffer timeout. Must be a positive integer");
            }
        }

        return asyncProps;
    }
}
