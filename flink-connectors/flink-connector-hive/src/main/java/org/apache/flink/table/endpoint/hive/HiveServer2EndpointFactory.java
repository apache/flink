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

package org.apache.flink.table.endpoint.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpoint;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactory;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.CATALOG_HIVE_CONF_DIR;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.CATALOG_NAME;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.MODULE_NAME;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_HOST;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_LOGIN_TIMEOUT;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_MAX_MESSAGE_SIZE;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_PORT;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_WORKER_KEEPALIVE_TIME;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_WORKER_THREADS_MAX;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_WORKER_THREADS_MIN;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Endpoint Factory to create {@code HiveServer2Endpoint}. */
public class HiveServer2EndpointFactory implements SqlGatewayEndpointFactory {

    static final String IDENTIFIER = "hiveserver2";

    @Override
    public SqlGatewayEndpoint createSqlGatewayEndpoint(Context context) {
        SqlGatewayEndpointFactoryUtils.EndpointFactoryHelper helper =
                SqlGatewayEndpointFactoryUtils.createEndpointFactoryHelper(this, context);
        ReadableConfig configuration = helper.getOptions();
        validate(configuration);
        return new HiveServer2Endpoint(
                context.getSqlGatewayService(),
                getInetSocketAddress(configuration),
                checkNotNull(configuration.get(THRIFT_MAX_MESSAGE_SIZE)),
                (int) configuration.get(THRIFT_LOGIN_TIMEOUT).toMillis(),
                (int) configuration.get(THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH).toMillis(),
                configuration.get(THRIFT_WORKER_THREADS_MIN),
                configuration.get(THRIFT_WORKER_THREADS_MAX),
                configuration.get(THRIFT_WORKER_KEEPALIVE_TIME),
                configuration.get(CATALOG_NAME),
                HiveCatalog.createHiveConf(configuration.get(CATALOG_HIVE_CONF_DIR), null),
                configuration.get(CATALOG_DEFAULT_DATABASE),
                configuration.get(MODULE_NAME));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(CATALOG_HIVE_CONF_DIR);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        THRIFT_HOST,
                        THRIFT_PORT,
                        THRIFT_MAX_MESSAGE_SIZE,
                        THRIFT_LOGIN_TIMEOUT,
                        THRIFT_WORKER_THREADS_MIN,
                        THRIFT_WORKER_THREADS_MAX,
                        THRIFT_WORKER_KEEPALIVE_TIME,
                        CATALOG_NAME,
                        CATALOG_DEFAULT_DATABASE,
                        MODULE_NAME));
    }

    private static InetSocketAddress getInetSocketAddress(ReadableConfig configuration) {
        Optional<String> host = configuration.getOptional(THRIFT_HOST);
        if (host.isPresent()) {
            return new InetSocketAddress(
                    configuration.get(THRIFT_HOST), configuration.get(THRIFT_PORT));
        } else {
            return new InetSocketAddress(configuration.get(THRIFT_PORT));
        }
    }

    private static void validate(ReadableConfig configuration) {
        int port = configuration.get(THRIFT_PORT);
        if (port < 0 || port > 65535) {
            throw new ValidationException(
                    String.format(
                            "The specified port is %s, which should range from 0 to 65535.", port));
        }
        if (configuration.get(THRIFT_WORKER_THREADS_MIN) <= 0) {
            throw new ValidationException(
                    String.format(
                            "The specified min thrift worker thread number is %s, which should be larger than 0.",
                            configuration.get(THRIFT_WORKER_THREADS_MIN)));
        }
        if (configuration.get(THRIFT_WORKER_THREADS_MAX) <= 0) {
            throw new ValidationException(
                    String.format(
                            "The specified max thrift worker thread number is %s, which should be larger than 0.",
                            configuration.get(THRIFT_WORKER_THREADS_MAX)));
        }
        if (configuration.get(THRIFT_LOGIN_TIMEOUT).toMillis() > Integer.MAX_VALUE
                || configuration.get(THRIFT_LOGIN_TIMEOUT).toMillis() < 0) {
            throw new ValidationException(
                    String.format(
                            "The specified login timeout should range from 0 ms to %s ms but the specified value is %s ms.",
                            Integer.MAX_VALUE, configuration.get(THRIFT_LOGIN_TIMEOUT).toMillis()));
        }
        if (configuration.get(THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH).toMillis() > Integer.MAX_VALUE
                || configuration.get(THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH).toMillis() < 0) {
            throw new ValidationException(
                    String.format(
                            "The specified binary exponential backoff slot time should range from 0 ms to %s ms but the specified value is %s ms.",
                            Integer.MAX_VALUE,
                            configuration.get(THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH).toMillis()));
        }
    }
}
