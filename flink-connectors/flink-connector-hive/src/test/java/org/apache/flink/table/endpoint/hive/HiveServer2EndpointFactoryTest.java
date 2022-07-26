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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils;
import org.apache.flink.table.gateway.api.utils.MockedSqlGatewayService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.SQL_GATEWAY_ENDPOINT_TYPE;
import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.GATEWAY_ENDPOINT_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HiveServer2EndpointFactory}. */
class HiveServer2EndpointFactoryTest {

    private final MockedSqlGatewayService service = new MockedSqlGatewayService();

    private final int port = 9091;
    private final int minWorkerThreads = 100;
    private final int maxWorkerThreads = 200;
    private final Duration workerAliveDuration = Duration.ofSeconds(10);
    private final long maxMessageSize = 1024L;
    private final Duration loginTimeout = Duration.ofSeconds(10);
    private final Duration backOffSlotLength = Duration.ofMillis(300);

    private final String catalogName = "test-hive";
    private final String hiveConfPath = "/path/to/conf";
    private final String defaultDatabase = "test-db";
    private final String moduleName = "test-module";

    @Test
    public void testCreateHiveServer2Endpoint() throws Exception {
        assertThat(
                        SqlGatewayEndpointFactoryUtils.createSqlGatewayEndpoint(
                                service, Configuration.fromMap(getDefaultConfig())))
                .isEqualTo(
                        Collections.singletonList(
                                new HiveServer2Endpoint(
                                        service,
                                        InetAddress.getByName("localhost"),
                                        port,
                                        maxMessageSize,
                                        (int) loginTimeout.toMillis(),
                                        (int) backOffSlotLength.toMillis(),
                                        minWorkerThreads,
                                        maxWorkerThreads,
                                        workerAliveDuration,
                                        catalogName,
                                        hiveConfPath,
                                        defaultDatabase,
                                        moduleName)));
    }

    @ParameterizedTest
    @MethodSource("getIllegalArgumentTestSpecs")
    public void testCreateHiveServer2EndpointWithIllegalArgument(TestSpec<?> spec) {
        assertThatThrownBy(
                        () ->
                                SqlGatewayEndpointFactoryUtils.createSqlGatewayEndpoint(
                                        service,
                                        Configuration.fromMap(
                                                getModifiedConfig(
                                                        config ->
                                                                setEndpointOption(
                                                                        config,
                                                                        spec.option,
                                                                        spec.value)))))
                .satisfies(FlinkAssertions.anyCauseMatches(spec.exceptionMessage));
    }

    // --------------------------------------------------------------------------------------------

    private static List<TestSpec<?>> getIllegalArgumentTestSpecs() {
        return Arrays.asList(
                new TestSpec<>(
                        THRIFT_WORKER_THREADS_MIN,
                        -1,
                        "The specified min thrift worker thread number is -1, which should be larger than 0."),
                new TestSpec<>(
                        THRIFT_WORKER_THREADS_MAX,
                        0,
                        "The specified max thrift worker thread number is 0, which should be larger than 0."),
                new TestSpec<>(
                        THRIFT_PORT,
                        1008668001,
                        "The specified port is 1008668001, which should range from 0 to 65535."),
                new TestSpec<>(
                        THRIFT_LOGIN_TIMEOUT,
                        "9223372036854775807 ms",
                        "The specified login timeout should range from 0 ms to 2147483647 ms but the specified value is 9223372036854775807 ms."),
                new TestSpec<>(
                        THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH,
                        "9223372036854775807 ms",
                        "The specified binary exponential backoff slot time should range from 0 ms to 2147483647 ms but the specified value is 9223372036854775807 ms."));
    }

    // --------------------------------------------------------------------------------------------

    private static class TestSpec<T> {

        private final ConfigOption<?> option;
        private final T value;
        private final String exceptionMessage;

        public TestSpec(ConfigOption<?> option, T value, String exceptionMessage) {
            this.option = option;
            this.value = value;
            this.exceptionMessage = exceptionMessage;
        }

        @Override
        public String toString() {
            return "TestSpec{option=" + option.key() + '}';
        }
    }

    private Map<String, String> getModifiedConfig(Consumer<Map<String, String>> consumer) {
        Map<String, String> config = getDefaultConfig();
        consumer.accept(config);
        return config;
    }

    private Map<String, String> getDefaultConfig() {
        Map<String, String> config = new HashMap<>();

        config.put(SQL_GATEWAY_ENDPOINT_TYPE.key(), IDENTIFIER);

        setEndpointOption(config, THRIFT_HOST, "localhost");
        setEndpointOption(config, THRIFT_PORT, port);
        setEndpointOption(config, THRIFT_WORKER_THREADS_MIN, minWorkerThreads);
        setEndpointOption(config, THRIFT_WORKER_THREADS_MAX, maxWorkerThreads);
        setEndpointOption(config, THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH, "300ms");
        setEndpointOption(config, THRIFT_WORKER_KEEPALIVE_TIME, "10s");
        setEndpointOption(config, THRIFT_MAX_MESSAGE_SIZE, maxMessageSize);
        setEndpointOption(config, THRIFT_LOGIN_TIMEOUT, "10s");

        setEndpointOption(config, CATALOG_NAME, catalogName);
        setEndpointOption(config, CATALOG_HIVE_CONF_DIR, hiveConfPath);
        setEndpointOption(config, CATALOG_DEFAULT_DATABASE, defaultDatabase);

        setEndpointOption(config, MODULE_NAME, moduleName);
        return config;
    }

    private <T> void setEndpointOption(
            Map<String, String> config, ConfigOption<?> option, T value) {
        String prefix = String.format("%s.%s.", GATEWAY_ENDPOINT_PREFIX, IDENTIFIER);
        config.put(prefix + option.key(), String.valueOf(value));
    }
}
