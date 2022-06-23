/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.gateway.api.endpoint;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.FactoryHelper;
import org.apache.flink.table.gateway.api.SqlGatewayService;

import java.util.List;
import java.util.Map;

/** Util to discover the {@link SqlGatewayEndpoint}. */
public class SqlGatewayEndpointFactoryUtils {

    private static final String GATEWAY_ENDPOINT_PREFIX = "sql-gateway.endpoint";

    public static final ConfigOption<List<String>> SQL_GATEWAY_ENDPOINT_TYPE =
            ConfigOptions.key(String.format("%s.type", GATEWAY_ENDPOINT_PREFIX))
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Specify the endpoints that are used.");

    public static SqlGatewayEndpoint createSqlGatewayEndpoint(
            String endpointIdentifier, SqlGatewayService service, Configuration configuration) {
        final SqlGatewayEndpointFactory factory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        SqlGatewayEndpointFactory.class,
                        endpointIdentifier);
        Configuration endpointConfig =
                new DelegatingConfiguration(
                        configuration,
                        String.format("%s.%s.", GATEWAY_ENDPOINT_PREFIX, endpointIdentifier));
        return factory.createSqlGatewayEndpoint(
                new DefaultSqlGatewayEndpointFactoryContext(service, endpointConfig));
    }

    /**
     * Creates a utility that helps to validate options for a {@link SqlGatewayEndpointFactory}.
     *
     * <p>Note: This utility checks for left-over options in the final step.
     */
    public static SqlGatewayEndpointFactoryHelper createSqlGatewayEndpointFactoryHelper(
            SqlGatewayEndpointFactory endpointFactory, SqlGatewayEndpointFactory.Context context) {
        return new SqlGatewayEndpointFactoryHelper(endpointFactory, context.getOptions());
    }

    // ----------------------------------------------------------------------------------------

    /**
     * Helper utility for validating all options for a {@link SqlGatewayEndpointFactory}.
     *
     * @see #createSqlGatewayEndpointFactoryHelper(SqlGatewayEndpointFactory,
     *     SqlGatewayEndpointFactory.Context)
     */
    public static class SqlGatewayEndpointFactoryHelper
            extends FactoryHelper<SqlGatewayEndpointFactory> {

        private SqlGatewayEndpointFactoryHelper(
                SqlGatewayEndpointFactory factory, Map<String, String> configOptions) {
            super(factory, configOptions);
        }
    }

    private static class DefaultSqlGatewayEndpointFactoryContext
            implements SqlGatewayEndpointFactory.Context {

        private final SqlGatewayService service;
        private final Configuration configuration;

        public DefaultSqlGatewayEndpointFactoryContext(
                SqlGatewayService service, Configuration configuration) {
            this.service = service;
            this.configuration = configuration;
        }

        @Override
        public SqlGatewayService getSqlGatewayService() {
            return service;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return configuration;
        }

        @Override
        public Map<String, String> getOptions() {
            return configuration.toMap();
        }
    }
}
