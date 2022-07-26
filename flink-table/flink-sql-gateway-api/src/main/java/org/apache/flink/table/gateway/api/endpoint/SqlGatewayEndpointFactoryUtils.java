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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.FactoryHelper;
import org.apache.flink.table.gateway.api.SqlGatewayService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;
import static org.apache.flink.table.factories.FactoryUtil.SQL_GATEWAY_ENDPOINT_TYPE;

/** Util to discover the {@link SqlGatewayEndpoint}. */
@PublicEvolving
public class SqlGatewayEndpointFactoryUtils {

    public static final String GATEWAY_ENDPOINT_PREFIX = "sql-gateway.endpoint";

    /**
     * Attempts to discover the appropriate endpoint factory and creates the instance of the
     * endpoints.
     */
    public static List<SqlGatewayEndpoint> createSqlGatewayEndpoint(
            SqlGatewayService service, Configuration configuration) {
        List<String> identifiers = configuration.get(SQL_GATEWAY_ENDPOINT_TYPE);

        if (identifiers == null || identifiers.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Endpoint options do not contain an option key '%s' for discovering an endpoint.",
                            SQL_GATEWAY_ENDPOINT_TYPE.key()));
        }
        validateSpecifiedEndpointsAreUnique(identifiers);

        List<SqlGatewayEndpoint> endpoints = new ArrayList<>();
        for (String identifier : identifiers) {
            final SqlGatewayEndpointFactory factory =
                    FactoryUtil.discoverFactory(
                            Thread.currentThread().getContextClassLoader(),
                            SqlGatewayEndpointFactory.class,
                            identifier);

            endpoints.add(
                    factory.createSqlGatewayEndpoint(
                            new DefaultEndpointFactoryContext(
                                    service,
                                    configuration,
                                    getEndpointConfig(configuration, identifier))));
        }
        return endpoints;
    }

    public static Map<String, String> getEndpointConfig(
            Configuration flinkConf, String identifier) {
        return new DelegatingConfiguration(flinkConf, getSqlGatewayOptionPrefix(identifier))
                .toMap();
    }

    public static String getSqlGatewayOptionPrefix(String identifier) {
        return String.format("%s.%s.", GATEWAY_ENDPOINT_PREFIX, identifier);
    }

    /**
     * Creates a utility that helps to validate options for a {@link SqlGatewayEndpointFactory}.
     *
     * <p>Note: This utility checks for left-over options in the final step.
     */
    public static EndpointFactoryHelper createEndpointFactoryHelper(
            SqlGatewayEndpointFactory endpointFactory, SqlGatewayEndpointFactory.Context context) {
        return new EndpointFactoryHelper(endpointFactory, context.getEndpointOptions());
    }

    // ----------------------------------------------------------------------------------------

    /**
     * Helper utility for validating all options for a {@link SqlGatewayEndpointFactory}.
     *
     * @see #createEndpointFactoryHelper(SqlGatewayEndpointFactory,
     *     SqlGatewayEndpointFactory.Context)
     */
    @PublicEvolving
    public static class EndpointFactoryHelper extends FactoryHelper<SqlGatewayEndpointFactory> {

        private EndpointFactoryHelper(
                SqlGatewayEndpointFactory factory, Map<String, String> configOptions) {
            super(factory, configOptions, PROPERTY_VERSION);
        }
    }

    /** The default context of {@link SqlGatewayEndpointFactory}. */
    @Internal
    public static class DefaultEndpointFactoryContext implements SqlGatewayEndpointFactory.Context {

        private final SqlGatewayService service;
        private final Configuration flinkConfiguration;
        private final Map<String, String> endpointConfig;

        public DefaultEndpointFactoryContext(
                SqlGatewayService service,
                Configuration flinkConfiguration,
                Map<String, String> endpointConfig) {
            this.service = service;
            this.flinkConfiguration = flinkConfiguration;
            this.endpointConfig = endpointConfig;
        }

        @Override
        public SqlGatewayService getSqlGatewayService() {
            return service;
        }

        @Override
        public ReadableConfig getFlinkConfiguration() {
            return flinkConfiguration;
        }

        @Override
        public Map<String, String> getEndpointOptions() {
            return endpointConfig;
        }
    }

    private static void validateSpecifiedEndpointsAreUnique(List<String> identifiers) {
        Set<String> uniqueIdentifiers = new HashSet<>();

        for (String identifier : identifiers) {
            if (uniqueIdentifiers.contains(identifier)) {
                throw new ValidationException(
                        String.format(
                                "Get the duplicate endpoint identifier '%s' for the option '%s'. "
                                        + "Please keep the specified endpoint identifier unique.",
                                identifier, SQL_GATEWAY_ENDPOINT_TYPE.key()));
            }
            uniqueIdentifiers.add(identifier);
        }
    }
}
