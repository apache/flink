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

package org.apache.flink.table.gateway.api.endpoint;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.gateway.api.SqlGatewayService;

import java.util.Map;

/**
 * A factory for creating Endpoint from Configuration. This factory is used with Java's Service
 * Provider Interfaces (SPI) for discovery.
 */
@PublicEvolving
public interface SqlGatewayEndpointFactory extends Factory {

    /**
     * Creates an endpoint from the given context and endpoint options.
     *
     * <p>The endpoint options have been projected to top-level options (e.g. from {@code
     * endpoint.port} to {@code port}).
     */
    SqlGatewayEndpoint createSqlGatewayEndpoint(Context context);

    /** Provides information describing the endpoint to be accessed. */
    @PublicEvolving
    interface Context {

        /** Get the service to execute the request. */
        SqlGatewayService getSqlGatewayService();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getFlinkConfiguration();

        /**
         * Returns the options with which the endpoint is created. All options that are prefixed
         * with the endpoint identifier are included in the map.
         *
         * <p>All the keys in the endpoint options are pruned with the prefix. For example, the
         * option {@code sql-gateway.endpoint.rest.host}'s key is {@code host} in the map.
         *
         * <p>An implementation should perform validation of these options.
         */
        Map<String, String> getEndpointOptions();
    }
}
