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

package org.apache.flink.table.gateway.rest.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpoint;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactory;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.ADDRESS;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.BIND_ADDRESS;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.BIND_PORT;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions.PORT;

/** The factory for sql gateway. */
public class SqlGatewayRestEndpointFactory implements SqlGatewayEndpointFactory {

    /** The identifier string for {@link SqlGatewayRestEndpointFactory}. */
    public static final String IDENTIFIER = "rest";

    @Override
    public SqlGatewayEndpoint createSqlGatewayEndpoint(Context context) {
        SqlGatewayEndpointFactoryUtils.EndpointFactoryHelper endpointFactoryHelper =
                SqlGatewayEndpointFactoryUtils.createEndpointFactoryHelper(this, context);
        // Check that ADDRESS must be set
        endpointFactoryHelper.validate();
        Configuration config = rebuildRestEndpointOptions(context.getEndpointOptions());
        try {
            return new SqlGatewayRestEndpoint(config, context.getSqlGatewayService());
        } catch (Exception e) {
            throw new SqlGatewayException("Cannot start the rest endpoint.", e);
        }
    }

    public static Configuration rebuildRestEndpointOptions(Map<String, String> configMap) {
        // Sql Gateway Rest Endpoint only supports following options
        Map<String, String> rebuildMap = new HashMap<>();
        // 1. ADDRESS(required option)
        rebuildMap.put(RestOptions.ADDRESS.key(), configMap.get(ADDRESS.key()));
        // 2. BIND ADDRESS
        // If the user specifies BIND ADDRESS, use BIND ADDRESS, otherwise use the default value of
        // ADDRESS
        rebuildMap.put(
                RestOptions.BIND_ADDRESS.key(),
                configMap.getOrDefault(BIND_ADDRESS.key(), configMap.get(ADDRESS.key())));
        // 3. BIND PORT
        // If the user specifies BIND PORT, use the BIND PORT
        if (configMap.containsKey(BIND_PORT.key())) {
            rebuildMap.put(RestOptions.BIND_PORT.key(), configMap.get(BIND_PORT.key()));
        }
        // 4. PORT
        // If the user doesn't specify BIND PORT, use PORT or the default value of BIND PORT
        else {
            if (configMap.containsKey(PORT.key())) {
                rebuildMap.put(RestOptions.BIND_PORT.key(), configMap.get(PORT.key()));
            } else {
                rebuildMap.put(RestOptions.BIND_PORT.key(), BIND_PORT.defaultValue());
            }
        }
        return Configuration.fromMap(rebuildMap);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ADDRESS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
