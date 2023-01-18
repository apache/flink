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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils;

import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.getEndpointConfig;
import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.getSqlGatewayOptionPrefix;
import static org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointFactory.IDENTIFIER;
import static org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointFactory.rebuildRestEndpointOptions;

/** The tools to get configuration in test cases. */
public class SqlGatewayRestEndpointTestUtils {

    /** Get the full name of sql gateway rest endpoint options. */
    public static String getSqlGatewayRestOptionFullName(String key) {
        return getSqlGatewayOptionPrefix(IDENTIFIER) + key;
    }

    /** Get the configuration used by SqlGatewayRestEndpoint. */
    public static Configuration getBaseConfig(Configuration flinkConf) {
        SqlGatewayEndpointFactoryUtils.DefaultEndpointFactoryContext context =
                new SqlGatewayEndpointFactoryUtils.DefaultEndpointFactoryContext(
                        null, flinkConf, getEndpointConfig(flinkConf, IDENTIFIER));

        return rebuildRestEndpointOptions(context.getEndpointOptions());
    }

    /** Create the configuration generated from flink-conf.yaml. */
    public static Configuration getFlinkConfig(
            String address, String bindAddress, String portRange) {
        final Configuration config = new Configuration();
        if (address != null) {
            config.setString(
                    getSqlGatewayRestOptionFullName(SqlGatewayRestOptions.ADDRESS.key()), address);
        }
        if (bindAddress != null) {
            config.setString(
                    getSqlGatewayRestOptionFullName(SqlGatewayRestOptions.BIND_ADDRESS.key()),
                    bindAddress);
        }
        if (portRange != null) {
            config.setString(
                    getSqlGatewayRestOptionFullName(SqlGatewayRestOptions.PORT.key()), portRange);
        }
        return config;
    }
}
