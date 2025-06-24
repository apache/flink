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

package org.apache.flink.table.gateway.api.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpoint;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactory;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Factory to create the {@link SqlGatewayEndpoint}. */
public class MockedSqlGatewayEndpointFactory implements SqlGatewayEndpointFactory {

    public static final ConfigOption<String> ID =
            ConfigOptions.key("id").stringType().noDefaultValue();
    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host").stringType().noDefaultValue();
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().noDefaultValue();
    public static final ConfigOption<String> DESCRIPTION =
            ConfigOptions.key("description").stringType().defaultValue("Hello World.");

    @Override
    public SqlGatewayEndpoint createSqlGatewayEndpoint(Context context) {
        SqlGatewayEndpointFactoryUtils.EndpointFactoryHelper helper =
                SqlGatewayEndpointFactoryUtils.createEndpointFactoryHelper(this, context);
        helper.validate();

        return new MockedSqlGatewayEndpoint(
                helper.getOptions().get(ID),
                helper.getOptions().get(HOST),
                helper.getOptions().get(PORT),
                helper.getOptions().get(DESCRIPTION));
    }

    @Override
    public String factoryIdentifier() {
        return "mocked";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ID);
        options.add(HOST);
        options.add(PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.singleton(DESCRIPTION);
    }
}
