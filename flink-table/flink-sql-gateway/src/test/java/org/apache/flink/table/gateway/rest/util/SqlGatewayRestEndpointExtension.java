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
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointTestUtils.getBaseConfig;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointTestUtils.getFlinkConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A simple {@link Extension} that manages the lifecycle of the {@link SqlGatewayRestEndpoint}. */
public class SqlGatewayRestEndpointExtension implements BeforeAllCallback, AfterAllCallback {

    private final Supplier<SqlGatewayService> serviceSupplier;

    private SqlGatewayRestEndpoint sqlGatewayRestEndpoint;
    private SqlGatewayService sqlGatewayService;
    private String targetAddress;
    private int targetPort;

    public String getTargetAddress() {
        return targetAddress;
    }

    public int getTargetPort() {
        return targetPort;
    }

    public SqlGatewayService getSqlGatewayService() {
        return sqlGatewayService;
    }

    public SqlGatewayRestEndpointExtension(Supplier<SqlGatewayService> serviceSupplier) {
        this.serviceSupplier = serviceSupplier;
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        String address = InetAddress.getLoopbackAddress().getHostAddress();
        Configuration config = getBaseConfig(getFlinkConfig(address, address, "0"));

        try {
            sqlGatewayService = serviceSupplier.get();
            sqlGatewayRestEndpoint = new SqlGatewayRestEndpoint(config, sqlGatewayService);
            sqlGatewayRestEndpoint.start();
        } catch (Exception e) {
            throw new SqlGatewayException(
                    "Unexpected error occurred when trying to start the rest endpoint of sql gateway.",
                    e);
        }

        InetSocketAddress serverAddress = checkNotNull(sqlGatewayRestEndpoint.getServerAddress());
        targetAddress = serverAddress.getHostName();
        targetPort = serverAddress.getPort();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        try {
            sqlGatewayRestEndpoint.stop();
        } catch (Exception e) {
            throw new SqlGatewayException(
                    "Unexpected error occurred when trying to stop the rest endpoint of sql gateway.",
                    e);
        }
    }
}
