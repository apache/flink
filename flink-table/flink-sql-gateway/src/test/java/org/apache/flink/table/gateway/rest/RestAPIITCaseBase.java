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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.table.gateway.rest.util.TestingRestClient;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointTestUtils.getBaseConfig;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointTestUtils.getFlinkConfig;
import static org.apache.flink.table.gateway.rest.util.TestingRestClient.getTestingRestClient;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The base class for Rest API IT test. */
abstract class RestAPIITCaseBase {

    @RegisterExtension
    @Order(1)
    private static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    @RegisterExtension
    @Order(2)
    protected static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @Nullable private static TestingRestClient restClient = null;
    @Nullable private static String targetAddress = null;
    @Nullable private static SqlGatewayRestEndpoint sqlGatewayRestEndpoint = null;

    private static int port = 0;

    @BeforeAll
    static void start() throws Exception {
        final String address = InetAddress.getLoopbackAddress().getHostAddress();
        Configuration config = getBaseConfig(getFlinkConfig(address, address, "0"));
        sqlGatewayRestEndpoint =
                new SqlGatewayRestEndpoint(config, SQL_GATEWAY_SERVICE_EXTENSION.getService());
        sqlGatewayRestEndpoint.start();
        InetSocketAddress serverAddress = checkNotNull(sqlGatewayRestEndpoint.getServerAddress());
        restClient = getTestingRestClient();
        targetAddress = serverAddress.getHostName();
        port = serverAddress.getPort();
    }

    @AfterAll
    static void stop() throws Exception {
        checkNotNull(sqlGatewayRestEndpoint);
        sqlGatewayRestEndpoint.close();
        checkNotNull(restClient);
        restClient.shutdown(Time.seconds(3));
    }

    public <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request)
                    throws IOException {
        checkNotNull(restClient);
        return restClient.sendRequest(
                targetAddress, port, messageHeaders, messageParameters, request);
    }
}
