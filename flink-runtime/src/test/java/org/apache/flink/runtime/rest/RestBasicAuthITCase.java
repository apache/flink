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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** This test validates basic auth for rest endpoints. */
public class RestBasicAuthITCase extends TestLogger {

    private static final String PASSWORD_FILE =
            RestBasicAuthITCase.class.getResource("/.htpasswd").getFile();

    private static RestServerEndpoint serverEndpoint;
    private static RestServerEndpointITCase.TestVersionHandler testVersionHandler;

    @BeforeClass
    public static void init() throws Exception {
        RestServerEndpointConfiguration restServerConfig =
                RestServerEndpointConfiguration.fromConfiguration(getConfig());

        RestfulGateway restfulGateway = new TestingRestfulGateway.Builder().build();
        testVersionHandler =
                new RestServerEndpointITCase.TestVersionHandler(
                        () -> CompletableFuture.completedFuture(restfulGateway),
                        RpcUtils.INF_TIMEOUT);

        serverEndpoint =
                new TestRestServerEndpoint(
                        restServerConfig,
                        Arrays.asList(
                                Tuple2.of(
                                        testVersionHandler.getMessageHeaders(),
                                        testVersionHandler)));
        serverEndpoint.start();
    }

    @Test
    public void testAuthGoodCredentials() throws Exception {
        RestClient restClientWithGoodCredentials =
                new RestClient(
                        RestClientConfiguration.fromConfiguration(getConfig()),
                        TestingUtils.defaultExecutor());
        restClientWithGoodCredentials
                .sendRequest(
                        serverEndpoint.getServerAddress().getHostString(),
                        serverEndpoint.getServerAddress().getPort(),
                        testVersionHandler.getMessageHeaders())
                .get();
    }

    @Test
    public void testAuthBadCredentials() throws Exception {
        Configuration badCredsConf = getConfig();
        badCredsConf.set(SecurityOptions.BASIC_AUTH_CLIENT_CREDENTIALS, "wrong:pwd");
        RestClient restClientWithBadCredentials =
                new RestClient(
                        RestClientConfiguration.fromConfiguration(badCredsConf),
                        TestingUtils.defaultExecutor());

        try {
            restClientWithBadCredentials
                    .sendRequest(
                            serverEndpoint.getServerAddress().getHostString(),
                            serverEndpoint.getServerAddress().getPort(),
                            testVersionHandler.getMessageHeaders())
                    .get();
            fail();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause().getMessage().contains("Invalid credentials"));
        }
    }

    private static Configuration getConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "0");
        conf.setString(RestOptions.ADDRESS, "localhost");
        conf.set(SecurityOptions.BASIC_AUTH_ENABLED, true);
        conf.set(SecurityOptions.BASIC_AUTH_PWD_FILE, PASSWORD_FILE);
        conf.set(SecurityOptions.BASIC_AUTH_CLIENT_CREDENTIALS, "testusr:testpwd");
        return conf;
    }
}
