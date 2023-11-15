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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.net.SSLUtilsTest;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.net.ssl.SSLException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;

/**
 * This test validates that connections are failing when mutual auth is enabled but untrusted keys
 * or fingerprints are used.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class RestServerSSLAuthITCase {

    private static final String KEY_STORE_FILE =
            RestServerSSLAuthITCase.class.getResource("/local127.keystore").getFile();
    private static final String TRUST_STORE_FILE =
            RestServerSSLAuthITCase.class.getResource("/local127.truststore").getFile();
    private static final String UNTRUSTED_KEY_STORE_FILE =
            RestServerSSLAuthITCase.class.getResource("/untrusted.keystore").getFile();

    private static final Time timeout = Time.seconds(10L);

    private final Configuration clientConfig;
    private final Configuration serverConfig;

    public RestServerSSLAuthITCase(final Tuple2<Configuration, Configuration> clientServerConfig) {
        this.clientConfig = clientServerConfig.f0;
        this.serverConfig = clientServerConfig.f1;
    }

    @Parameters
    public static Collection<Object[]> data() throws Exception {
        // client and server trust store does not match
        Tuple2<Configuration, Configuration> untrusted = getClientServerConfiguration();

        Configuration serverConfig = new Configuration(untrusted.f1);
        serverConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_FILE);
        // expect fingerprint which client does not have
        serverConfig.setString(
                SecurityOptions.SSL_REST_CERT_FINGERPRINT,
                SSLUtilsTest.getRestCertificateFingerprint(serverConfig, "flink.test")
                        .replaceAll("[0-9A-Z]", "0"));

        Configuration clientConfig = new Configuration(untrusted.f0);
        clientConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_FILE);

        // client and server uses same trust store, however server configured with mismatching
        // fingerprint
        Tuple2<Configuration, Configuration> withFingerprint =
                Tuple2.of(clientConfig, serverConfig);

        return Arrays.asList(new Object[][] {{untrusted}, {withFingerprint}});
    }

    @TestTemplate
    void testConnectFailure() throws Exception {
        RestClient restClient = null;
        RestServerEndpoint serverEndpoint = null;

        try {
            RestfulGateway restfulGateway = new TestingRestfulGateway.Builder().build();
            RestServerEndpointITCase.TestVersionHandler testVersionHandler =
                    new RestServerEndpointITCase.TestVersionHandler(
                            () -> CompletableFuture.completedFuture(restfulGateway),
                            RpcUtils.INF_TIMEOUT);

            serverEndpoint =
                    TestRestServerEndpoint.builder(serverConfig)
                            .withHandler(testVersionHandler.getMessageHeaders(), testVersionHandler)
                            .buildAndStart();
            restClient = new RestClient(clientConfig, Executors.directExecutor());

            assertThatFuture(
                            restClient.sendRequest(
                                    serverEndpoint.getServerAddress().getHostName(),
                                    serverEndpoint.getServerAddress().getPort(),
                                    RestServerEndpointITCase.TestVersionHeaders.INSTANCE,
                                    EmptyMessageParameters.getInstance(),
                                    EmptyRequestBody.getInstance(),
                                    Collections.emptyList()))
                    .failsWithin(60, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .satisfies(anyCauseMatches(SSLException.class));
        } finally {
            if (restClient != null) {
                restClient.shutdown(timeout);
            }

            if (serverEndpoint != null) {
                serverEndpoint.close();
            }
        }
    }

    private static Tuple2<Configuration, Configuration> getClientServerConfiguration() {
        final Configuration baseConfig = new Configuration();
        baseConfig.setString(RestOptions.BIND_PORT, "0");
        baseConfig.setString(RestOptions.ADDRESS, "localhost");
        baseConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        baseConfig.setBoolean(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, true);
        baseConfig.setString(SecurityOptions.SSL_ALGORITHMS, "TLS_RSA_WITH_AES_128_CBC_SHA");

        Configuration serverConfig = new Configuration(baseConfig);
        serverConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_FILE);
        serverConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "password");
        serverConfig.setString(SecurityOptions.SSL_REST_KEYSTORE, KEY_STORE_FILE);
        serverConfig.setString(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "password");
        serverConfig.setString(SecurityOptions.SSL_REST_KEY_PASSWORD, "password");

        Configuration clientConfig = new Configuration(baseConfig);
        clientConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE, UNTRUSTED_KEY_STORE_FILE);
        clientConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "password");
        clientConfig.setString(SecurityOptions.SSL_REST_KEYSTORE, KEY_STORE_FILE);
        clientConfig.setString(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "password");
        clientConfig.setString(SecurityOptions.SSL_REST_KEY_PASSWORD, "password");
        return Tuple2.of(clientConfig, serverConfig);
    }
}
