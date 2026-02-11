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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.util.TestMessageHeaders;
import org.apache.flink.runtime.rest.util.TestRestHandler;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test validates that REST SSL endpoint verification works as expected and will fail the
 * request if the endpoint certificate does not match the REST server hostname.
 */
public class RestSSLEndpointVerificationITCase {

    private static final File KEY_STORE_FILE =
            new File(
                    RestSSLEndpointVerificationITCase.class
                            .getResource("/local127.keystore")
                            .getFile());
    private static final File TRUST_STORE_FILE =
            new File(
                    RestSSLEndpointVerificationITCase.class
                            .getResource("/local127.truststore")
                            .getFile());

    private static final TestMessageHeaders<
                    EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters>
            TEST_MESSAGE_HEADERS =
                    TestMessageHeaders.emptyBuilder()
                            .setTargetRestEndpointURL("/test-handler")
                            .build();

    private final Configuration config;

    public RestSSLEndpointVerificationITCase() {
        config = new Configuration();
        config.set(RestOptions.BIND_PORT, "0");
        config.set(RestOptions.ADDRESS, "localhost");
        config.set(SecurityOptions.SSL_REST_ENABLED, true);
        config.set(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_FILE.getAbsolutePath());
        config.set(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "password");
        config.set(SecurityOptions.SSL_REST_KEYSTORE, KEY_STORE_FILE.getAbsolutePath());
        config.set(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "password");
        config.set(SecurityOptions.SSL_REST_KEY_PASSWORD, "password");
        config.set(SecurityOptions.SSL_REST_VERIFY_HOSTNAME, true);
    }

    /**
     * With hostname verification turned on, this should execute fine as "127.0.0.1" is part of the
     * Subject Alternative Names.
     */
    @Test
    void testConnectSuccess() throws Exception {
        try (RestServerEndpoint serverEndpoint = getRestServerEndpoint();
                RestClient restClient = getRestClient("127.0.0.1", serverEndpoint.getRestPort())) {
            assertSan(serverEndpoint);

            var result =
                    restClient
                            .sendRequest(
                                    serverEndpoint.getServerAddress().getHostName(),
                                    serverEndpoint.getServerAddress().getPort(),
                                    TEST_MESSAGE_HEADERS,
                                    EmptyMessageParameters.getInstance(),
                                    EmptyRequestBody.getInstance(),
                                    Collections.emptyList())
                            .get();
            assertThat(result).isNotNull();
        }
    }

    /** This should fail as "127.0.0.2" is not part of the Subject Alternative Names. */
    @Test
    void testConnectFailure() throws Exception {
        try (RestServerEndpoint serverEndpoint = getRestServerEndpoint();
                RestClient restClient = getRestClient("127.0.0.2", serverEndpoint.getRestPort())) {
            assertSan(serverEndpoint);

            assertThatFuture(
                            restClient.sendRequest(
                                    serverEndpoint.getServerAddress().getHostName(),
                                    serverEndpoint.getServerAddress().getPort(),
                                    TEST_MESSAGE_HEADERS,
                                    EmptyMessageParameters.getInstance(),
                                    EmptyRequestBody.getInstance(),
                                    Collections.emptyList()))
                    .failsWithin(60, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .satisfies(anyCauseMatches(CertificateException.class))
                    .withMessageContaining("No subject alternative names matching IP address");
        }
    }

    /** Asserts that the SANs in the certificate are what we expect. */
    private void assertSan(RestServerEndpoint serverEndpoint) throws Exception {
        KeyStore ts = KeyStore.getInstance("JKS");
        try (InputStream in = new FileInputStream(TRUST_STORE_FILE)) {
            ts.load(in, null);
        }

        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);

        SSLSocketFactory factory = ctx.getSocketFactory();
        try (SSLSocket socket =
                (SSLSocket)
                        factory.createSocket(
                                serverEndpoint.getServerAddress().getHostName(),
                                serverEndpoint.getServerAddress().getPort())) {

            socket.startHandshake();
            Certificate[] certs = socket.getSession().getPeerCertificates();
            X509Certificate x509 = (X509Certificate) certs[0];

            // Extract Subject Alternative Names
            Collection<List<?>> sans = x509.getSubjectAlternativeNames();
            List<String> ipAddresses =
                    sans.stream()
                            .filter(san -> (Integer) san.get(0) == 7) // Type 7 is IP address
                            .map(san -> (String) san.get(1))
                            .collect(Collectors.toList());

            // Assert that 127.0.0.1 is in the certificate
            assertThat(ipAddresses).contains("127.0.0.1");
        }
    }

    private RestServerEndpoint getRestServerEndpoint() throws Exception {
        RestfulGateway restfulGateway = new TestingRestfulGateway.Builder().build();
        final GatewayRetriever<RestfulGateway> gatewayRetriever =
                () -> CompletableFuture.completedFuture(restfulGateway);

        final TestRestHandler<
                        RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters>
                testRestHandler =
                        new TestRestHandler<>(
                                gatewayRetriever,
                                TEST_MESSAGE_HEADERS,
                                CompletableFuture.completedFuture(EmptyResponseBody.getInstance()));

        return TestRestServerEndpoint.builder(config)
                .withHandler(TEST_MESSAGE_HEADERS, testRestHandler)
                .buildAndStart();
    }

    private RestClient getRestClient(String host, int port) throws ConfigurationException {
        return new RestClient(config, Executors.directExecutor(), host, port, null);
    }
}
