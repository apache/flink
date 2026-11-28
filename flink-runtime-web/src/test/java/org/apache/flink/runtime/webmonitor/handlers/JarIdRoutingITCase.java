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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.util.BlobServerExtension;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.WebSubmissionExtension;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sends jar IDs over HTTP so the IDs pass through the router (which URL-decodes path segments) and
 * through {@link JarIdPathParameter} as they do in a cluster.
 *
 * <p>Each endpoint is tested with every jar ID on purpose, to test behaviour as black-box contract
 * tests, rather than assume that the endpoints will always share a router or path parameter.
 */
class JarIdRoutingITCase {

    private static final String JAR_NAME = "test-program.jar";

    /** The endpoints that take a jar ID, with the status each returns on success. */
    enum Endpoint {
        GET_PLAN("GET", "/jars/%s/plan", 200),
        POST_PLAN("POST", "/jars/%s/plan", 200),
        RUN("POST", "/jars/%s/run", 200),
        RUN_APPLICATION("POST", "/jars/%s/run-application", 202),
        DELETE("DELETE", "/jars/%s", 200);

        final String method;
        private final String pathTemplate;
        final int successStatus;

        Endpoint(final String method, final String pathTemplate, final int successStatus) {
            this.method = method;
            this.pathTemplate = pathTemplate;
            this.successStatus = successStatus;
        }

        String path(final String jarId) {
            return String.format(pathTemplate, jarId);
        }
    }

    /**
     * Jar IDs as they appear in the URL, before the router decodes them. The router decodes the
     * path and then each segment again (so double-encoded IDs reach the handler decoded).
     */
    private static final List<String> INVALID_ENCODED_JAR_IDS =
            List.of("..", ".", "%2E%2E", "%252E%252E", "%252F", "subdir%252F" + JAR_NAME);

    private static final String INVALID_JAR_ID_MESSAGE = "Cannot resolve path parameter (jarid)";

    @RegisterExtension
    private static final AllCallbackWrapper<BlobServerExtension> BLOB_SERVER_EXTENSION =
            new AllCallbackWrapper<>(new BlobServerExtension());

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static Path jarDir;
    private static Path testProgramJar;
    private static TestRestServerEndpoint serverEndpoint;
    private static String baseUrl;

    @BeforeAll
    static void startServer(@TempDir final Path tempDir) throws Exception {
        jarDir = tempDir.resolve("jars");
        testProgramJar = Paths.get(System.getProperty("targetDir")).resolve(JAR_NAME);

        final TestingDispatcherGateway gateway =
                TestingDispatcherGateway.newBuilder()
                        .setBlobServerPort(
                                BLOB_SERVER_EXTENSION.getCustomExtension().getBlobServerPort())
                        .setSubmitApplicationFunction(
                                application -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .build();
        final String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();
        final Configuration config = new Configuration();
        config.set(RestOptions.BIND_PORT, "0");
        config.set(RestOptions.BIND_ADDRESS, loopbackAddress);
        config.set(RestOptions.ADDRESS, loopbackAddress);

        // handlers that a cluster registers, so requests are routed as they are in a cluster
        final WebSubmissionExtension webSubmission =
                new WebSubmissionExtension(
                        new Configuration(),
                        () -> CompletableFuture.completedFuture(gateway),
                        Collections.emptyMap(),
                        CompletableFuture.completedFuture(loopbackAddress),
                        jarDir,
                        EXECUTOR_EXTENSION.getExecutor(),
                        Duration.ofSeconds(10));
        final TestRestServerEndpoint.Builder builder = TestRestServerEndpoint.builder(config);
        webSubmission.getHandlers().forEach(handler -> builder.withHandler(handler.f0, handler.f1));
        serverEndpoint = builder.buildAndStart();
        baseUrl = serverEndpoint.getRestBaseUrl();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (serverEndpoint != null) {
            serverEndpoint.closeAsync().get();
        }
    }

    @BeforeEach
    void resetJarDir() throws IOException {
        Files.createDirectories(jarDir.resolve("subdir"));
        Files.copy(testProgramJar, jarDir.resolve(JAR_NAME), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                testProgramJar,
                jarDir.resolve("subdir").resolve(JAR_NAME),
                StandardCopyOption.REPLACE_EXISTING);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("invalidJarIdRequests")
    void testInvalidJarIdIsRejected(final String method, final String path) throws Exception {
        final Tuple2<Integer, String> response = sendRequest(method, path);

        assertThat(response.f0).as("status of %s %s: %s", method, path, response.f1).isEqualTo(400);
        assertThat(response.f1).contains(INVALID_JAR_ID_MESSAGE);
        assertThat(jarDir.resolve(JAR_NAME)).isRegularFile();
        assertThat(jarDir.resolve("subdir").resolve(JAR_NAME)).isRegularFile();
    }

    static Stream<Arguments> invalidJarIdRequests() {
        return Stream.of(Endpoint.values())
                .flatMap(
                        endpoint ->
                                INVALID_ENCODED_JAR_IDS.stream()
                                        .map(
                                                jarId ->
                                                        Arguments.of(
                                                                endpoint.method,
                                                                endpoint.path(jarId))));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("jarIdWithEncodedSeparatorRequests")
    void testJarIdWithEncodedSeparatorMatchesNoRoute(final String method, final String path)
            throws Exception {
        final Tuple2<Integer, String> response = sendRequest(method, path);

        assertThat(response.f0).as("status of %s %s: %s", method, path, response.f1).isEqualTo(404);
        assertThat(jarDir.resolve("subdir").resolve(JAR_NAME)).isRegularFile();
    }

    static Stream<Arguments> jarIdWithEncodedSeparatorRequests() {
        return Stream.of(Endpoint.values())
                .map(
                        endpoint ->
                                Arguments.of(
                                        endpoint.method, endpoint.path("subdir%2F" + JAR_NAME)));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("emptyJarIdRequests")
    void testEmptyJarIdIsRejected(final String method, final String path) throws Exception {
        final Tuple2<Integer, String> response = sendRequest(method, path);

        assertThat(response.f0).as("status of %s %s: %s", method, path, response.f1).isEqualTo(400);
        assertThat(response.f1).contains(INVALID_JAR_ID_MESSAGE);
    }

    static Stream<Arguments> emptyJarIdRequests() {
        return Stream.of(Endpoint.values())
                .filter(endpoint -> endpoint != Endpoint.DELETE)
                .map(endpoint -> Arguments.of(endpoint.method, endpoint.path("")));
    }

    @Test
    void testDeleteWithoutJarIdMatchesNoRoute() throws Exception {
        Files.delete(jarDir.resolve("subdir").resolve(JAR_NAME));
        Files.delete(jarDir.resolve("subdir"));
        Files.delete(jarDir.resolve(JAR_NAME));

        final Tuple2<Integer, String> response = sendRequest("DELETE", "/jars/");

        assertThat(response.f0).as("status: %s", response.f1).isEqualTo(404);
        assertThat(jarDir).isDirectory();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Endpoint.class)
    void testUploadedJarIdIsAccepted(final Endpoint endpoint) throws Exception {
        final String path = endpoint.path(JAR_NAME);

        final Tuple2<Integer, String> response = sendRequest(endpoint.method, path);

        assertThat(response.f0)
                .as("status of %s %s: %s", endpoint.method, path, response.f1)
                .isEqualTo(endpoint.successStatus);
    }

    private static Tuple2<Integer, String> sendRequest(final String method, final String path)
            throws IOException {
        // URI#toURL keeps "..", "." and empty segments, so the server sees the path as written
        final HttpURLConnection connection =
                (HttpURLConnection) URI.create(baseUrl + path).toURL().openConnection();
        try {
            connection.setRequestMethod(method);
            if ("POST".equals(method)) {
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                try (OutputStream body = connection.getOutputStream()) {
                    body.write("{}".getBytes(StandardCharsets.UTF_8));
                }
            }
            final int status = connection.getResponseCode();
            try (InputStream body =
                    status >= 400 ? connection.getErrorStream() : connection.getInputStream()) {
                return Tuple2.of(
                        status, body == null ? "" : IOUtils.toString(body, StandardCharsets.UTF_8));
            }
        } finally {
            connection.disconnect();
        }
    }
}
