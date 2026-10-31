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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.GROUPING_KEY;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST_URL;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.JOB_NAME;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the requests the push reporter sends: the path, the job name and grouping key encoded
 * into it, the body, and what happens on shutdown and on failure. The Authorization header is
 * covered by {@link PrometheusPushGatewayReporterAuthenticationTest}.
 */
class PrometheusPushGatewayWireTest {

    private static final String LOGICAL_SCOPE = "scope";

    @RegisterExtension
    private final LoggerAuditingExtension loggerExtension =
            new LoggerAuditingExtension(PrometheusPushGatewayReporter.class, Level.WARN);

    private RecordingGateway gateway;
    private boolean expectWarnings;

    @BeforeEach
    void startGateway() throws IOException {
        gateway = new RecordingGateway();
    }

    @AfterEach
    void stopGateway() {
        if (gateway != null) {
            gateway.stop();
        }
        if (!expectWarnings) {
            assertThat(loggerExtension.getMessages())
                    .as("the reporter logged a warning, so the gateway rejected a request")
                    .isEmpty();
        }
    }

    /**
     * A gateway reached through an ingress is configured with a path, and that path has to survive
     * into the request.
     */
    @Test
    void aPathPrefixInTheHostUrlIsPreserved() throws Exception {
        final MetricConfig config = config("myJob");
        config.setProperty(HOST_URL.key(), gateway.baseUrl() + "/behind/an/ingress");

        reportAndClose(config);

        assertThat(gateway.requests().get(0).path)
                .isEqualTo("/behind/an/ingress/metrics/job/myJob");
    }

    @Test
    void groupingKeyEntriesBecomePathSegments() throws Exception {
        final MetricConfig config = config("myJob");
        config.setProperty(GROUPING_KEY.key(), "k1=v1;k2=v2");

        reportAndClose(config);

        // The grouping key is a map, so the segment order is not fixed; the pairs are.
        assertThat(gateway.requests().get(0).path)
                .startsWith("/metrics/job/myJob/")
                .contains("/k1/v1")
                .contains("/k2/v2");
    }

    @Test
    void aSlashInTheJobNameIsBase64Encoded() throws Exception {
        reportAndClose(config("my/job"));

        assertThat(gateway.requests().get(0).path).isEqualTo("/metrics/job@base64/bXkvam9i");
    }

    @Test
    void aSlashInAGroupingKeyValueIsBase64Encoded() throws Exception {
        final MetricConfig config = config("myJob");
        config.setProperty(GROUPING_KEY.key(), "instance=host/1");

        reportAndClose(config);

        assertThat(gateway.requests().get(0).path)
                .isEqualTo("/metrics/job/myJob/instance@base64/aG9zdC8x");
    }

    @Test
    void theRandomJobNameSuffixIsAppendedToTheConfiguredName() throws Exception {
        final MetricConfig config = config("myJob");
        config.setProperty(RANDOM_JOB_NAME_SUFFIX.key(), "true");

        reportAndClose(config);

        assertThat(gateway.requests().get(0).path).matches("/metrics/job/myJob[0-9a-f]{32}");
    }

    @Test
    void thePushedBodyIsTheExpositionAndTheContentTypeIsTheLegacyTextFormat() throws Exception {
        reportAndClose(config("myJob"));

        final RecordedRequest push = gateway.requests().get(0);
        assertThat(push.contentType).isEqualTo("text/plain; version=0.0.4; charset=utf-8");
        assertThat(push.body).contains("flink_" + LOGICAL_SCOPE + "_someCounter 7.0\n");
        // A delete carries no body. Its method is asserted by the authentication test.
        assertThat(gateway.requests().get(1).body).isEmpty();
    }

    @Test
    void nothingIsDeletedWhenDeleteOnShutdownIsDisabled() throws Exception {
        final MetricConfig config = config("myJob");
        config.setProperty(DELETE_ON_SHUTDOWN.key(), "false");

        reportAndClose(config);

        assertThat(gateway.requests()).hasSize(1);
        assertThat(gateway.requests().get(0).method).isEqualTo("PUT");
    }

    @Test
    void aFailedPushIsLoggedAndSwallowed() throws Exception {
        expectWarnings = true;
        gateway.respondWith(500);

        reportAndClose(config("myJob"));

        assertThat(loggerExtension.getMessages())
                .anyMatch(message -> message.contains("Failed to push metrics to PushGateway"));
    }

    private MetricConfig config(String jobName) {
        final MetricConfig config = new MetricConfig();
        config.setProperty(HOST_URL.key(), gateway.baseUrl());
        config.setProperty(JOB_NAME.key(), jobName);
        config.setProperty(RANDOM_JOB_NAME_SUFFIX.key(), "false");
        config.setProperty(DELETE_ON_SHUTDOWN.key(), "true");
        return config;
    }

    private void reportAndClose(MetricConfig config) {
        final PrometheusPushGatewayReporter reporter =
                new PrometheusPushGatewayReporterFactory().createMetricReporter(config);
        try {
            final Counter counter = new SimpleCounter();
            counter.inc(7);
            reporter.notifyOfAddedMetric(
                    counter,
                    "someCounter",
                    TestUtils.createTestMetricGroup(LOGICAL_SCOPE, Collections.emptyMap()));
            reporter.report();
        } finally {
            reporter.close();
        }
    }

    private static final class RecordedRequest {
        private final String method;
        private final String path;
        private final String contentType;
        private final String body;

        private RecordedRequest(HttpExchange exchange, byte[] body) {
            this.method = exchange.getRequestMethod();
            this.path = exchange.getRequestURI().getRawPath();
            this.contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            this.body = new String(body, StandardCharsets.UTF_8);
        }
    }

    private static final class RecordingGateway {
        private final HttpServer server;
        private final List<RecordedRequest> requests =
                Collections.synchronizedList(new ArrayList<>());
        private volatile int responseCode = 202;

        private RecordingGateway() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext(
                    "/",
                    exchange -> {
                        try {
                            // Record before responding, so a client that has its response has our
                            // record too.
                            requests.add(
                                    new RecordedRequest(
                                            exchange, exchange.getRequestBody().readAllBytes()));
                            exchange.sendResponseHeaders(responseCode, -1);
                        } finally {
                            exchange.close();
                        }
                    });
            server.start();
        }

        private String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private void respondWith(int code) {
            responseCode = code;
        }

        private List<RecordedRequest> requests() {
            return requests;
        }

        private void stop() {
            server.stop(0);
        }
    }
}
