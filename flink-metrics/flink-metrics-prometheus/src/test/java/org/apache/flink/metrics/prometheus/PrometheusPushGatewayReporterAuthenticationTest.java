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

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.metrics.reporter.Scheduled;

import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.Collector;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.exporter.common.TextFormat;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST_URL;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.JOB_NAME;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.PASSWORD;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests authentication on actual PushGateway requests. */
class PrometheusPushGatewayReporterAuthenticationTest {

    @ParameterizedTest
    @CsvSource({
        "user, password, Basic dXNlcjpwYXNzd29yZA==",
        "user, päss:word, Basic dXNlcjpww6Rzczp3b3Jk",
        "'', '', Basic Og==",
        ", ,",
        "user, ,",
        ", password,"
    })
    void testAuthenticationOnPushAndDeleteWithoutJaxb(
            String username, String password, String expectedAuthorization) throws Exception {
        final List<String> methods = Collections.synchronizedList(new ArrayList<>());
        final List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
        final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/metrics/job/test-job",
                exchange -> {
                    try {
                        exchange.getRequestBody().readAllBytes();
                        methods.add(exchange.getRequestMethod());
                        authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
                        exchange.sendResponseHeaders(202, -1);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        try (URLClassLoader classLoader = createClassLoaderWithoutJaxb()) {
            final MetricConfig config = new MetricConfig();
            config.setProperty(HOST_URL.key(), "http://127.0.0.1:" + server.getAddress().getPort());
            config.setProperty(JOB_NAME.key(), "test-job");
            config.setProperty(RANDOM_JOB_NAME_SUFFIX.key(), "false");
            config.setProperty(DELETE_ON_SHUTDOWN.key(), "true");
            if (username != null) {
                config.setProperty(USERNAME.key(), username);
            }
            if (password != null) {
                config.setProperty(PASSWORD.key(), password);
            }
            final MetricReporterFactory factory =
                    (MetricReporterFactory)
                            classLoader
                                    .loadClass(PrometheusPushGatewayReporterFactory.class.getName())
                                    .getDeclaredConstructor()
                                    .newInstance();
            final MetricReporter reporter = factory.createMetricReporter(config);
            try {
                assertThatThrownBy(
                                () ->
                                        reporter.getClass()
                                                .getClassLoader()
                                                .loadClass("javax.xml.bind.DatatypeConverter"))
                        .isInstanceOf(ClassNotFoundException.class);
                ((Scheduled) reporter).report();
            } finally {
                reporter.close();
            }
            assertThat(methods).containsExactly("PUT", "DELETE");
            assertThat(authorizations)
                    .containsExactly(expectedAuthorization, expectedAuthorization);
        } finally {
            server.stop(0);
        }
    }

    private static URLClassLoader createClassLoaderWithoutJaxb() {
        final URL[] urls =
                Stream.of(
                                PrometheusPushGatewayReporter.class,
                                Collector.class,
                                PushGateway.class,
                                TextFormat.class)
                        .map(type -> type.getProtectionDomain().getCodeSource().getLocation())
                        .distinct()
                        .toArray(URL[]::new);
        // Reload the reporter and its Prometheus dependencies so JAXB on the test classpath
        // cannot hide the missing runtime dependency.
        final ClassLoader parent =
                new ClassLoader(
                        PrometheusPushGatewayReporterAuthenticationTest.class.getClassLoader()) {
                    @Override
                    protected Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (name.startsWith("javax.xml.bind.")
                                || name.startsWith("org.apache.flink.metrics.prometheus.")
                                || name.startsWith("io.prometheus.")) {
                            throw new ClassNotFoundException(name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };
        return new URLClassLoader(urls, parent);
    }
}
