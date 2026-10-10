/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.prometheus;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.util.TestMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.PortRange;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

class TestUtils {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    public static PrometheusReporter reporterOnFreePort() {
        try (NetUtils.Port port = NetUtils.getAvailablePort()) {
            return new PrometheusReporter(new PortRange(String.valueOf(port.getPort())));
        } catch (Exception e) {
            throw new RuntimeException("Could not start a reporter on a free port.", e);
        }
    }

    public static HttpResponse<String> scrape(int port, @Nullable String accept)
            throws IOException, InterruptedException {
        return request(port, "/metrics", accept);
    }

    public static HttpResponse<String> request(int port, String path, @Nullable String accept)
            throws IOException, InterruptedException {
        final HttpRequest.Builder request =
                HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + path)).GET();
        if (accept != null) {
            request.header("Accept", accept);
        }
        return HTTP_CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** The reporter's current output. */
    public static String scrapeBody(PrometheusReporter reporter)
            throws IOException, InterruptedException {
        return scrape(reporter.getPort(), null).body();
    }

    /** How many samples in the body carry this exact sample name. */
    public static long samplesNamed(String body, String sampleName) {
        return Arrays.stream(body.split("\n"))
                .filter(l -> l.startsWith(sampleName + "{") || l.startsWith(sampleName + " "))
                .count();
    }

    /** Registers a metric the way {@code MetricRegistryImpl} does, swallowing what it throws. */
    public static void registerLikeRegistry(
            AbstractPrometheusReporter reporter,
            Metric metric,
            String metricName,
            MetricGroup group) {
        try {
            reporter.notifyOfAddedMetric(metric, metricName, group);
        } catch (Exception e) {
            // The registry catches and logs whatever a reporter throws, so a metric lost this way
            // looks the same to a user as one the reporter dropped itself.
        }
    }

    public static MetricGroup createTestMetricGroup(
            String logicalScope, Map<String, String> variables) {
        return TestMetricGroup.newBuilder()
                .setLogicalScopeFunction(
                        (characterFilter, character) ->
                                characterFilter.filterCharacters(logicalScope))
                .setVariables(variables)
                .build();
    }

    public static Map<String, String> toMap(String[] labels, String[] values) {
        // when querying metrics the order of labels is important; use insertion order for
        // simplicity
        final Map<String, String> variables = new LinkedHashMap<>();
        for (int i = 0; i < labels.length; i++) {
            variables.put(ScopeFormat.asVariable(labels[i]), values[i]);
        }

        return variables;
    }
}
