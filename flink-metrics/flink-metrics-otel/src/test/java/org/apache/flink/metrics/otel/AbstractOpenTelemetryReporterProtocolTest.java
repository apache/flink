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

package org.apache.flink.metrics.otel;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for OpenTelemetry reporter protocol configuration tests. */
@ExtendWith(TestLoggerExtension.class)
public abstract class AbstractOpenTelemetryReporterProtocolTest<T> extends OpenTelemetryTestBase {

    protected T reporter;

    @BeforeEach
    public void setUpEach() {
        reporter = createReporter();
    }

    @AfterEach
    public void tearDownEach() {
        if (reporter != null) {
            try {
                closeReporter(reporter);
            } catch (Exception e) {
                // Ignore exceptions during cleanup
            }
        }
    }

    @Test
    public void testExplicitGrpcProtocol() throws Exception {
        MetricConfig config = createConfig("gRPC");
        setupAndReport(config);
        assertReported();
    }

    @Test
    public void testHttpProtocol() throws Exception {
        MetricConfig config = createConfig("HTTP");
        setupAndReport(config);
        assertReported();
    }

    @Test
    public void testHttpProtocolWithLowerCase() throws Exception {
        MetricConfig config = createConfig("http");
        setupAndReport(config);
        assertReported();
    }

    @Test
    public void testGrpcProtocolWithLowerCase() throws Exception {
        MetricConfig config = createConfig("grpc");
        setupAndReport(config);
        assertReported();
    }

    protected MetricConfig createConfig(String protocol) {
        boolean isHttp = protocol.equalsIgnoreCase("HTTP");
        MetricConfig config = isHttp ? new MetricConfig() : createMetricConfig();
        if (isHttp) {
            config.setProperty(
                    OpenTelemetryReporterOptions.EXPORTER_ENDPOINT.key(),
                    getOtelContainer().getHttpEndpoint());
        }
        // For gRPC, not setting the endpoint here, it will be set by default by the test container
        config.setProperty(OpenTelemetryReporterOptions.EXPORTER_PROTOCOL.key(), protocol);
        return config;
    }

    protected abstract T createReporter();

    protected abstract void closeReporter(T reporter);

    protected abstract void setupAndReport(MetricConfig config);

    protected abstract void assertReported() throws Exception;
}
