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

package org.apache.flink.metrics.jmx;

import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.metrics.util.MetricReporterTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JMXReporterFactory}. */
class JMXReporterFactoryTest {

    @AfterEach
    void shutdownService() throws IOException {
        JMXService.stopInstance();
    }

    @Test
    void testPortRangeArgument() {
        Properties properties = new Properties();
        properties.setProperty(JMXReporterFactory.ARG_PORT, "9000-9010");

        JMXReporter metricReporter = new JMXReporterFactory().createMetricReporter(properties);
        try {
            assertThat(metricReporter.getPort())
                    .hasValueSatisfying(
                            port ->
                                    assertThat(port)
                                            .isGreaterThanOrEqualTo(9000)
                                            .isLessThanOrEqualTo(9010));
        } finally {
            metricReporter.close();
        }
    }

    @Test
    void testWithoutArgument() {
        JMXReporter metricReporter =
                new JMXReporterFactory().createMetricReporter(new Properties());

        try {
            assertThat(metricReporter.getPort()).isEmpty();
        } finally {
            metricReporter.close();
        }
    }

    @Test
    void testMetricReporterSetupViaSPI() {
        MetricReporterTestUtils.testMetricReporterSetupViaSPI(JMXReporterFactory.class);
    }
}
