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
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

/** Tests for the {@link JMXReporterFactory}. */
public class JMXReporterFactoryTest extends TestLogger {

    @After
    public void shutdownService() throws IOException {
        JMXService.stopInstance();
    }

    @Test
    public void testPortRangeArgument() {
        Properties properties = new Properties();
        properties.setProperty(JMXReporterFactory.ARG_PORT, "9000-9010");

        JMXReporter metricReporter = new JMXReporterFactory().createMetricReporter(properties);
        try {

            Assert.assertThat(
                    metricReporter.getPort().get(),
                    allOf(greaterThanOrEqualTo(9000), lessThanOrEqualTo(9010)));
        } finally {
            metricReporter.close();
        }
    }

    @Test
    public void testWithoutArgument() {
        JMXReporter metricReporter =
                new JMXReporterFactory().createMetricReporter(new Properties());

        try {
            Assert.assertFalse(metricReporter.getPort().isPresent());
        } finally {
            metricReporter.close();
        }
    }

    @Test
    public void testMetricReporterSetupViaSPI() {
        MetricReporterTestUtils.testMetricReporterSetupViaSPI(JMXReporterFactory.class);
    }
}
