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
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST_URL;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.PORT;

/** Test for {@link PrometheusPushGatewayReporter}. */
public class PrometheusPushGatewayReporterTest extends TestLogger {

    @Test
    public void testParseGroupingKey() {
        Map<String, String> groupingKey =
                PrometheusPushGatewayReporterFactory.parseGroupingKey("k1=v1;k2=v2");
        Assert.assertNotNull(groupingKey);
        Assert.assertEquals("v1", groupingKey.get("k1"));
        Assert.assertEquals("v2", groupingKey.get("k2"));
    }

    @Test
    public void testParseIncompleteGroupingKey() {
        Map<String, String> groupingKey =
                PrometheusPushGatewayReporterFactory.parseGroupingKey("k1=");
        Assert.assertTrue(groupingKey.isEmpty());

        groupingKey = PrometheusPushGatewayReporterFactory.parseGroupingKey("=v1");
        Assert.assertTrue(groupingKey.isEmpty());

        groupingKey = PrometheusPushGatewayReporterFactory.parseGroupingKey("k1");
        Assert.assertTrue(groupingKey.isEmpty());
    }

    @Test
    public void testConnectToPushGatewayUsingHostAndPort() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST.key(), "localhost");
        metricConfig.setProperty(PORT.key(), "18080");
        PrometheusPushGatewayReporter reporter = factory.createMetricReporter(metricConfig);
        String gatewayBaseURL = factory.createMetricReporter(metricConfig).hostUrl.toString();
        Assert.assertEquals(gatewayBaseURL, "http://localhost:18080");
    }

    @Test
    public void testConnectToPushGatewayUsingHostUrl() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST_URL.key(), "https://localhost:18080");
        PrometheusPushGatewayReporter reporter = factory.createMetricReporter(metricConfig);
        String gatewayBaseURL = factory.createMetricReporter(metricConfig).hostUrl.toString();
        Assert.assertEquals(gatewayBaseURL, "https://localhost:18080");
    }

    @Test
    public void testConnectToPushGatewayPreferHostUrl() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST_URL.key(), "https://localhost:18080");
        metricConfig.setProperty(HOST.key(), "localhost1");
        metricConfig.setProperty(PORT.key(), "18081");
        String gatewayBaseURL = factory.createMetricReporter(metricConfig).hostUrl.toString();
        Assert.assertEquals(gatewayBaseURL, "https://localhost:18080");
    }

    @Test
    public void testConnectToPushGatewayThrowsExceptionWithoutHostInformation() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        Assert.assertThrows(
                IllegalArgumentException.class, () -> factory.createMetricReporter(metricConfig));

        metricConfig.setProperty(HOST.key(), "localhost");
        Assert.assertThrows(
                IllegalArgumentException.class, () -> factory.createMetricReporter(metricConfig));

        metricConfig.clear();
        metricConfig.setProperty(PORT.key(), "18080");
        Assert.assertThrows(
                IllegalArgumentException.class, () -> factory.createMetricReporter(metricConfig));
    }
}
