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

package org.apache.flink.metrics.datadog;

import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** {@link MetricReporterFactory} for {@link DatadogHttpReporter}. */
public class DatadogHttpReporterFactory implements MetricReporterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DatadogHttpReporterFactory.class);

    private static final String API_KEY = "apikey";
    private static final String PROXY_HOST = "proxyHost";
    private static final String PROXY_PORT = "proxyPort";
    private static final String DATA_CENTER = "dataCenter";
    private static final String TAGS = "tags";
    private static final String MAX_METRICS_PER_REQUEST = "maxMetricsPerRequest";
    private static final String USE_LOGICAL_IDENTIFIER = "useLogicalIdentifier";

    @Override
    public MetricReporter createMetricReporter(Properties config) {
        final String apiKey = config.getProperty(API_KEY, null);
        final String proxyHost = config.getProperty(PROXY_HOST, null);
        final int proxyPort = Integer.valueOf(config.getProperty(PROXY_PORT, "8080"));
        final String rawDataCenter = config.getProperty(DATA_CENTER, "US");
        final int maxMetricsPerRequestValue =
                Integer.valueOf(config.getProperty(MAX_METRICS_PER_REQUEST, "2000"));
        final DataCenter dataCenter = DataCenter.valueOf(rawDataCenter);
        if (config.containsKey(TAGS)) {
            LOG.warn(
                    "The 'tags' option is deprecated; please use 'scope.variables.additional' instead.");
        }
        final String tags = config.getProperty(TAGS, "");
        final boolean useLogicalIdentifier =
                Boolean.parseBoolean(config.getProperty(USE_LOGICAL_IDENTIFIER, "false"));

        return new DatadogHttpReporter(
                apiKey,
                proxyHost,
                proxyPort,
                maxMetricsPerRequestValue,
                dataCenter,
                tags,
                useLogicalIdentifier);
    }
}
