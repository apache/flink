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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.GROUPING_KEY;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.JOB_NAME;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.PORT;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX;

/** {@link MetricReporterFactory} for {@link PrometheusPushGatewayReporter}. */
@InterceptInstantiationViaReflection(
        reporterClassName = "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter")
public class PrometheusPushGatewayReporterFactory implements MetricReporterFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(PrometheusPushGatewayReporterFactory.class);

    @Override
    public PrometheusPushGatewayReporter createMetricReporter(Properties properties) {
        MetricConfig metricConfig = (MetricConfig) properties;
        String host = metricConfig.getString(HOST.key(), HOST.defaultValue());
        int port = metricConfig.getInteger(PORT.key(), PORT.defaultValue());
        String configuredJobName = metricConfig.getString(JOB_NAME.key(), JOB_NAME.defaultValue());
        boolean randomSuffix =
                metricConfig.getBoolean(
                        RANDOM_JOB_NAME_SUFFIX.key(), RANDOM_JOB_NAME_SUFFIX.defaultValue());
        boolean deleteOnShutdown =
                metricConfig.getBoolean(
                        DELETE_ON_SHUTDOWN.key(), DELETE_ON_SHUTDOWN.defaultValue());
        Map<String, String> groupingKey =
                parseGroupingKey(
                        metricConfig.getString(GROUPING_KEY.key(), GROUPING_KEY.defaultValue()));

        if (host == null || host.isEmpty() || port < 1) {
            throw new IllegalArgumentException(
                    "Invalid host/port configuration. Host: " + host + " Port: " + port);
        }

        String jobName = configuredJobName;
        if (randomSuffix) {
            jobName = configuredJobName + new AbstractID();
        }

        LOG.info(
                "Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName:{}, randomJobNameSuffix:{}, deleteOnShutdown:{}, groupingKey:{}}",
                host,
                port,
                jobName,
                randomSuffix,
                deleteOnShutdown,
                groupingKey);

        return new PrometheusPushGatewayReporter(
                host, port, jobName, groupingKey, deleteOnShutdown);
    }

    @VisibleForTesting
    static Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
        if (!groupingKeyConfig.isEmpty()) {
            Map<String, String> groupingKey = new HashMap<>();
            String[] kvs = groupingKeyConfig.split(";");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    LOG.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
                    continue;
                }

                String labelKey = kv.substring(0, idx);
                String labelValue = kv.substring(idx + 1);
                if (StringUtils.isNullOrWhitespaceOnly(labelKey)
                        || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
                    LOG.warn(
                            "Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty",
                            labelKey,
                            labelValue);
                    continue;
                }
                groupingKey.put(labelKey, labelValue);
            }

            return groupingKey;
        }

        return Collections.emptyMap();
    }
}
