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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.GROUPING_KEY;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.JOB_NAME;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.PORT;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus {@link PushGateway}.
 */
@PublicEvolving
@InstantiateViaFactory(
        factoryClassName =
                "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterFactory")
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {

    private PushGateway pushGateway;
    private String jobName;
    private boolean deleteOnShutdown;
    private Map<String, String> groupingKey;

    PrometheusPushGatewayReporter(
        @Nullable final String hostConfig,
        @Nullable final int portConfig,
        @Nullable final String jobNameConfig,
        @Nullable final boolean randomJobSuffixConfig,
        @Nullable final boolean deleteOnShutdownConfig,
        @Nullable final Map<String, String> groupingKeyConfig
    ) {
        deleteOnShutdown = deleteOnShutdownConfig
        groupingKey = parseGroupingKey(groupingKeyConfig)

        if (hostConfig == null || hostConfig.isEmpty() || portConfig < 1) {
            throw new IllegalArgumentException(
                    "Invalid host/port configuration. Host: " + hostConfig + " Port: " + portConfig);
        }

        if (randomJobSuffixConfig) {
            this.jobName = jobNameConfig + new AbstractID();
        } else {
            this.jobName = jobNameConfig;
        }

        pushGateway = new PushGateway(hostConfig + ':' + portConfig);
        log.info(
                "Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName:{}, randomJobNameSuffix:{}, deleteOnShutdown:{}, groupingKey:{}}",
                hostConfig,
                portConfig,
                jobNameConfig,
                randomJobSuffixConfig,
                deleteOnShutdown,
                groupingKey);
    }

    @Override
    public void open(MetricConfig config) { }

    Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
        if (!groupingKeyConfig.isEmpty()) {
            Map<String, String> groupingKey = new HashMap<>();
            String[] kvs = groupingKeyConfig.split(";");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    log.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
                    continue;
                }

                String labelKey = kv.substring(0, idx);
                String labelValue = kv.substring(idx + 1);
                if (StringUtils.isNullOrWhitespaceOnly(labelKey)
                        || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
                    log.warn(
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

    @Override
    public void report() {
        try {
            pushGateway.push(CollectorRegistry.defaultRegistry, jobName, groupingKey);
        } catch (Exception e) {
            log.warn(
                    "Failed to push metrics to PushGateway with jobName {}, groupingKey {}.",
                    jobName,
                    groupingKey,
                    e);
        }
    }

    @Override
    public void close() {
        if (deleteOnShutdown && pushGateway != null) {
            try {
                pushGateway.delete(jobName, groupingKey);
            } catch (IOException e) {
                log.warn(
                        "Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.",
                        jobName,
                        groupingKey,
                        e);
            }
        }
        super.close();
    }
}
