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
import io.prometheus.client.exporter.BasicAuthHttpConnectionFactory;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    private Map<String, String> groupingKey;
    private boolean deleteOnShutdown;

    public void open(MetricConfig config) {
        super.open(config);
        String host =
                config.getString(
                        PrometheusPushGatewayReporterOptions.HOST.key(),
                        (String) PrometheusPushGatewayReporterOptions.HOST.defaultValue());
        int port =
                config.getInteger(
                        PrometheusPushGatewayReporterOptions.PORT.key(),
                        (Integer) PrometheusPushGatewayReporterOptions.PORT.defaultValue());
        String configuredJobName =
                config.getString(
                        PrometheusPushGatewayReporterOptions.JOB_NAME.key(),
                        (String) PrometheusPushGatewayReporterOptions.JOB_NAME.defaultValue());
        boolean randomSuffix =
                config.getBoolean(
                        PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX.key(),
                        (Boolean)
                                PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX
                                        .defaultValue());
        this.deleteOnShutdown =
                config.getBoolean(
                        PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN.key(),
                        (Boolean)
                                PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN
                                        .defaultValue());
        this.groupingKey =
                this.parseGroupingKey(
                        config.getString(
                                PrometheusPushGatewayReporterOptions.GROUPING_KEY.key(),
                                (String)
                                        PrometheusPushGatewayReporterOptions.GROUPING_KEY
                                                .defaultValue()));
        if (host != null && !host.isEmpty() && port >= 1) {
            if (randomSuffix) {
                this.jobName = configuredJobName + new AbstractID();
            } else {
                this.jobName = configuredJobName;
            }

            this.pushGateway = new PushGateway(host + ':' + port);
            boolean needBasicAuth =
                    config.getBoolean(
                            PrometheusPushGatewayReporterOptions.NEED_BASIC_AUTH.key(),
                            (Boolean)
                                    PrometheusPushGatewayReporterOptions.NEED_BASIC_AUTH
                                            .defaultValue());
            if (needBasicAuth) {
                String user =
                        config.getString(
                                PrometheusPushGatewayReporterOptions.USERNAME.key(),
                                (String)
                                        PrometheusPushGatewayReporterOptions.USERNAME
                                                .defaultValue());
                String password =
                        config.getString(
                                PrometheusPushGatewayReporterOptions.PASSWORD.key(),
                                (String)
                                        PrometheusPushGatewayReporterOptions.PASSWORD
                                                .defaultValue());
                this.pushGateway.setConnectionFactory(
                        new BasicAuthHttpConnectionFactory(user, password));
            }

            this.log.info(
                    "Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName:{}, randomJobNameSuffix:{}, deleteOnShutdown:{}, groupingKey:{}, needBasicAuth: {}",
                    host,
                    port,
                    this.jobName,
                    randomSuffix,
                    this.deleteOnShutdown,
                    this.groupingKey,
                    needBasicAuth);
        } else {
            throw new IllegalArgumentException(
                    "Invalid host/port configuration. Host: " + host + " Port: " + port);
        }
    }

    Map<String, String> parseGroupingKey(String groupingKeyConfig) {
        if (groupingKeyConfig.isEmpty()) {
            return Collections.emptyMap();
        } else {
            Map<String, String> groupingKey = new HashMap();
            String[] kvs = groupingKeyConfig.split(";");
            String[] var4 = kvs;
            int var5 = kvs.length;

            for (int var6 = 0; var6 < var5; ++var6) {
                String kv = var4[var6];
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    this.log.warn(
                            "Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
                } else {
                    String labelKey = kv.substring(0, idx);
                    String labelValue = kv.substring(idx + 1);
                    if (!StringUtils.isNullOrWhitespaceOnly(labelKey)
                            && !StringUtils.isNullOrWhitespaceOnly(labelValue)) {
                        groupingKey.put(labelKey, labelValue);
                    } else {
                        this.log.warn(
                                "Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty",
                                labelKey,
                                labelValue);
                    }
                }
            }

            return groupingKey;
        }
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
