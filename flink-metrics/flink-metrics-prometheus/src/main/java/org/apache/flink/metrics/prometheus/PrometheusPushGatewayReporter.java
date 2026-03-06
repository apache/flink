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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import io.prometheus.client.exporter.BasicAuthHttpConnectionFactory;
import io.prometheus.client.exporter.PushGateway;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus {@link PushGateway}.
 */
@PublicEvolving
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {
    public static final String REPORTER_ID_GROUPPING_KEY =
            "flink_prometheus_push_gateway_reporter_id";

    private final PushGateway pushGateway;
    private final String jobName;
    private final Map<String, String> groupingKey;
    private final boolean deleteOnShutdown;
    @VisibleForTesting final URL hostUrl;
    @VisibleForTesting final boolean basicAuthEnabled;

    PrometheusPushGatewayReporter(
            URL hostUrl,
            String jobName,
            Map<String, String> groupingKey,
            final boolean deleteOnShutdown,
            @Nullable String username,
            @Nullable String password) {
        this.hostUrl = hostUrl;
        this.pushGateway = new PushGateway(hostUrl);
        if (username != null && password != null) {
            this.pushGateway.setConnectionFactory(
                    new BasicAuthHttpConnectionFactory(username, password));
            this.basicAuthEnabled = true;
        } else {
            this.basicAuthEnabled = false;
        }
        this.jobName = Preconditions.checkNotNull(jobName);
        this.groupingKey = new GroupingKeyMap(Preconditions.checkNotNull(groupingKey));
        this.deleteOnShutdown = deleteOnShutdown;
    }

    @Override
    public void report() {
        try {
            pushGateway.push(registry, jobName, groupingKey);
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

    /**
     * (FLINK-21309) Put "flink_prometheus_push_gateway_reporter_id" as the last entry of the
     * grouping keys, so that each taskmanger instance and jobmanager will not collide their metrics
     * in the PushGateway.
     */
    static class GroupingKeyMap extends AbstractMap<String, String> {
        private final Map<String, String> customGroupingKeys;
        private final String reporterId = new AbstractID().toString();
        private final Set<Entry<String, String>> entrySet = new GroupingKeySet();

        GroupingKeyMap(Map<String, String> customGroupingKeys) {
            if (customGroupingKeys.containsKey(REPORTER_ID_GROUPPING_KEY)) {
                throw new IllegalArgumentException(
                        "Grouping keys must not contain the reserved key: "
                                + REPORTER_ID_GROUPPING_KEY);
            }
            this.customGroupingKeys = customGroupingKeys;
        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            return entrySet;
        }

        private class GroupingKeySet extends AbstractSet<Map.Entry<String, String>> {
            @Override
            public Iterator<Entry<String, String>> iterator() {

                return new Iterator<>() {
                    private Iterator<Entry<String, String>> customEntryIterator =
                            customGroupingKeys.entrySet().iterator();
                    private boolean consumedLast = false;

                    @Override
                    public boolean hasNext() {
                        return customEntryIterator.hasNext() || !consumedLast;
                    }

                    @Override
                    public Entry<String, String> next() {
                        if (customEntryIterator.hasNext()) {
                            return customEntryIterator.next();
                        }
                        if (!consumedLast) {
                            consumedLast = true;
                            return new AbstractMap.SimpleEntry<>(
                                    REPORTER_ID_GROUPPING_KEY, reporterId);
                        }
                        throw new NoSuchElementException();
                    }
                };
            }

            @Override
            public int size() {
                return customGroupingKeys.size() + 1;
            }
        }

        String reporterId() {
            return reporterId;
        }
    }
}
