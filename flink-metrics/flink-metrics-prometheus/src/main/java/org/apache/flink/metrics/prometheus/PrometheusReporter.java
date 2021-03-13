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
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus. */
@PublicEvolving
@InstantiateViaFactory(
        factoryClassName = "org.apache.flink.metrics.prometheus.PrometheusReporterFactory")
public class PrometheusReporter extends AbstractPrometheusReporter {

    private HTTPServer httpServer;
    private int port;

    @VisibleForTesting
    int getPort() {
        Preconditions.checkState(httpServer != null, "Server has not been initialized.");
        return port;
    }

    PrometheusReporter(
        final int port, 
        @Nullable HTTPServer httpServer) {
        this.httpServer = Preconditions.checkNotNull(httpServer);
        this.port = port;
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop();
        }

        super.close();
    }
}
