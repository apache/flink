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
import org.apache.flink.metrics.reporter.MetricReporter;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink {@link MetricReporter} which is made to export metrics using Open Telemetry's {@link
 * MetricExporter}.
 */
public abstract class OpenTelemetryReporterBase {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryReporterBase.class);

    protected Resource resource;
    protected MetricExporter exporter;

    protected OpenTelemetryReporterBase() {
        resource = Resource.getDefault();
    }

    protected void open(MetricConfig metricConfig) {
        if (metricConfig.containsKey(OpenTelemetryReporterOptions.SERVICE_NAME.key())) {
            resource =
                    resource.merge(
                            Resource.create(
                                    Attributes.of(
                                            ResourceAttributes.SERVICE_NAME,
                                            metricConfig.getString(
                                                    OpenTelemetryReporterOptions.SERVICE_NAME.key(),
                                                    OpenTelemetryReporterOptions.SERVICE_NAME
                                                            .defaultValue()))));
        }
        if (metricConfig.containsKey(OpenTelemetryReporterOptions.SERVICE_VERSION.key())) {
            resource =
                    resource.merge(
                            Resource.create(
                                    Attributes.of(
                                            ResourceAttributes.SERVICE_VERSION,
                                            metricConfig.getString(
                                                    OpenTelemetryReporterOptions.SERVICE_VERSION
                                                            .key(),
                                                    OpenTelemetryReporterOptions.SERVICE_VERSION
                                                            .defaultValue()))));
        }
    }
}
