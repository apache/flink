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

package org.apache.flink.events.otel;

import org.apache.flink.events.Event;
import org.apache.flink.events.reporter.EventReporter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.otel.OpenTelemetryReporterBase;
import org.apache.flink.metrics.otel.VariableNameUtil;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporterBuilder;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.tryConfigureEndpoint;
import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.tryConfigureTimeout;

/**
 * A Flink {@link EventReporter} which is made to export log records/events using Open Telemetry's
 * {@link LogRecordExporter}.
 */
public class OpenTelemetryEventReporter extends OpenTelemetryReporterBase implements EventReporter {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryEventReporter.class);
    private LogRecordExporter logRecordExporter;
    private SdkLoggerProvider loggerProvider;
    private BatchLogRecordProcessor logRecordProcessor;

    public OpenTelemetryEventReporter() {
        super();
    }

    @Override
    public void open(MetricConfig metricConfig) {
        LOG.info("Starting OpenTelemetryEventReporter");
        OtlpGrpcLogRecordExporterBuilder builder = OtlpGrpcLogRecordExporter.builder();
        tryConfigureEndpoint(metricConfig, builder::setEndpoint);
        tryConfigureTimeout(metricConfig, builder::setTimeout);

        logRecordExporter = builder.build();
        logRecordProcessor = BatchLogRecordProcessor.builder(logRecordExporter).build();
        loggerProvider =
                SdkLoggerProvider.builder()
                        .addLogRecordProcessor(logRecordProcessor)
                        .setResource(resource)
                        .build();
    }

    @Override
    public void close() {
        logRecordProcessor.forceFlush();
        logRecordProcessor.close();
        logRecordExporter.flush();
        logRecordExporter.close();
    }

    @Override
    public void notifyOfAddedEvent(Event event) {
        io.opentelemetry.api.logs.Logger logger = loggerProvider.get(event.getClassScope());
        LogRecordBuilder logRecordBuilder = logger.logRecordBuilder();

        event.getAttributes().forEach(setAttribute(logRecordBuilder));

        logRecordBuilder.setObservedTimestamp(event.getObservedTsMillis(), TimeUnit.MILLISECONDS);

        logRecordBuilder.setBody(event.getBody());
        logRecordBuilder.setSeverityText(event.getSeverity());
        try {
            logRecordBuilder.setSeverity(Severity.valueOf(event.getSeverity()));
        } catch (IllegalArgumentException iae) {
            logRecordBuilder.setSeverity(Severity.UNDEFINED_SEVERITY_NUMBER);
        }

        logRecordBuilder.setTimestamp(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        logRecordBuilder.emit();
    }

    private static BiConsumer<String, Object> setAttribute(LogRecordBuilder logRecordBuilder) {
        return (key, value) -> {
            key = VariableNameUtil.getVariableName(key);
            if (value instanceof String) {
                logRecordBuilder.setAttribute(AttributeKey.stringKey(key), (String) value);
            } else if (value instanceof Integer) {
                Long longValue = ((Integer) value).longValue();
                logRecordBuilder.setAttribute(AttributeKey.longKey(key), longValue);
            } else if (value instanceof Long) {
                logRecordBuilder.setAttribute(AttributeKey.longKey(key), (Long) value);
            } else if (value instanceof Float) {
                Double doubleValue = ((Float) value).doubleValue();
                logRecordBuilder.setAttribute(AttributeKey.doubleKey(key), doubleValue);
            } else if (value instanceof Double) {
                logRecordBuilder.setAttribute(AttributeKey.doubleKey(key), (Double) value);
            } else if (value instanceof Boolean) {
                logRecordBuilder.setAttribute(AttributeKey.booleanKey(key), (Boolean) value);
            } else {
                LOG.warn("Unsupported attribute type [{}={}]", key, value);
            }
        };
    }
}
