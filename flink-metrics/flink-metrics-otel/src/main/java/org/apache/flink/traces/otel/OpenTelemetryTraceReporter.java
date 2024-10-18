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

package org.apache.flink.traces.otel;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.otel.OpenTelemetryReporterBase;
import org.apache.flink.metrics.otel.VariableNameUtil;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.reporter.TraceReporter;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.tryConfigureEndpoint;
import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.tryConfigureTimeout;

/**
 * A Flink {@link org.apache.flink.traces.reporter.TraceReporter} which is made to export spans
 * using Open Telemetry's {@link SpanExporter}.
 */
public class OpenTelemetryTraceReporter extends OpenTelemetryReporterBase implements TraceReporter {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryTraceReporter.class);
    private SpanExporter spanExporter;
    private TracerProvider tracerProvider;
    private BatchSpanProcessor spanProcessor;

    @Override
    public void open(MetricConfig metricConfig) {
        LOG.info("Starting OpenTelemetryTraceReporter");
        super.open(metricConfig);
        OtlpGrpcSpanExporterBuilder builder = OtlpGrpcSpanExporter.builder();
        tryConfigureEndpoint(metricConfig, builder::setEndpoint);
        tryConfigureTimeout(metricConfig, builder::setTimeout);
        spanExporter = builder.build();
        spanProcessor = BatchSpanProcessor.builder(spanExporter).build();
        tracerProvider =
                SdkTracerProvider.builder()
                        .addSpanProcessor(spanProcessor)
                        .setResource(resource)
                        .build();
    }

    @Override
    public void close() {
        spanProcessor.forceFlush();
        spanProcessor.close();
        spanExporter.flush();
        spanExporter.close();
    }

    private void notifyOfAddedSpanInternal(Span span, io.opentelemetry.api.trace.Span parent) {

        Tracer tracer = tracerProvider.get(span.getScope());
        SpanBuilder spanBuilder = tracer.spanBuilder(span.getName());

        span.getAttributes().forEach(setAttribute(spanBuilder));

        if (parent == null) {
            // root span case
            spanBuilder.setNoParent();
        } else {
            // child / nested span case
            spanBuilder.setParent(Context.current().with(parent));
        }

        io.opentelemetry.api.trace.Span currentOtelSpan =
                spanBuilder
                        .setStartTimestamp(span.getStartTsMillis(), TimeUnit.MILLISECONDS)
                        .startSpan();

        // Recursively add child spans to this parent
        // TODO: not yet supported
        // for (Span childSpan : span.getChildren()) {
        //    notifyOfAddedSpanInternal(childSpan, currentOtelSpan);
        // }

        currentOtelSpan.end(span.getEndTsMillis(), TimeUnit.MILLISECONDS);
    }

    private static BiConsumer<String, Object> setAttribute(SpanBuilder spanBuilder) {
        return (key, value) -> {
            key = VariableNameUtil.getVariableName(key);
            if (value instanceof String) {
                spanBuilder.setAttribute(key, (String) value);
            } else if (value instanceof Long) {
                spanBuilder.setAttribute(key, (Long) value);
            } else if (value instanceof Double) {
                spanBuilder.setAttribute(key, (Double) value);
            } else {
                LOG.warn("Unsupported attribute type [{}={}]", key, value);
            }
        };
    }

    @Override
    public void notifyOfAddedSpan(org.apache.flink.traces.Span span) {
        notifyOfAddedSpanInternal(span, null);
    }
}
