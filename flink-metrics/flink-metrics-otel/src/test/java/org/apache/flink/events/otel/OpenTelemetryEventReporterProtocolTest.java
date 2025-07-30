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
import org.apache.flink.events.EventBuilder;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.otel.AbstractOpenTelemetryReporterProtocolTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the OpenTelemetry event reporter protocol configuration. */
public class OpenTelemetryEventReporterProtocolTest
        extends AbstractOpenTelemetryReporterProtocolTest<OpenTelemetryEventReporter> {

    private static final String TEST_EVENT_NAME = "test.event";
    private static final Duration OBSERVED_TS = Duration.ofMillis(42);

    @Override
    protected OpenTelemetryEventReporter createReporter() {
        return new OpenTelemetryEventReporter();
    }

    @Override
    protected void closeReporter(OpenTelemetryEventReporter reporter) {
        reporter.close();
    }

    @Override
    protected void setupAndReport(MetricConfig config) {
        reporter.open(config);
        EventBuilder eventBuilder =
                Event.builder(this.getClass(), TEST_EVENT_NAME)
                        .setObservedTsMillis(OBSERVED_TS.toMillis());
        reporter.notifyOfAddedEvent(eventBuilder.build());
    }

    @Override
    protected void assertReported() throws Exception {
        eventuallyConsumeJson(json -> assertThat(json.toString()).contains(TEST_EVENT_NAME));
    }
}
