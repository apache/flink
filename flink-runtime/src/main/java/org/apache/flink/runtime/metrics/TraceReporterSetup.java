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

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.runtime.metrics.filter.ReporterFilter;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.traces.reporter.TraceReporter;

import java.util.Map;

/** Setup for {@link org.apache.flink.traces.reporter.TraceReporter}. */
public final class TraceReporterSetup extends AbstractReporterSetup<TraceReporter, SpanBuilder> {

    public TraceReporterSetup(
            final String name,
            final MetricConfig configuration,
            TraceReporter reporter,
            ReporterFilter<SpanBuilder> spanFilter,
            final Map<String, String> additionalVariables) {
        super(name, configuration, reporter, spanFilter, additionalVariables);
    }

    @Override
    protected ConfigOption<String> getDelimiterConfigOption() {
        return TraceOptions.REPORTER_SCOPE_DELIMITER;
    }

    @Override
    protected ConfigOption<String> getExcludedVariablesConfigOption() {
        return TraceOptions.REPORTER_EXCLUDED_VARIABLES;
    }
}
