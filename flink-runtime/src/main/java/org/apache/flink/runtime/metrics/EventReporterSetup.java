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
import org.apache.flink.configuration.EventOptions;
import org.apache.flink.events.EventBuilder;
import org.apache.flink.events.reporter.EventReporter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.runtime.metrics.filter.ReporterFilter;

import java.util.Map;

/** Setup for {@link org.apache.flink.events.reporter.EventReporter}. */
public final class EventReporterSetup extends AbstractReporterSetup<EventReporter, EventBuilder> {

    public EventReporterSetup(
            final String name,
            final MetricConfig configuration,
            EventReporter reporter,
            ReporterFilter<EventBuilder> eventFilter,
            final Map<String, String> additionalVariables) {
        super(name, configuration, reporter, eventFilter, additionalVariables);
    }

    @Override
    protected ConfigOption<String> getDelimiterConfigOption() {
        return EventOptions.REPORTER_SCOPE_DELIMITER;
    }

    @Override
    protected ConfigOption<String> getExcludedVariablesConfigOption() {
        return EventOptions.REPORTER_EXCLUDED_VARIABLES;
    }
}
