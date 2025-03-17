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

import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.filter.MetricFilter;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Setup for {@link MetricReporter}. */
public final class ReporterSetup extends AbstractReporterSetup<MetricReporter> {

    public ReporterSetup(
            final String name,
            final MetricConfig configuration,
            MetricReporter reporter,
            final MetricFilter filter,
            final Map<String, String> additionalVariables) {
        super(name, configuration, reporter, filter, additionalVariables);
    }

    public Optional<String> getDelimiter() {
        return Optional.ofNullable(
                configuration.getString(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), null));
    }

    public Optional<String> getIntervalSettings() {
        return Optional.ofNullable(
                configuration.getString(MetricOptions.REPORTER_INTERVAL.key(), null));
    }

    public Set<String> getExcludedVariables() {
        String excludedVariablesList =
                configuration.getString(MetricOptions.REPORTER_EXCLUDED_VARIABLES.key(), null);
        if (excludedVariablesList == null) {
            return Collections.emptySet();
        } else {
            final Set<String> excludedVariables = new HashSet<>();
            for (String exclusion : excludedVariablesList.split(";")) {
                excludedVariables.add(ScopeFormat.asVariable(exclusion));
            }
            return Collections.unmodifiableSet(excludedVariables);
        }
    }
}
