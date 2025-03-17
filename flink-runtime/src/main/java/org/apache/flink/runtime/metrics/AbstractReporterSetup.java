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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.runtime.metrics.filter.ReporterFilter;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Abstract base class for reporter setups.
 *
 * @param <REPORTER> generic type of the reporter.
 * @param <REPORTED> generic type of what is reported.
 */
public abstract class AbstractReporterSetup<REPORTER, REPORTED> {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractReporterSetup.class);

    protected final String name;
    protected final MetricConfig configuration;
    protected final REPORTER reporter;
    protected final ReporterFilter<REPORTED> filter;
    protected final Map<String, String> additionalVariables;

    public AbstractReporterSetup(
            final String name,
            final MetricConfig configuration,
            REPORTER reporter,
            ReporterFilter<REPORTED> filter,
            final Map<String, String> additionalVariables) {
        this.name = name;
        this.configuration = configuration;
        this.reporter = reporter;
        this.filter = filter;
        this.additionalVariables = additionalVariables;
    }

    public Map<String, String> getAdditionalVariables() {
        return additionalVariables;
    }

    public String getName() {
        return name;
    }

    @VisibleForTesting
    MetricConfig getConfiguration() {
        return configuration;
    }

    public REPORTER getReporter() {
        return reporter;
    }

    public ReporterFilter<REPORTED> getFilter() {
        return filter;
    }

    public Optional<String> getDelimiter() {
        return Optional.ofNullable(configuration.getString(getDelimiterConfigOption().key(), null));
    }

    public Set<String> getExcludedVariables() {
        String excludedVariablesList =
                configuration.getString(getExcludedVariablesConfigOption().key(), null);
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

    protected abstract ConfigOption<String> getDelimiterConfigOption();

    protected abstract ConfigOption<String> getExcludedVariablesConfigOption();
}
