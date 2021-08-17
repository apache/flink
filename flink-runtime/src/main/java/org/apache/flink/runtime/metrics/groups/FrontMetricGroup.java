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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;

/**
 * Metric group which forwards all registration calls to a variable parent metric group that injects
 * a variable reporter index into calls to {@link
 * org.apache.flink.metrics.MetricGroup#getMetricIdentifier(String)} or {@link
 * org.apache.flink.metrics.MetricGroup#getMetricIdentifier(String, CharacterFilter)}. This allows
 * us to use reporter-specific delimiters, without requiring any action by the reporter.
 *
 * @param <P> parentMetricGroup to {@link AbstractMetricGroup AbstractMetricGroup}
 */
public class FrontMetricGroup<P extends AbstractMetricGroup<?>> extends ProxyMetricGroup<P>
        implements LogicalScopeProvider {

    @VisibleForTesting static final char DEFAULT_REPLACEMENT = '_';
    @VisibleForTesting static final char DEFAULT_REPLACEMENT_ALTERNATIVE = '-';

    private final ReporterScopedSettings settings;

    public FrontMetricGroup(ReporterScopedSettings settings, P reference) {
        super(reference);
        this.settings = settings;
    }

    @Override
    public String getMetricIdentifier(String metricName) {
        return parentMetricGroup.getMetricIdentifier(
                metricName,
                getDelimiterFilter(this.settings, CharacterFilter.NO_OP_FILTER),
                this.settings.getReporterIndex(),
                this.settings.getDelimiter());
    }

    @Override
    public String getMetricIdentifier(String metricName, CharacterFilter filter) {
        return parentMetricGroup.getMetricIdentifier(
                metricName,
                getDelimiterFilter(this.settings, filter),
                this.settings.getReporterIndex(),
                this.settings.getDelimiter());
    }

    @Override
    public MetricGroup getWrappedMetricGroup() {
        return parentMetricGroup;
    }

    @Override
    public Map<String, String> getAllVariables() {
        return parentMetricGroup.getAllVariables(
                this.settings.getReporterIndex(), this.settings.getExcludedVariables());
    }

    /** @deprecated work against the LogicalScopeProvider interface instead. */
    @Override
    @Deprecated
    public String getLogicalScope(CharacterFilter filter) {
        return parentMetricGroup.getLogicalScope(
                getDelimiterFilter(this.settings, filter), this.settings.getDelimiter());
    }

    /** @deprecated work against the LogicalScopeProvider interface instead. */
    @Override
    @Deprecated
    public String getLogicalScope(CharacterFilter filter, char delimiter) {
        return parentMetricGroup.getLogicalScope(
                getDelimiterFilter(this.settings, filter),
                delimiter,
                this.settings.getReporterIndex());
    }

    private static CharacterFilter getDelimiterFilter(
            ReporterScopedSettings reporterScopedSettings, CharacterFilter generalCharacterFilter) {

        if (reporterScopedSettings.getDelimiter() != DEFAULT_REPLACEMENT) {
            return input ->
                    generalCharacterFilter.filterCharacters(
                            input.replace(
                                    reporterScopedSettings.getDelimiter(), DEFAULT_REPLACEMENT));
        } else {
            return input ->
                    generalCharacterFilter.filterCharacters(
                            input.replace(
                                    reporterScopedSettings.getDelimiter(),
                                    DEFAULT_REPLACEMENT_ALTERNATIVE));
        }
    }
}
