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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.runtime.metrics.filter.MetricFilter;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Set;

/** Encapsulates all settings that are defined per reporter. */
public class ReporterScopedSettings {

    private final int reporterIndex;

    private final char delimiter;

    private final Set<String> excludedVariables;

    private final MetricFilter filter;

    private final Map<String, String> additionalVariables;

    public ReporterScopedSettings(
            int reporterIndex,
            char delimiter,
            MetricFilter filter,
            Set<String> excludedVariables,
            Map<String, String> additionalVariables) {
        this.excludedVariables = excludedVariables;
        Preconditions.checkArgument(reporterIndex >= 0);
        this.reporterIndex = reporterIndex;
        this.delimiter = delimiter;
        this.filter = filter;
        this.additionalVariables = additionalVariables;
    }

    public int getReporterIndex() {
        return reporterIndex;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public MetricFilter getFilter() {
        return filter;
    }

    public Set<String> getExcludedVariables() {
        return excludedVariables;
    }

    public Map<String, String> getAdditionalVariables() {
        return additionalVariables;
    }
}
