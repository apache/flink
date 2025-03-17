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
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.Reporter;
import org.apache.flink.runtime.metrics.filter.MetricFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Abstract base class for reporter setups.
 *
 * @param <REPORTER> generic type of the created {@link Reporter}.
 */
public abstract class AbstractReporterSetup<REPORTER extends Reporter> {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractReporterSetup.class);

    protected final String name;
    protected final MetricConfig configuration;
    protected final REPORTER reporter;
    protected final MetricFilter filter;
    protected final Map<String, String> additionalVariables;

    public AbstractReporterSetup(
            final String name,
            final MetricConfig configuration,
            REPORTER reporter,
            MetricFilter filter,
            final Map<String, String> additionalVariables) {
        this.name = name;
        this.configuration = configuration;
        this.reporter = reporter;
        this.filter = filter;
        this.additionalVariables = additionalVariables;
    }

    public AbstractReporterSetup(
            final String name,
            final MetricConfig configuration,
            REPORTER reporter,
            final Map<String, String> additionalVariables) {
        this(name, configuration, reporter, MetricFilter.NO_OP_FILTER, additionalVariables);
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

    public MetricFilter getFilter() {
        return filter;
    }
}
