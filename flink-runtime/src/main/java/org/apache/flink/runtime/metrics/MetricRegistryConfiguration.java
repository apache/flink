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

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configuration object for {@link MetricRegistryImpl}. */
public class MetricRegistryConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(MetricRegistryConfiguration.class);

    private static volatile MetricRegistryConfiguration defaultConfiguration;

    // scope formats for the different components
    private final ScopeFormats scopeFormats;

    // delimiter for the scope strings
    private final char delimiter;

    private final long queryServiceMessageSizeLimit;

    public MetricRegistryConfiguration(
            ScopeFormats scopeFormats, char delimiter, long queryServiceMessageSizeLimit) {

        this.scopeFormats = Preconditions.checkNotNull(scopeFormats);
        this.delimiter = delimiter;
        this.queryServiceMessageSizeLimit = queryServiceMessageSizeLimit;
    }

    // ------------------------------------------------------------------------
    //  Getter
    // ------------------------------------------------------------------------

    public ScopeFormats getScopeFormats() {
        return scopeFormats;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public long getQueryServiceMessageSizeLimit() {
        return queryServiceMessageSizeLimit;
    }

    // ------------------------------------------------------------------------
    //  Static factory methods
    // ------------------------------------------------------------------------

    /**
     * Create a metric registry configuration object from the given {@link Configuration}.
     *
     * @param configuration to generate the metric registry configuration from
     * @return Metric registry configuration generated from the configuration
     */
    public static MetricRegistryConfiguration fromConfiguration(Configuration configuration) {
        ScopeFormats scopeFormats;
        try {
            scopeFormats = ScopeFormats.fromConfig(configuration);
        } catch (Exception e) {
            LOG.warn("Failed to parse scope format, using default scope formats", e);
            scopeFormats = ScopeFormats.fromConfig(new Configuration());
        }

        char delim;
        try {
            delim = configuration.getString(MetricOptions.SCOPE_DELIMITER).charAt(0);
        } catch (Exception e) {
            LOG.warn("Failed to parse delimiter, using default delimiter.", e);
            delim = '.';
        }

        final long maximumFrameSize = AkkaRpcServiceUtils.extractMaximumFramesize(configuration);

        // padding to account for serialization overhead
        final long messageSizeLimitPadding = 256;

        return new MetricRegistryConfiguration(
                scopeFormats, delim, maximumFrameSize - messageSizeLimitPadding);
    }

    public static MetricRegistryConfiguration defaultMetricRegistryConfiguration() {
        // create the default metric registry configuration only once
        if (defaultConfiguration == null) {
            synchronized (MetricRegistryConfiguration.class) {
                if (defaultConfiguration == null) {
                    defaultConfiguration = fromConfiguration(new Configuration());
                }
            }
        }

        return defaultConfiguration;
    }
}
