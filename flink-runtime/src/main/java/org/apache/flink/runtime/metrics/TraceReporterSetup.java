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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.runtime.metrics.filter.DefaultMetricFilter;
import org.apache.flink.runtime.metrics.filter.MetricFilter;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.traces.reporter.TraceReporter;
import org.apache.flink.traces.reporter.TraceReporterFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.metrics.ReporterSetup.loadReporterConfigurations;

/**
 * Encapsulates everything needed for the instantiation and configuration of a {@link
 * org.apache.flink.traces.reporter.TraceReporter}.
 */
public final class TraceReporterSetup {

    private static final Logger LOG = LoggerFactory.getLogger(TraceReporterSetup.class);

    // regex pattern to split the defined reporters
    private static final Pattern traceReporterListPattern = Pattern.compile("\\s*,\\s*");

    // regex pattern to extract the name from trace reporter configuration keys, e.g. "rep" from
    // "traces.reporter.rep.class"
    @SuppressWarnings("deprecation")
    private static final Pattern traceReporterClassPattern =
            Pattern.compile(
                    Pattern.quote(ConfigConstants.TRACES_REPORTER_PREFIX)
                            +
                            // [\S&&[^.]] = intersection of non-whitespace and non-period character
                            // classes
                            "([\\S&&[^.]]*)\\."
                            + Pattern.quote(TraceOptions.REPORTER_FACTORY_CLASS.key()));

    private final String name;
    private final MetricConfig configuration;
    private final TraceReporter reporter;
    private final Map<String, String> additionalVariables;

    public TraceReporterSetup(
            final String name,
            final MetricConfig configuration,
            TraceReporter reporter,
            final Map<String, String> additionalVariables) {
        this.name = name;
        this.configuration = configuration;
        this.reporter = reporter;
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

    public TraceReporter getReporter() {
        return reporter;
    }

    @VisibleForTesting
    public static TraceReporterSetup forReporter(String reporterName, TraceReporter reporter) {
        return createReporterSetup(
                reporterName, new MetricConfig(), reporter, Collections.emptyMap());
    }

    @VisibleForTesting
    public static TraceReporterSetup forReporter(
            String reporterName, MetricConfig metricConfig, TraceReporter reporter) {
        return createReporterSetup(reporterName, metricConfig, reporter, Collections.emptyMap());
    }

    private static TraceReporterSetup createReporterSetup(
            String reporterName,
            MetricConfig metricConfig,
            TraceReporter reporter,
            Map<String, String> additionalVariables) {
        reporter.open(metricConfig);

        return new TraceReporterSetup(reporterName, metricConfig, reporter, additionalVariables);
    }

    public static List<TraceReporterSetup> fromConfiguration(
            final Configuration configuration, @Nullable final PluginManager pluginManager) {
        String includedReportersString = configuration.get(TraceOptions.TRACE_REPORTERS_LIST, "");

        Set<String> namedReporters =
                ReporterSetup.findEnabledTraceReportersInConfiguration(
                        configuration,
                        includedReportersString,
                        traceReporterListPattern,
                        traceReporterClassPattern,
                        ConfigConstants.TRACES_REPORTER_PREFIX);

        if (namedReporters.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Tuple2<String, Configuration>> reporterConfigurations =
                loadReporterConfigurations(
                        configuration, namedReporters, ConfigConstants.TRACES_REPORTER_PREFIX);

        final Map<String, TraceReporterFactory> reporterFactories =
                loadAvailableReporterFactories(pluginManager);

        return setupReporters(reporterFactories, reporterConfigurations);
    }

    private static Map<String, TraceReporterFactory> loadAvailableReporterFactories(
            @Nullable PluginManager pluginManager) {
        final Map<String, TraceReporterFactory> reporterFactories = new HashMap<>(2);
        final Iterator<TraceReporterFactory> factoryIterator =
                getAllReporterFactories(pluginManager);
        // do not use streams or for-each loops here because they do not allow catching individual
        // ServiceConfigurationErrors
        // such an error might be caused if the META-INF/services contains an entry to a
        // non-existing factory class
        while (factoryIterator.hasNext()) {
            try {
                TraceReporterFactory factory = factoryIterator.next();
                String factoryClassName = factory.getClass().getName();
                TraceReporterFactory existingFactory = reporterFactories.get(factoryClassName);
                if (existingFactory == null) {
                    reporterFactories.put(factoryClassName, factory);
                    LOG.debug(
                            "Found {} {} at {} ",
                            TraceReporterFactory.class.getSimpleName(),
                            factoryClassName,
                            new File(
                                            factory.getClass()
                                                    .getProtectionDomain()
                                                    .getCodeSource()
                                                    .getLocation()
                                                    .toURI())
                                    .getCanonicalPath());
                } else {
                    LOG.warn(
                            "Multiple implementations of the same {} were found in 'lib' and/or 'plugins' directories for {}. It is recommended to remove redundant reporter JARs to resolve used versions' ambiguity.",
                            TraceReporter.class.getSimpleName(),
                            factoryClassName);
                }
            } catch (Exception | ServiceConfigurationError e) {
                LOG.warn("Error while loading {}.", TraceReporterFactory.class.getSimpleName(), e);
            }
        }

        return Collections.unmodifiableMap(reporterFactories);
    }

    private static Iterator<TraceReporterFactory> getAllReporterFactories(
            @Nullable PluginManager pluginManager) {
        final Spliterator<TraceReporterFactory> factoryIteratorSPI =
                ServiceLoader.load(TraceReporterFactory.class).spliterator();
        final Spliterator<TraceReporterFactory> factoryIteratorPlugins =
                pluginManager != null
                        ? Spliterators.spliteratorUnknownSize(
                                pluginManager.load(TraceReporterFactory.class), 0)
                        : Collections.<TraceReporterFactory>emptyList().spliterator();

        return Stream.concat(
                        StreamSupport.stream(factoryIteratorPlugins, false),
                        StreamSupport.stream(factoryIteratorSPI, false))
                .iterator();
    }

    private static List<TraceReporterSetup> setupReporters(
            Map<String, TraceReporterFactory> reporterFactories,
            List<Tuple2<String, Configuration>> reporterConfigurations) {
        List<TraceReporterSetup> reporterSetups = new ArrayList<>(reporterConfigurations.size());
        for (Tuple2<String, Configuration> reporterConfiguration : reporterConfigurations) {
            String reporterName = reporterConfiguration.f0;
            Configuration reporterConfig = reporterConfiguration.f1;

            try {
                Optional<TraceReporter> metricReporterOptional =
                        loadReporter(reporterName, reporterConfig, reporterFactories);

                final MetricFilter metricFilter =
                        DefaultMetricFilter.fromConfiguration(reporterConfig);

                // massage user variables keys into scope format for parity to variable exclusion
                Map<String, String> additionalVariables =
                        reporterConfig.get(TraceOptions.REPORTER_ADDITIONAL_VARIABLES).entrySet()
                                .stream()
                                .collect(
                                        Collectors.toMap(
                                                e -> ScopeFormat.asVariable(e.getKey()),
                                                Entry::getValue));

                metricReporterOptional.ifPresent(
                        reporter -> {
                            MetricConfig metricConfig = new MetricConfig();
                            reporterConfig.addAllToProperties(metricConfig);
                            reporterSetups.add(
                                    createReporterSetup(
                                            reporterName,
                                            metricConfig,
                                            reporter,
                                            additionalVariables));
                        });
            } catch (Throwable t) {
                LOG.error(
                        "Could not instantiate {} {}. Metrics might not be exposed/reported.",
                        TraceReporter.class.getSimpleName(),
                        reporterName,
                        t);
            }
        }
        return reporterSetups;
    }

    @SuppressWarnings("deprecation")
    private static Optional<TraceReporter> loadReporter(
            final String reporterName,
            final Configuration reporterConfig,
            final Map<String, TraceReporterFactory> reporterFactories) {

        final String factoryClassName = reporterConfig.get(TraceOptions.REPORTER_FACTORY_CLASS);

        if (factoryClassName != null) {
            return loadViaFactory(
                    factoryClassName, reporterName, reporterConfig, reporterFactories);
        }

        LOG.warn(
                "No reporter factory set for reporter {}. Traces might not be exposed/reported.",
                reporterName);
        return Optional.empty();
    }

    private static Optional<TraceReporter> loadViaFactory(
            final String factoryClassName,
            final String reporterName,
            final Configuration reporterConfig,
            final Map<String, TraceReporterFactory> reporterFactories) {

        TraceReporterFactory factory = reporterFactories.get(factoryClassName);

        if (factory == null) {
            LOG.warn(
                    "The reporter factory ({}) could not be found for reporter {}. Available factories: {}.",
                    factoryClassName,
                    reporterName,
                    reporterFactories.keySet());
            return Optional.empty();
        } else {
            return loadViaFactory(reporterConfig, factory);
        }
    }

    private static Optional<TraceReporter> loadViaFactory(
            final Configuration reporterConfig, final TraceReporterFactory factory) {

        final MetricConfig metricConfig = new MetricConfig();
        reporterConfig.addAllToProperties(metricConfig);

        return Optional.of(factory.createTraceReporter(metricConfig));
    }
}
