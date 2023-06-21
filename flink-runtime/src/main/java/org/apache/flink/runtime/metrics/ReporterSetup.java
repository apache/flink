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
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.runtime.metrics.filter.DefaultMetricFilter;
import org.apache.flink.runtime.metrics.filter.MetricFilter;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Encapsulates everything needed for the instantiation and configuration of a {@link
 * MetricReporter}.
 */
public final class ReporterSetup {

    private static final Logger LOG = LoggerFactory.getLogger(ReporterSetup.class);

    // regex pattern to split the defined reporters
    private static final Pattern reporterListPattern = Pattern.compile("\\s*,\\s*");

    // regex pattern to extract the name from reporter configuration keys, e.g. "rep" from
    // "metrics.reporter.rep.class"
    @SuppressWarnings("deprecation")
    private static final Pattern reporterClassPattern =
            Pattern.compile(
                    Pattern.quote(ConfigConstants.METRICS_REPORTER_PREFIX)
                            +
                            // [\S&&[^.]] = intersection of non-whitespace and non-period character
                            // classes
                            "([\\S&&[^.]]*)\\."
                            + '('
                            + Pattern.quote(MetricOptions.REPORTER_CLASS.key())
                            + '|'
                            + Pattern.quote(MetricOptions.REPORTER_FACTORY_CLASS.key())
                            + ')');

    private final String name;
    private final MetricConfig configuration;
    private final MetricReporter reporter;
    private final MetricFilter filter;
    private final Map<String, String> additionalVariables;

    public ReporterSetup(
            final String name,
            final MetricConfig configuration,
            MetricReporter reporter,
            final MetricFilter filter,
            final Map<String, String> additionalVariables) {
        this.name = name;
        this.configuration = configuration;
        this.reporter = reporter;
        this.filter = filter;
        this.additionalVariables = additionalVariables;
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

    public MetricFilter getFilter() {
        return filter;
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

    public MetricReporter getReporter() {
        return reporter;
    }

    @VisibleForTesting
    public static ReporterSetup forReporter(String reporterName, MetricReporter reporter) {
        return createReporterSetup(
                reporterName,
                new MetricConfig(),
                reporter,
                MetricFilter.NO_OP_FILTER,
                Collections.emptyMap());
    }

    @VisibleForTesting
    public static ReporterSetup forReporter(
            String reporterName, MetricConfig metricConfig, MetricReporter reporter) {
        return createReporterSetup(
                reporterName,
                metricConfig,
                reporter,
                MetricFilter.NO_OP_FILTER,
                Collections.emptyMap());
    }

    @VisibleForTesting
    public static ReporterSetup forReporter(
            String reporterName, MetricFilter metricFilter, MetricReporter reporter) {
        return createReporterSetup(
                reporterName, new MetricConfig(), reporter, metricFilter, Collections.emptyMap());
    }

    private static ReporterSetup createReporterSetup(
            String reporterName,
            MetricConfig metricConfig,
            MetricReporter reporter,
            MetricFilter metricFilter,
            Map<String, String> additionalVariables) {
        reporter.open(metricConfig);

        return new ReporterSetup(
                reporterName, metricConfig, reporter, metricFilter, additionalVariables);
    }

    public static List<ReporterSetup> fromConfiguration(
            final Configuration configuration, @Nullable final PluginManager pluginManager) {
        String includedReportersString = configuration.getString(MetricOptions.REPORTERS_LIST, "");

        Set<String> namedReporters =
                findEnabledReportersInConfiguration(configuration, includedReportersString);

        if (namedReporters.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Tuple2<String, Configuration>> reporterConfigurations =
                loadReporterConfigurations(configuration, namedReporters);

        final Map<String, MetricReporterFactory> reporterFactories =
                loadAvailableReporterFactories(pluginManager);

        return setupReporters(reporterFactories, reporterConfigurations);
    }

    private static Set<String> findEnabledReportersInConfiguration(
            Configuration configuration, String includedReportersString) {
        Set<String> includedReporters =
                reporterListPattern
                        .splitAsStream(includedReportersString)
                        .filter(r -> !r.isEmpty()) // splitting an empty string results in
                        // an empty string on jdk9+
                        .collect(Collectors.toSet());

        // use a TreeSet to make the reporter order deterministic, which is useful for testing
        Set<String> namedOrderedReporters = new TreeSet<>(String::compareTo);

        // scan entire configuration for keys starting with METRICS_REPORTER_PREFIX and determine
        // the set of enabled reporters
        for (String key : configuration.keySet()) {
            if (key.startsWith(ConfigConstants.METRICS_REPORTER_PREFIX)) {
                Matcher matcher = reporterClassPattern.matcher(key);
                if (matcher.matches()) {
                    String reporterName = matcher.group(1);
                    if (includedReporters.isEmpty() || includedReporters.contains(reporterName)) {
                        if (namedOrderedReporters.contains(reporterName)) {
                            LOG.warn(
                                    "Duplicate class configuration detected for reporter {}.",
                                    reporterName);
                        } else {
                            namedOrderedReporters.add(reporterName);
                        }
                    } else {
                        LOG.info(
                                "Excluding reporter {}, not configured in reporter list ({}).",
                                reporterName,
                                includedReportersString);
                    }
                }
            }
        }
        return namedOrderedReporters;
    }

    private static List<Tuple2<String, Configuration>> loadReporterConfigurations(
            Configuration configuration, Set<String> namedReporters) {
        final List<Tuple2<String, Configuration>> reporterConfigurations =
                new ArrayList<>(namedReporters.size());

        for (String namedReporter : namedReporters) {
            DelegatingConfiguration delegatingConfiguration =
                    new DelegatingConfiguration(
                            configuration,
                            ConfigConstants.METRICS_REPORTER_PREFIX + namedReporter + '.');

            reporterConfigurations.add(Tuple2.of(namedReporter, delegatingConfiguration));
        }
        return reporterConfigurations;
    }

    private static Map<String, MetricReporterFactory> loadAvailableReporterFactories(
            @Nullable PluginManager pluginManager) {
        final Map<String, MetricReporterFactory> reporterFactories =
                CollectionUtil.newHashMapWithExpectedSize(2);
        final Iterator<MetricReporterFactory> factoryIterator =
                getAllReporterFactories(pluginManager);
        // do not use streams or for-each loops here because they do not allow catching individual
        // ServiceConfigurationErrors
        // such an error might be caused if the META-INF/services contains an entry to a
        // non-existing factory class
        while (factoryIterator.hasNext()) {
            try {
                MetricReporterFactory factory = factoryIterator.next();
                String factoryClassName = factory.getClass().getName();
                MetricReporterFactory existingFactory = reporterFactories.get(factoryClassName);
                if (existingFactory == null) {
                    reporterFactories.put(factoryClassName, factory);
                    LOG.debug(
                            "Found reporter factory {} at {} ",
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
                            "Multiple implementations of the same reporter were found in 'lib' and/or 'plugins' directories for {}. It is recommended to remove redundant reporter JARs to resolve used versions' ambiguity.",
                            factoryClassName);
                }
            } catch (Exception | ServiceConfigurationError e) {
                LOG.warn("Error while loading reporter factory.", e);
            }
        }

        return Collections.unmodifiableMap(reporterFactories);
    }

    private static Iterator<MetricReporterFactory> getAllReporterFactories(
            @Nullable PluginManager pluginManager) {
        final Iterator<MetricReporterFactory> factoryIteratorSPI =
                ServiceLoader.load(MetricReporterFactory.class).iterator();
        final Iterator<MetricReporterFactory> factoryIteratorPlugins =
                pluginManager != null
                        ? pluginManager.load(MetricReporterFactory.class)
                        : Collections.emptyIterator();

        return Iterators.concat(factoryIteratorPlugins, factoryIteratorSPI);
    }

    private static List<ReporterSetup> setupReporters(
            Map<String, MetricReporterFactory> reporterFactories,
            List<Tuple2<String, Configuration>> reporterConfigurations) {
        List<ReporterSetup> reporterSetups = new ArrayList<>(reporterConfigurations.size());
        for (Tuple2<String, Configuration> reporterConfiguration : reporterConfigurations) {
            String reporterName = reporterConfiguration.f0;
            Configuration reporterConfig = reporterConfiguration.f1;

            try {
                Optional<MetricReporter> metricReporterOptional =
                        loadReporter(reporterName, reporterConfig, reporterFactories);

                final MetricFilter metricFilter =
                        DefaultMetricFilter.fromConfiguration(reporterConfig);

                // massage user variables keys into scope format for parity to variable exclusion
                Map<String, String> additionalVariables =
                        reporterConfig.get(MetricOptions.REPORTER_ADDITIONAL_VARIABLES).entrySet()
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
                                            metricFilter,
                                            additionalVariables));
                        });
            } catch (Throwable t) {
                LOG.error(
                        "Could not instantiate metrics reporter {}. Metrics might not be exposed/reported.",
                        reporterName,
                        t);
            }
        }
        return reporterSetups;
    }

    @SuppressWarnings("deprecation")
    private static Optional<MetricReporter> loadReporter(
            final String reporterName,
            final Configuration reporterConfig,
            final Map<String, MetricReporterFactory> reporterFactories) {

        final String reporterClassName = reporterConfig.get(MetricOptions.REPORTER_CLASS);
        final String factoryClassName = reporterConfig.get(MetricOptions.REPORTER_FACTORY_CLASS);

        if (factoryClassName != null) {
            return loadViaFactory(
                    factoryClassName, reporterName, reporterConfig, reporterFactories);
        }

        if (reporterClassName != null) {
            LOG.warn(
                    "The reporter configuration of '{}' configures the reporter class, which is no a no longer supported approach to configure reporters."
                            + " Please configure a factory class instead: '{}{}.{}: <factoryClass>'.",
                    reporterName,
                    ConfigConstants.METRICS_REPORTER_PREFIX,
                    reporterName,
                    MetricOptions.REPORTER_FACTORY_CLASS.key());
        }

        LOG.warn(
                "No reporter factory set for reporter {}. Metrics might not be exposed/reported.",
                reporterName);
        return Optional.empty();
    }

    private static Optional<MetricReporter> loadViaFactory(
            final String factoryClassName,
            final String reporterName,
            final Configuration reporterConfig,
            final Map<String, MetricReporterFactory> reporterFactories) {

        MetricReporterFactory factory = reporterFactories.get(factoryClassName);

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

    private static Optional<MetricReporter> loadViaFactory(
            final Configuration reporterConfig, final MetricReporterFactory factory) {

        final MetricConfig metricConfig = new MetricConfig();
        reporterConfig.addAllToProperties(metricConfig);

        return Optional.of(factory.createMetricReporter(metricConfig));
    }
}
