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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.EventOptions;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.events.EventBuilder;
import org.apache.flink.events.reporter.EventReporter;
import org.apache.flink.events.reporter.EventReporterFactory;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.Reporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.runtime.metrics.filter.ReporterFilter;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.traces.reporter.TraceReporter;
import org.apache.flink.traces.reporter.TraceReporterFactory;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Builder class for {@link Reporter}.
 *
 * @param <REPORTED> Generic type of what's reported.
 * @param <REPORTER> Generic type of the reporter.
 * @param <SETUP> Generic type of the created setup.
 * @param <REPORTER_FACTORY> Generic type of the reporter factory.
 */
public class ReporterSetupBuilder<
        REPORTED,
        REPORTER extends Reporter,
        SETUP extends AbstractReporterSetup<REPORTER, REPORTED>,
        REPORTER_FACTORY> {

    private static final Logger LOG = LoggerFactory.getLogger(ReporterSetupBuilder.class);

    private static final Pattern LIST_PATTERN = Pattern.compile("\\s*,\\s*");

    private static Pattern createReporterClassPattern(String prefix, String suffix) {
        // [\S&&[^.]] = intersection of non-whitespace and non-period character classes
        return Pattern.compile(prefix + "([\\S&&[^.]]*)\\." + suffix);
    }

    /** Builder for metric reporter. */
    public static final ReporterSetupBuilder<
                    Metric, MetricReporter, ReporterSetup, MetricReporterFactory>
            METRIC_SETUP_BUILDER =
                    new ReporterSetupBuilder<>(
                            new ReporterSetupInfo<>(
                                    MetricReporter.class,
                                    MetricReporterFactory.class,
                                    ConfigConstants.METRICS_REPORTER_PREFIX,
                                    MetricOptions.REPORTER_FACTORY_CLASS,
                                    MetricOptions.REPORTERS_LIST,
                                    MetricOptions.REPORTER_ADDITIONAL_VARIABLES,
                                    createReporterClassPattern(
                                            Pattern.quote(ConfigConstants.METRICS_REPORTER_PREFIX),
                                            '('
                                                    + Pattern.quote(
                                                            MetricOptions.REPORTER_FACTORY_CLASS
                                                                    .key())
                                                    + ')'),
                                    LIST_PATTERN,
                                    MetricReporterFactory::createMetricReporter,
                                    ReporterSetup::new));

    /** Builder for span/trace reporter. */
    public static final ReporterSetupBuilder<
                    SpanBuilder, TraceReporter, TraceReporterSetup, TraceReporterFactory>
            TRACE_SETUP_BUILDER =
                    new ReporterSetupBuilder<>(
                            new ReporterSetupInfo<>(
                                    TraceReporter.class,
                                    TraceReporterFactory.class,
                                    ConfigConstants.TRACES_REPORTER_PREFIX,
                                    TraceOptions.REPORTER_FACTORY_CLASS,
                                    TraceOptions.TRACE_REPORTERS_LIST,
                                    TraceOptions.REPORTER_ADDITIONAL_VARIABLES,
                                    createReporterClassPattern(
                                            Pattern.quote(ConfigConstants.TRACES_REPORTER_PREFIX),
                                            Pattern.quote(
                                                    TraceOptions.REPORTER_FACTORY_CLASS.key())),
                                    LIST_PATTERN,
                                    TraceReporterFactory::createTraceReporter,
                                    TraceReporterSetup::new));

    /** Builder for event reporter. */
    public static final ReporterSetupBuilder<
                    EventBuilder, EventReporter, EventReporterSetup, EventReporterFactory>
            EVENT_SETUP_BUILDER =
                    new ReporterSetupBuilder<>(
                            new ReporterSetupInfo<>(
                                    EventReporter.class,
                                    EventReporterFactory.class,
                                    ConfigConstants.EVENTS_REPORTER_PREFIX,
                                    EventOptions.REPORTER_FACTORY_CLASS,
                                    EventOptions.REPORTERS_LIST,
                                    EventOptions.REPORTER_ADDITIONAL_VARIABLES,
                                    createReporterClassPattern(
                                            Pattern.quote(ConfigConstants.EVENTS_REPORTER_PREFIX),
                                            Pattern.quote(
                                                    EventOptions.REPORTER_FACTORY_CLASS.key())),
                                    LIST_PATTERN,
                                    EventReporterFactory::createEventReporter,
                                    EventReporterSetup::new));

    /**
     * Functional interface to unify access to different reporter factories that don't have a proper
     * superclass.
     *
     * @param <REPORTER_FACTORY> type of the reporter factory.
     * @param <REPORTER> type of the reporter.
     */
    @FunctionalInterface
    interface ReporterFactoryAdapter<REPORTER_FACTORY, REPORTER> {
        REPORTER invoke(REPORTER_FACTORY factory, MetricConfig metricConfig);
    }

    /**
     * Factory interface to create the desired type of reporter setup.
     *
     * @param <SETUP> type of the created setup.
     * @param <REPORTER> type of the reporter.
     */
    interface ReporterSetupFactory<SETUP, REPORTER, REPORTED> {
        SETUP createReporterSetup(
                String reporterName,
                MetricConfig metricConfig,
                REPORTER reporter,
                ReporterFilter<REPORTED> reporterFilter,
                Map<String, String> additionalVariables);
    }

    /**
     * Helper class that bundles all the information for a reporter type together so that we can
     * handle all types in generic code.
     *
     * @param <REPORTER> type of the reporter.
     * @param <SETUP> type of the setup.
     * @param <REPORTER_FACTORY> type of the reporter factory.
     */
    static class ReporterSetupInfo<
            REPORTED,
            REPORTER extends Reporter,
            SETUP extends AbstractReporterSetup<REPORTER, REPORTED>,
            REPORTER_FACTORY> {
        ReporterSetupInfo(
                Class<REPORTER> reporterTypeClass,
                Class<REPORTER_FACTORY> factoryTypeClass,
                String reporterPrefix,
                ConfigOption<String> factoryClassConfig,
                ConfigOption<String> reportersList,
                ConfigOption<Map<String, String>> reporterAdditionalVariables,
                Pattern reporterClassPattern,
                Pattern reporterListPattern,
                ReporterFactoryAdapter<REPORTER_FACTORY, REPORTER> reporterFactoryAdapter,
                ReporterSetupFactory<SETUP, REPORTER, REPORTED> reporterSetupFactory) {
            this.reporterTypeClass = reporterTypeClass;
            this.factoryTypeClass = factoryTypeClass;
            this.reporterPrefix = reporterPrefix;
            this.factoryClassConfig = factoryClassConfig;
            this.reportersList = reportersList;
            this.reporterAdditionalVariables = reporterAdditionalVariables;
            this.reporterClassPattern = reporterClassPattern;
            this.reporterListPattern = reporterListPattern;
            this.reporterFactoryAdapter = reporterFactoryAdapter;
            this.reporterSetupFactory = reporterSetupFactory;
        }

        /** Class of the reporter. */
        Class<REPORTER> reporterTypeClass;

        /** Class of the reporter factory. */
        Class<REPORTER_FACTORY> factoryTypeClass;

        /**
         * Package prefix for the reporter class. Taken from {@link ConfigConstants}, e.g. {@link
         * ConfigConstants#METRICS_REPORTER_PREFIX}.
         */
        String reporterPrefix;

        /**
         * Config option for the factory class, e.g. {@link MetricOptions#REPORTER_FACTORY_CLASS}.
         */
        ConfigOption<String> factoryClassConfig;

        /** Config option for the reporters list, e.g. {@link MetricOptions#REPORTERS_LIST}. */
        ConfigOption<String> reportersList;

        /** Additional variables to report through he reporter. */
        ConfigOption<Map<String, String>> reporterAdditionalVariables;

        /**
         * Regex pattern to extract the name from event reporter configuration keys, e.g. "rep" from
         * "events.reporter.rep.class".
         */
        Pattern reporterClassPattern;

        /**
         * Regex pattern to split the defined reporters, e.g.
         *
         * <p>Pattern.compile("\\s*,\\s*");
         */
        Pattern reporterListPattern;

        /** Adapter to call the specific reporter factory. */
        ReporterFactoryAdapter<REPORTER_FACTORY, REPORTER> reporterFactoryAdapter;

        /** Factory for the specific setup object. */
        ReporterSetupFactory<SETUP, REPORTER, REPORTED> reporterSetupFactory;
    }

    /**
     * Information with specific for the type of reporter for which this builder can create a setup.
     */
    private final ReporterSetupInfo<REPORTED, REPORTER, SETUP, REPORTER_FACTORY> reporterSetupInfo;

    ReporterSetupBuilder(
            ReporterSetupInfo<REPORTED, REPORTER, SETUP, REPORTER_FACTORY> reporterSetupInfo) {
        this.reporterSetupInfo = reporterSetupInfo;
    }

    @VisibleForTesting
    public SETUP forReporter(String reporterName, REPORTER reporter) {
        return forReporter(reporterName, new MetricConfig(), reporter);
    }

    @VisibleForTesting
    public SETUP forReporter(
            String reporterName, REPORTER reporter, ReporterFilter<REPORTED> reporterFilter) {
        return createReporterSetup(
                reporterName, new MetricConfig(), reporter, reporterFilter, Collections.emptyMap());
    }

    @VisibleForTesting
    public SETUP forReporter(String reporterName, MetricConfig metricConfig, REPORTER reporter) {
        return createReporterSetup(
                reporterName,
                metricConfig,
                reporter,
                ReporterFilter.getNoOpFilter(),
                Collections.emptyMap());
    }

    @VisibleForTesting
    public SETUP forReporter(
            String reporterName,
            MetricConfig metricConfig,
            REPORTER reporter,
            ReporterFilter<REPORTED> reporterFilter,
            Map<String, String> additionalVariables) {
        return createReporterSetup(
                reporterName, metricConfig, reporter, reporterFilter, additionalVariables);
    }

    @VisibleForTesting
    public SETUP forReporter(
            String reporterName,
            MetricConfig metricConfig,
            REPORTER reporter,
            ReporterFilter<REPORTED> reporterFilter) {
        return createReporterSetup(
                reporterName, metricConfig, reporter, reporterFilter, Collections.emptyMap());
    }

    private SETUP createReporterSetup(
            String reporterName,
            MetricConfig metricConfig,
            REPORTER reporter,
            ReporterFilter<REPORTED> reporterFilter,
            Map<String, String> additionalVariables) {
        reporter.open(metricConfig);
        return reporterSetupInfo.reporterSetupFactory.createReporterSetup(
                reporterName, metricConfig, reporter, reporterFilter, additionalVariables);
    }

    public List<SETUP> fromConfiguration(
            final Configuration configuration,
            Function<Configuration, ReporterFilter<REPORTED>> filterFactory,
            @Nullable final PluginManager pluginManager) {

        Set<String> namedReporters = findEnabledReportersInConfiguration(configuration);

        if (namedReporters.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Tuple2<String, Configuration>> reporterConfigurations =
                loadReporterConfigurations(configuration, namedReporters);

        final Map<String, REPORTER_FACTORY> reporterFactories =
                loadAvailableReporterFactories(pluginManager);

        return setupReporters(reporterFactories, filterFactory, reporterConfigurations);
    }

    private Map<String, REPORTER_FACTORY> loadAvailableReporterFactories(
            @Nullable PluginManager pluginManager) {
        final Map<String, REPORTER_FACTORY> reporterFactories =
                CollectionUtil.newHashMapWithExpectedSize(2);
        final Iterator<REPORTER_FACTORY> factoryIterator = getAllReporterFactories(pluginManager);
        // do not use streams or for-each loops here because they do not allow catching
        // individual
        // ServiceConfigurationErrors
        // such an error might be caused if the META-INF/services contains an entry to a
        // non-existing factory class
        while (factoryIterator.hasNext()) {
            try {
                REPORTER_FACTORY factory = factoryIterator.next();
                String factoryClassName = factory.getClass().getName();
                REPORTER_FACTORY existingFactory = reporterFactories.get(factoryClassName);
                if (existingFactory == null) {
                    reporterFactories.put(factoryClassName, factory);
                    LOG.debug(
                            "Found {} {} at {} ",
                            reporterSetupInfo.factoryTypeClass.getSimpleName(),
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
                            reporterSetupInfo.factoryTypeClass.getSimpleName(),
                            factoryClassName);
                }
            } catch (Exception | ServiceConfigurationError e) {
                LOG.warn(
                        "Error while loading {}.",
                        reporterSetupInfo.factoryTypeClass.getSimpleName(),
                        e);
            }
        }

        return Collections.unmodifiableMap(reporterFactories);
    }

    private Iterator<REPORTER_FACTORY> getAllReporterFactories(
            @Nullable PluginManager pluginManager) {
        final Spliterator<REPORTER_FACTORY> factoryIteratorSPI =
                ServiceLoader.load(reporterSetupInfo.factoryTypeClass).spliterator();
        final Spliterator<REPORTER_FACTORY> factoryIteratorPlugins =
                pluginManager != null
                        ? Spliterators.spliteratorUnknownSize(
                                pluginManager.load(reporterSetupInfo.factoryTypeClass), 0)
                        : Spliterators.emptySpliterator();

        return Stream.concat(
                        StreamSupport.stream(factoryIteratorPlugins, false),
                        StreamSupport.stream(factoryIteratorSPI, false))
                .iterator();
    }

    private List<SETUP> setupReporters(
            Map<String, REPORTER_FACTORY> reporterFactories,
            Function<Configuration, ReporterFilter<REPORTED>> filterFactory,
            List<Tuple2<String, Configuration>> reporterConfigurations) {
        List<SETUP> reporterSetups = new ArrayList<>(reporterConfigurations.size());
        for (Tuple2<String, Configuration> reporterConfiguration : reporterConfigurations) {
            String reporterName = reporterConfiguration.f0;
            Configuration reporterConfig = reporterConfiguration.f1;

            try {
                Optional<REPORTER> metricReporterOptional =
                        loadReporter(reporterName, reporterConfig, reporterFactories);

                final ReporterFilter<REPORTED> reporterFilter = filterFactory.apply(reporterConfig);

                // massage user variables keys into scope format for parity to variable
                // exclusion
                Map<String, String> additionalVariables =
                        reporterConfig
                                .get(reporterSetupInfo.reporterAdditionalVariables)
                                .entrySet()
                                .stream()
                                .collect(
                                        Collectors.toMap(
                                                e -> ScopeFormat.asVariable(e.getKey()),
                                                Map.Entry::getValue));

                metricReporterOptional.ifPresent(
                        reporter -> {
                            MetricConfig metricConfig = new MetricConfig();
                            reporterConfig.addAllToProperties(metricConfig);
                            reporterSetups.add(
                                    createReporterSetup(
                                            reporterName,
                                            metricConfig,
                                            reporter,
                                            reporterFilter,
                                            additionalVariables));
                        });
            } catch (Throwable t) {
                LOG.error(
                        "Could not instantiate {} {}. Reporting might not be exposed.",
                        reporterSetupInfo.reporterTypeClass.getSimpleName(),
                        reporterName,
                        t);
            }
        }
        return reporterSetups;
    }

    private Optional<REPORTER> loadReporter(
            final String reporterName,
            final Configuration reporterConfig,
            final Map<String, REPORTER_FACTORY> reporterFactories) {

        final String factoryClassName = reporterConfig.get(reporterSetupInfo.factoryClassConfig);

        if (factoryClassName != null) {
            return loadViaFactory(
                    factoryClassName, reporterName, reporterConfig, reporterFactories);
        }

        LOG.warn(
                "No reporter factory set for reporter {}. Events might not be exposed/reported.",
                reporterName);
        return Optional.empty();
    }

    private Optional<REPORTER> loadViaFactory(
            final String factoryClassName,
            final String reporterName,
            final Configuration reporterConfig,
            final Map<String, REPORTER_FACTORY> reporterFactories) {

        REPORTER_FACTORY factory = reporterFactories.get(factoryClassName);

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

    private Optional<REPORTER> loadViaFactory(
            final Configuration reporterConfig, final REPORTER_FACTORY factory) {

        final MetricConfig metricConfig = new MetricConfig();
        reporterConfig.addAllToProperties(metricConfig);

        return Optional.of(reporterSetupInfo.reporterFactoryAdapter.invoke(factory, metricConfig));
    }

    private List<Tuple2<String, Configuration>> loadReporterConfigurations(
            Configuration configuration, Set<String> namedReporters) {
        final List<Tuple2<String, Configuration>> reporterConfigurations =
                new ArrayList<>(namedReporters.size());

        for (String namedReporter : namedReporters) {
            DelegatingConfiguration delegatingConfiguration =
                    new DelegatingConfiguration(
                            configuration, reporterSetupInfo.reporterPrefix + namedReporter + '.');

            reporterConfigurations.add(Tuple2.of(namedReporter, delegatingConfiguration));
        }
        return reporterConfigurations;
    }

    private Set<String> findEnabledReportersInConfiguration(Configuration configuration) {
        String includedReportersString = configuration.get(reporterSetupInfo.reportersList, "");
        Set<String> includedReporters =
                reporterSetupInfo
                        .reporterListPattern
                        .splitAsStream(includedReportersString)
                        .filter(r -> !r.isEmpty()) // splitting an empty string results in
                        // an empty string on jdk9+
                        .collect(Collectors.toSet());

        // use a TreeSet to make the reporter order deterministic, which is useful for testing
        Set<String> namedOrderedReporters = new TreeSet<>(String::compareTo);

        // scan entire configuration for keys starting with reporterPrefix and determine
        // the set of enabled reporters
        for (String key : configuration.keySet()) {
            if (key.startsWith(reporterSetupInfo.reporterPrefix)) {
                Matcher matcher = reporterSetupInfo.reporterClassPattern.matcher(key);
                if (matcher.matches()) {
                    String reporterName = matcher.group(1);
                    if (includedReporters.isEmpty() || includedReporters.contains(reporterName)) {
                        if (namedOrderedReporters.contains(reporterName)) {
                            LOG.warn(
                                    "Duplicate class configuration detected for {}{}.",
                                    reporterSetupInfo.reporterPrefix.replace('.', ' '),
                                    reporterName);
                        } else {
                            namedOrderedReporters.add(reporterName);
                        }
                    } else {
                        LOG.info(
                                "Excluding {}{}, not configured in reporter list ({}).",
                                reporterSetupInfo.reporterPrefix.replace('.', ' '),
                                reporterName,
                                includedReportersString);
                    }
                }
            }
        }
        return namedOrderedReporters;
    }
}
