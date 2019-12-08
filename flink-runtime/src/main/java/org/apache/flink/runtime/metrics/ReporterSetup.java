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
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Encapsulates everything needed for the instantiation and configuration of a {@link MetricReporter}.
 */
public final class ReporterSetup {

	private static final Logger LOG = LoggerFactory.getLogger(ReporterSetup.class);

	// regex pattern to split the defined reporters
	private static final Pattern reporterListPattern = Pattern.compile("\\s*,\\s*");

	// regex pattern to extract the name from reporter configuration keys, e.g. "rep" from "metrics.reporter.rep.class"
	private static final Pattern reporterClassPattern = Pattern.compile(
		Pattern.quote(ConfigConstants.METRICS_REPORTER_PREFIX) +
			// [\S&&[^.]] = intersection of non-whitespace and non-period character classes
			"([\\S&&[^.]]*)\\." +
			'(' + Pattern.quote(ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX) + '|' + Pattern.quote(ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX) + ')');

	private final String name;
	private final MetricConfig configuration;
	private final MetricReporter reporter;

	public ReporterSetup(final String name, final MetricConfig configuration, MetricReporter reporter) {
		this.name = name;
		this.configuration = configuration;
		this.reporter = reporter;
	}

	public Optional<String> getDelimiter() {
		return Optional.ofNullable(configuration.getString(ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, null));
	}

	public Optional<String> getIntervalSettings() {
		return Optional.ofNullable(configuration.getString(ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, null));
	}

	public Set<String> getExcludedVariables() {
		String excludedVariablesList = configuration.getString(ConfigConstants.METRICS_REPORTER_EXCLUDED_VARIABLES, null);
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
		return createReporterSetup(reporterName, new MetricConfig(), reporter);
	}

	@VisibleForTesting
	public static ReporterSetup forReporter(String reporterName, MetricConfig metricConfig, MetricReporter reporter) {
		return createReporterSetup(reporterName, metricConfig, reporter);
	}

	private static ReporterSetup createReporterSetup(String reporterName, MetricConfig metricConfig, MetricReporter reporter) {
		LOG.info("Configuring {} with {}.", reporterName, metricConfig);
		reporter.open(metricConfig);

		return new ReporterSetup(reporterName, metricConfig, reporter);
	}

	public static List<ReporterSetup> fromConfiguration(final Configuration configuration) {
		String includedReportersString = configuration.getString(MetricOptions.REPORTERS_LIST, "");
		Set<String> includedReporters = reporterListPattern.splitAsStream(includedReportersString)
			.filter(r -> !r.isEmpty()) // splitting an empty string results in an empty string on jdk9+
			.collect(Collectors.toSet());

		// use a TreeSet to make the reporter order deterministic, which is useful for testing
		Set<String> namedReporters = new TreeSet<>(String::compareTo);
		// scan entire configuration for "metric.reporter" keys and parse individual reporter configurations
		for (String key : configuration.keySet()) {
			if (key.startsWith(ConfigConstants.METRICS_REPORTER_PREFIX)) {
				Matcher matcher = reporterClassPattern.matcher(key);
				if (matcher.matches()) {
					String reporterName = matcher.group(1);
					if (includedReporters.isEmpty() || includedReporters.contains(reporterName)) {
						if (namedReporters.contains(reporterName)) {
							LOG.warn("Duplicate class configuration detected for reporter {}.", reporterName);
						} else {
							namedReporters.add(reporterName);
						}
					} else {
						LOG.info("Excluding reporter {}, not configured in reporter list ({}).", reporterName, includedReportersString);
					}
				}
			}
		}

		if (namedReporters.isEmpty()) {
			return Collections.emptyList();
		}

		List<Tuple2<String, Configuration>> reporterConfigurations = new ArrayList<>(namedReporters.size());

		for (String namedReporter: namedReporters) {
			DelegatingConfiguration delegatingConfiguration = new DelegatingConfiguration(
				configuration,
				ConfigConstants.METRICS_REPORTER_PREFIX + namedReporter + '.');

			reporterConfigurations.add(Tuple2.of(namedReporter, delegatingConfiguration));
		}

		final Map<String, MetricReporterFactory> reporterFactories = loadReporterFactories();
		List<ReporterSetup> reporterArguments = new ArrayList<>(reporterConfigurations.size());
		for (Tuple2<String, Configuration> reporterConfiguration: reporterConfigurations) {
			String reporterName = reporterConfiguration.f0;
			Configuration reporterConfig = reporterConfiguration.f1;

			try {
				Optional<MetricReporter> metricReporterOptional = loadReporter(reporterName, reporterConfig, reporterFactories);
				metricReporterOptional.ifPresent(reporter -> {
					MetricConfig metricConfig = new MetricConfig();
					reporterConfig.addAllToProperties(metricConfig);

					reporterArguments.add(createReporterSetup(reporterName, metricConfig, reporter));
				});
			}
			catch (Throwable t) {
				LOG.error("Could not instantiate metrics reporter {}. Metrics might not be exposed/reported.", reporterName, t);
			}
		}
		return reporterArguments;
	}

	private static Map<String, MetricReporterFactory> loadReporterFactories() {
		final ServiceLoader<MetricReporterFactory> serviceLoader = ServiceLoader.load(MetricReporterFactory.class);

		final Map<String, MetricReporterFactory> reporterFactories = new HashMap<>(2);
		final Iterator<MetricReporterFactory> factoryIterator = serviceLoader.iterator();
		// do not use streams or for-each loops here because they do not allow catching individual ServiceConfigurationErrors
		// such an error might be caused if the META-INF/services contains an entry to a non-existing factory class
		while (factoryIterator.hasNext()) {
			try {
				MetricReporterFactory factory = factoryIterator.next();
				reporterFactories.put(factory.getClass().getName(), factory);
			} catch (Exception | ServiceConfigurationError e) {
				LOG.warn("Error while loading reporter factory.", e);
			}
		}

		return Collections.unmodifiableMap(reporterFactories);
	}

	private static Optional<MetricReporter> loadReporter(
			final String reporterName,
			final Configuration reporterConfig,
			final Map<String, MetricReporterFactory> reporterFactories)
			throws ClassNotFoundException, IllegalAccessException, InstantiationException {

		final String reporterClassName = reporterConfig.getString(ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, null);
		final String factoryClassName = reporterConfig.getString(ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX, null);

		if (factoryClassName != null) {
			return loadViaFactory(factoryClassName, reporterName, reporterConfig, reporterFactories);
		}

		if (reporterClassName != null) {
			return loadViaReflection(reporterClassName, reporterName, reporterConfig, reporterFactories);
		}

		LOG.warn("No reporter class nor factory set for reporter {}. Metrics might not be exposed/reported.", reporterName);
		return Optional.empty();
	}

	private static Optional<MetricReporter> loadViaFactory(
			final String factoryClassName,
			final String reporterName,
			final Configuration reporterConfig,
			final Map<String, MetricReporterFactory> reporterFactories) {

		MetricReporterFactory factory = reporterFactories.get(factoryClassName);

		if (factory == null) {
			LOG.warn("The reporter factory ({}) could not be found for reporter {}. Available factories: {}.", factoryClassName, reporterName, reporterFactories.keySet());
			return Optional.empty();
		} else {
			final MetricConfig metricConfig = new MetricConfig();
			reporterConfig.addAllToProperties(metricConfig);

			return Optional.of(factory.createMetricReporter(metricConfig));
		}
	}

	private static Optional<MetricReporter> loadViaReflection(
			final String reporterClassName,
			final String reporterName,
			final Configuration reporterConfig,
			final Map<String, MetricReporterFactory> reporterFactories) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

		final Class<?> reporterClass = Class.forName(reporterClassName);

		final InstantiateViaFactory alternativeFactoryAnnotation = reporterClass.getAnnotation(InstantiateViaFactory.class);
		if (alternativeFactoryAnnotation != null) {
			final String alternativeFactoryClassName = alternativeFactoryAnnotation.factoryClassName();
			LOG.info("The reporter configuration of {} is out-dated (but still supported)." +
					" Please configure a factory class instead: '{}{}.{}: {}' to ensure that the configuration" +
					" continues to work with future versions.",
				reporterName,
				ConfigConstants.METRICS_REPORTER_PREFIX,
				reporterName,
				ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX,
				alternativeFactoryClassName);
			return loadViaFactory(alternativeFactoryClassName, reporterName, reporterConfig, reporterFactories);
		}

		return Optional.of((MetricReporter) reporterClass.newInstance());
	}
}
