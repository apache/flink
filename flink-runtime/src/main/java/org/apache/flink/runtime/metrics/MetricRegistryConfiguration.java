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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Configuration object for {@link MetricRegistry}.
 */
public class MetricRegistryConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(MetricRegistryConfiguration.class);

	private static volatile MetricRegistryConfiguration DEFAULT_CONFIGURATION;

	// regex pattern to split the defined reporters
	private static final Pattern splitPattern = Pattern.compile("\\s*,\\s*");

	// scope formats for the different components
	private final ScopeFormats scopeFormats;

	// delimiter for the scope strings
	private final char delimiter;

	// contains for every configured reporter its name and the configuration object
	private final List<Tuple2<String, Configuration>> reporterConfigurations;

	public MetricRegistryConfiguration(
		ScopeFormats scopeFormats,
		char delimiter,
		List<Tuple2<String, Configuration>> reporterConfigurations) {

		this.scopeFormats = Preconditions.checkNotNull(scopeFormats);
		this.delimiter = delimiter;
		this.reporterConfigurations = Preconditions.checkNotNull(reporterConfigurations);
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

	public List<Tuple2<String, Configuration>> getReporterConfigurations() {
		return reporterConfigurations;
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
			scopeFormats = createScopeConfig(configuration);
		} catch (Exception e) {
			LOG.warn("Failed to parse scope format, using default scope formats", e);
			scopeFormats = new ScopeFormats();
		}

		char delim;
		try {
			delim = configuration.getString(MetricOptions.SCOPE_DELIMITER).charAt(0);
		} catch (Exception e) {
			LOG.warn("Failed to parse delimiter, using default delimiter.", e);
			delim = '.';
		}

		final String definedReporters = configuration.getString(MetricOptions.REPORTERS_LIST);
		List<Tuple2<String, Configuration>> reporterConfigurations;

		if (definedReporters == null) {
			reporterConfigurations = Collections.emptyList();
		} else {
			String[] namedReporters = splitPattern.split(definedReporters);

			reporterConfigurations = new ArrayList<>(namedReporters.length);

			for (String namedReporter: namedReporters) {
				DelegatingConfiguration delegatingConfiguration = new DelegatingConfiguration(
					configuration,
					ConfigConstants.METRICS_REPORTER_PREFIX + namedReporter + '.');

				reporterConfigurations.add(Tuple2.of(namedReporter, (Configuration) delegatingConfiguration));
			}
		}

		return new MetricRegistryConfiguration(scopeFormats, delim, reporterConfigurations);
	}

	/**
	 *	Create the scope formats from the given {@link Configuration}.
	 *
	 * @param configuration to extract the scope formats from
	 * @return Scope formats extracted from the given configuration
	 */
	static ScopeFormats createScopeConfig(Configuration configuration) {
		String jmFormat = configuration.getString(MetricOptions.SCOPE_NAMING_JM);
		String jmJobFormat = configuration.getString(MetricOptions.SCOPE_NAMING_JM_JOB);
		String tmFormat = configuration.getString(MetricOptions.SCOPE_NAMING_TM);
		String tmJobFormat = configuration.getString(MetricOptions.SCOPE_NAMING_TM_JOB);
		String taskFormat = configuration.getString(MetricOptions.SCOPE_NAMING_TASK);
		String operatorFormat = configuration.getString(MetricOptions.SCOPE_NAMING_OPERATOR);

		return new ScopeFormats(jmFormat, jmJobFormat, tmFormat, tmJobFormat, taskFormat, operatorFormat);
	}

	public static MetricRegistryConfiguration defaultMetricRegistryConfiguration() {
		// create the default metric registry configuration only once
		if (DEFAULT_CONFIGURATION == null) {
			synchronized (MetricRegistryConfiguration.class) {
				if (DEFAULT_CONFIGURATION == null) {
					DEFAULT_CONFIGURATION = fromConfiguration(new Configuration());
				}
			}
		}

		return DEFAULT_CONFIGURATION;
	}

}
