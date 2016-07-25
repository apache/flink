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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A MetricRegistry keeps track of all registered {@link Metric Metrics}. It serves as the
 * connection between {@link MetricGroup MetricGroups} and {@link MetricReporter MetricReporters}.
 */
public class MetricRegistry {
	static final Logger LOG = LoggerFactory.getLogger(MetricRegistry.class);

	private final MetricReporter reporter;
	private final ScheduledExecutorService executor;

	private final ScopeFormats scopeFormats;

	private final char delimiter;

	/**
	 * Creates a new MetricRegistry and starts the configured reporter.
	 */
	public MetricRegistry(Configuration config) {
		// first parse the scope formats, these are needed for all reporters
		ScopeFormats scopeFormats;
		try {
			scopeFormats = createScopeConfig(config);
		}
		catch (Exception e) {
			LOG.warn("Failed to parse scope format, using default scope formats", e);
			scopeFormats = new ScopeFormats();
		}
		this.scopeFormats = scopeFormats;

		char delim;
		try {
			delim = config.getString(ConfigConstants.METRICS_SCOPE_DELIMITER, ".").charAt(0);
		} catch (Exception e) {
			LOG.warn("Failed to parse delimiter, using default delimiter.", e);
			delim = '.';
		}
		this.delimiter = delim;

		// second, instantiate any custom configured reporters

		final String className = config.getString(ConfigConstants.METRICS_REPORTER_CLASS, null);
		if (className == null) {
			// by default, don't report anything
			LOG.info("No metrics reporter configured, no metrics will be exposed/reported.");
			this.reporter = null;
			this.executor = null;
		}
		else {
			MetricReporter reporter;
			ScheduledExecutorService executor = null;
			try {
				String configuredPeriod = config.getString(ConfigConstants.METRICS_REPORTER_INTERVAL, null);
				TimeUnit timeunit = TimeUnit.SECONDS;
				long period = 10;

				if (configuredPeriod != null) {
					try {
						String[] interval = configuredPeriod.split(" ");
						period = Long.parseLong(interval[0]);
						timeunit = TimeUnit.valueOf(interval[1]);
					}
					catch (Exception e) {
						LOG.error("Cannot parse report interval from config: " + configuredPeriod +
							" - please use values like '10 SECONDS' or '500 MILLISECONDS'. " +
							"Using default reporting interval.");
					}
				}

				MetricConfig reporterConfig = createReporterConfig(config);

				Class<?> reporterClass = Class.forName(className);
				reporter = (MetricReporter) reporterClass.newInstance();
				reporter.open(reporterConfig);

				if (reporter instanceof Scheduled) {
					executor = Executors.newSingleThreadScheduledExecutor();
					LOG.info("Periodically reporting metrics in intervals of {} {}", period, timeunit.name());

					executor.scheduleWithFixedDelay(new ReporterTask((Scheduled) reporter), period, period, timeunit);
				}
			}
			catch (Throwable t) {
				shutdownExecutor();
				LOG.info("Could not instantiate metrics reporter. No metrics will be exposed/reported.", t);
				reporter = null;
			}

			this.reporter = reporter;
			this.executor = executor;
		}
	}

	public char getDelimiter() {
		return this.delimiter;
	}

	public MetricReporter getReporter() {
		return reporter;
	}

	/**
	 * Shuts down this registry and the associated {@link org.apache.flink.metrics.reporter.MetricReporter}.
	 */
	public void shutdown() {
		if (reporter != null) {
			try {
				reporter.close();
			} catch (Throwable t) {
				LOG.warn("Metrics reporter did not shut down cleanly", t);
			}
		}
		shutdownExecutor();
	}

	private void shutdownExecutor() {
		if (executor != null) {
			executor.shutdown();

			try {
				if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
					executor.shutdownNow();
				}
			} catch (InterruptedException e) {
				executor.shutdownNow();
			}
		}
	}

	public ScopeFormats getScopeFormats() {
		return scopeFormats;
	}

	// ------------------------------------------------------------------------
	//  Metrics (de)registration
	// ------------------------------------------------------------------------

	/**
	 * Registers a new {@link Metric} with this registry.
	 *
	 * @param metric      the metric that was added
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	public void register(Metric metric, String metricName, MetricGroup group) {
		try {
			if (reporter != null) {
				reporter.notifyOfAddedMetric(metric, metricName, group);
			}
		} catch (Exception e) {
			LOG.error("Error while registering metric.", e);
		}
	}

	/**
	 * Un-registers the given {@link org.apache.flink.metrics.Metric} with this registry.
	 *
	 * @param metric      the metric that should be removed
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	public void unregister(Metric metric, String metricName, MetricGroup group) {
		try {
			if (reporter != null) {
				reporter.notifyOfRemovedMetric(metric, metricName, group);
			}
		} catch (Exception e) {
			LOG.error("Error while registering metric.", e);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	static MetricConfig createReporterConfig(Configuration config) {
		MetricConfig reporterConfig = new MetricConfig();

		String[] arguments = config.getString(ConfigConstants.METRICS_REPORTER_ARGUMENTS, "").split(" ");
		if (arguments.length > 1) {
			for (int x = 0; x < arguments.length; x += 2) {
				reporterConfig.setProperty(arguments[x].replace("--", ""), arguments[x + 1]);
			}
		}
		return reporterConfig;
	}

	static ScopeFormats createScopeConfig(Configuration config) {
		String jmFormat = config.getString(
			ConfigConstants.METRICS_SCOPE_NAMING_JM, ScopeFormat.DEFAULT_SCOPE_JOBMANAGER_GROUP);
		String jmJobFormat = config.getString(
			ConfigConstants.METRICS_SCOPE_NAMING_JM_JOB, ScopeFormat.DEFAULT_SCOPE_JOBMANAGER_JOB_GROUP);
		String tmFormat = config.getString(
			ConfigConstants.METRICS_SCOPE_NAMING_TM, ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_GROUP);
		String tmJobFormat = config.getString(
			ConfigConstants.METRICS_SCOPE_NAMING_TM_JOB, ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_JOB_GROUP);
		String taskFormat = config.getString(
			ConfigConstants.METRICS_SCOPE_NAMING_TASK, ScopeFormat.DEFAULT_SCOPE_TASK_GROUP);
		String operatorFormat = config.getString(
			ConfigConstants.METRICS_SCOPE_NAMING_OPERATOR, ScopeFormat.DEFAULT_SCOPE_OPERATOR_GROUP);

		return new ScopeFormats(jmFormat, jmJobFormat, tmFormat, tmJobFormat, taskFormat, operatorFormat);
	}

	// ------------------------------------------------------------------------

	/**
	 * This task is explicitly a static class, so that it does not hold any references to the enclosing
	 * MetricsRegistry instance.
	 *
	 * This is a subtle difference, but very important: With this static class, the enclosing class instance
	 * may become garbage-collectible, whereas with an anonymous inner class, the timer thread
	 * (which is a GC root) will hold a reference via the timer task and its enclosing instance pointer.
	 * Making the MetricsRegistry garbage collectible makes the java.util.Timer garbage collectible,
	 * which acts as a fail-safe to stop the timer thread and prevents resource leaks.
	 */
	private static final class ReporterTask extends TimerTask {

		private final Scheduled reporter;

		private ReporterTask(Scheduled reporter) {
			this.reporter = reporter;
		}

		@Override
		public void run() {
			try {
				reporter.report();
			} catch (Throwable t) {
				LOG.warn("Error while reporting metrics", t);
			}
		}
	}
}
