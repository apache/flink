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

package org.apache.flink.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.AbstractMetricGroup;
import org.apache.flink.metrics.groups.scope.ScopeFormat;
import org.apache.flink.metrics.groups.scope.ScopeFormats;
import org.apache.flink.metrics.reporter.JMXReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * A MetricRegistry keeps track of all registered {@link Metric Metrics}. It serves as the
 * connection between {@link MetricGroup MetricGroups} and {@link MetricReporter MetricReporters}.
 */
@Internal
public class MetricRegistry {

	// ------------------------------------------------------------------------
	//  configuration keys
	// ------------------------------------------------------------------------

	public static final String KEY_METRICS_REPORTER_CLASS = "metrics.reporter.class";
	public static final String KEY_METRICS_REPORTER_ARGUMENTS = "metrics.reporter.arguments";
	public static final String KEY_METRICS_REPORTER_INTERVAL = "metrics.reporter.interval";

	public static final String KEY_METRICS_SCOPE_NAMING_TM = "metrics.scopeName.tm";
	public static final String KEY_METRICS_SCOPE_NAMING_JOB = "metrics.scopeName.job";
	public static final String KEY_METRICS_SCOPE_NAMING_TASK = "metrics.scopeName.task";
	public static final String KEY_METRICS_SCOPE_NAMING_OPERATOR = "metrics.scopeName.operator";

	// ------------------------------------------------------------------------
	//  configuration keys
	// ------------------------------------------------------------------------
	
	private static final Logger LOG = LoggerFactory.getLogger(MetricRegistry.class);
	
	private final MetricReporter reporter;
	private final java.util.Timer timer;

	private final ScopeFormats scopeFormats;

	/**
	 * Creates a new MetricRegistry and starts the configured reporter.
	 */
	public MetricRegistry(Configuration config) {
		// first parse the scopeName formats, these are needed for all reporters
		ScopeFormats scopeFormats;
		try {
			scopeFormats = createScopeConfig(config);
		}
		catch (Exception e) {
			LOG.warn("Failed to parse scopeName format, using default scopeName formats", e);
			scopeFormats = new ScopeFormats();
		}
		this.scopeFormats = scopeFormats;

		// second, instantiate any custom configured reporters
		
		final String className = config.getString(KEY_METRICS_REPORTER_CLASS, null);
		if (className == null) {
			this.reporter = null;
			this.timer = null;
		}
		else {
			MetricReporter reporter;
			java.util.Timer timer;
			
			try {
				String configuredPeriod = config.getString(KEY_METRICS_REPORTER_INTERVAL, null);
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
				
				Configuration reporterConfig = createReporterConfig(config, timeunit, period);
			
				Class<?> reporterClass = Class.forName(className);
				reporter = (MetricReporter) reporterClass.newInstance();
				reporter.open(reporterConfig);

				if (reporter instanceof Scheduled) {
					LOG.info("Periodically reporting metrics in intervals of {} {}", period, timeunit.name());
					long millis = timeunit.toMillis(period);
					
					timer = new java.util.Timer("Periodic Metrics Reporter", true);
					timer.schedule(new ReporterTask((Scheduled) reporter), millis, millis);
				}
				else {
					timer = null;
				}
			}
			catch (Throwable t) {
				reporter = new JMXReporter();
				timer = null;
				LOG.error("Could not instantiate custom metrics reporter. Defaulting to JMX metrics export.", t);
			}

			this.reporter = reporter;
			this.timer = timer;
		}
	}

	/**
	 * Shuts down this registry and the associated {@link org.apache.flink.metrics.reporter.MetricReporter}.
	 */
	public void shutdown() {
		if (timer != null) {
			timer.cancel();
		}
		if (reporter != null) {
			try {
				reporter.close();
			} catch (Throwable t) {
				LOG.warn("Metrics reporter did not shut down cleanly", t);
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
	public void register(Metric metric, String metricName, AbstractMetricGroup group) {
		if (reporter != null) {
			reporter.notifyOfAddedMetric(metric, metricName, group);
		}
	}

	/**
	 * Un-registers the given {@link org.apache.flink.metrics.Metric} with this registry.
	 *
	 * @param metric      the metric that should be removed
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
		if (reporter != null) {
			reporter.notifyOfRemovedMetric(metric, metricName, group);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	static Configuration createReporterConfig(Configuration config, TimeUnit timeunit, long period) {
		Configuration reporterConfig = new Configuration();
		reporterConfig.setLong("period", period);
		reporterConfig.setString("timeunit", timeunit.name());

		String[] arguments = config.getString(KEY_METRICS_REPORTER_ARGUMENTS, "").split(" ");
		if (arguments.length > 1) {
			for (int x = 0; x < arguments.length; x += 2) {
				reporterConfig.setString(arguments[x].replace("--", ""), arguments[x + 1]);
			}
		}
		return reporterConfig;
	}

	static ScopeFormats createScopeConfig(Configuration config) {
		String tmFormat = config.getString(
				KEY_METRICS_SCOPE_NAMING_TM, ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_GROUP);
		String jobFormat = config.getString(
				KEY_METRICS_SCOPE_NAMING_JOB, ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_JOB_GROUP);
		String taskFormat = config.getString(
				KEY_METRICS_SCOPE_NAMING_TASK, ScopeFormat.DEFAULT_SCOPE_TASK_GROUP);
		String operatorFormat = config.getString(
				KEY_METRICS_SCOPE_NAMING_OPERATOR, ScopeFormat.DEFAULT_SCOPE_OPERATOR_GROUP);
		
		return new ScopeFormats(tmFormat, jobFormat, taskFormat, operatorFormat);
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
			reporter.report();
		}
	}
}
