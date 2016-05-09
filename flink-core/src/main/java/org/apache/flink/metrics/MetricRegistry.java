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
import org.apache.flink.metrics.groups.Scope;
import org.apache.flink.metrics.reporter.JMXReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.metrics.groups.JobMetricGroup.DEFAULT_SCOPE_JOB;
import static org.apache.flink.metrics.groups.OperatorMetricGroup.DEFAULT_SCOPE_OPERATOR;
import static org.apache.flink.metrics.groups.TaskManagerMetricGroup.DEFAULT_SCOPE_TM;
import static org.apache.flink.metrics.groups.TaskMetricGroup.DEFAULT_SCOPE_TASK;

/**
 * A MetricRegistry keeps track of all registered {@link org.apache.flink.metrics.Metric}s. It serves as the
 * connection between {@link org.apache.flink.metrics.MetricGroup}s and {@link org.apache.flink.metrics.reporter.MetricReporter}s.
 */
@Internal
public class MetricRegistry {
	private static final Logger LOG = LoggerFactory.getLogger(MetricRegistry.class);

	private final MetricReporter reporter;
	private java.util.Timer timer;

	public static final String KEY_METRICS_REPORTER_CLASS = "metrics.reporter.class";
	public static final String KEY_METRICS_REPORTER_ARGUMENTS = "metrics.reporter.arguments";
	public static final String KEY_METRICS_REPORTER_INTERVAL = "metrics.reporter.interval";

	public static final String KEY_METRICS_SCOPE_NAMING_TM = "metrics.scope.tm";
	public static final String KEY_METRICS_SCOPE_NAMING_JOB = "metrics.scope.job";
	public static final String KEY_METRICS_SCOPE_NAMING_TASK = "metrics.scope.task";
	public static final String KEY_METRICS_SCOPE_NAMING_OPERATOR = "metrics.scope.operator";

	private final Scope.ScopeFormat scopeConfig;

	/**
	 * Creates a new {@link MetricRegistry} and starts the configured reporter.
	 */
	public MetricRegistry(Configuration config) {
		try {
			String className = config.getString(KEY_METRICS_REPORTER_CLASS, null);
			if (className == null) {
				LOG.info("No reporter class name defined in flink-conf.yaml, defaulting to " + JMXReporter.class.getName() + ".");
				className = JMXReporter.class.getName();
			}

			this.scopeConfig = createScopeConfig(config);

			Configuration reporterConfig = createReporterConfig(config);
			Class<?> reporterClass = Class.forName(className);
			reporter = (MetricReporter) reporterClass.newInstance();
			reporter.open(reporterConfig);

			if (reporter instanceof Scheduled) {
				String[] interval = config.getString(KEY_METRICS_REPORTER_INTERVAL, "10 SECONDS").split(" ");
				long millis = TimeUnit.valueOf(interval[1]).toMillis(Long.parseLong(interval[0]));
				timer = new java.util.Timer(true);
				timer.schedule(new TimerTask() {
					@Override
					public void run() {
						((Scheduled) reporter).report();
					}
				}, millis, millis);
			}
		} catch (InstantiationException | ClassNotFoundException e) {
			throw new RuntimeException("Error while instantiating reporter.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Implementation error.", e);
		}
	}

	private static Configuration createReporterConfig(Configuration config) {
		String[] interval = config.getString(KEY_METRICS_REPORTER_INTERVAL, "10 SECONDS").split(" ");

		String[] arguments = config.getString(KEY_METRICS_REPORTER_ARGUMENTS, "").split(" ");

		Configuration reporterConfig = new Configuration();
		reporterConfig.setString("period", interval[0]);
		reporterConfig.setString("timeunit", interval[1]);

		if (arguments.length > 1) {
			for (int x = 0; x < arguments.length; x += 2) {
				reporterConfig.setString(arguments[x].replace("--", ""), arguments[x + 1]);
			}
		}
		return reporterConfig;
	}

	private static Scope.ScopeFormat createScopeConfig(Configuration config) {
		String tmFormat = config.getString(KEY_METRICS_SCOPE_NAMING_TM, DEFAULT_SCOPE_TM);
		String jobFormat = config.getString(KEY_METRICS_SCOPE_NAMING_JOB, DEFAULT_SCOPE_JOB);
		String taskFormat = config.getString(KEY_METRICS_SCOPE_NAMING_TASK, DEFAULT_SCOPE_TASK);
		String operatorFormat = config.getString(KEY_METRICS_SCOPE_NAMING_OPERATOR, DEFAULT_SCOPE_OPERATOR);


		Scope.ScopeFormat format = new Scope.ScopeFormat();
		format.setTaskManagerFormat(tmFormat);
		format.setJobFormat(jobFormat);
		format.setTaskFormat(taskFormat);
		format.setOperatorFormat(operatorFormat);
		return format;
	}

	public Scope.ScopeFormat getScopeConfig() {
		return this.scopeConfig;
	}

	/**
	 * Shuts down this registry and the associated {@link org.apache.flink.metrics.reporter.MetricReporter}.
	 */
	public void shutdown() {
		if (timer != null) {
			timer.cancel();
		}
		if (reporter != null) {
			reporter.close();
		}
	}

	/**
	 * Registers a new {@link org.apache.flink.metrics.Metric} with this registry.
	 *
	 * @param metric metric to register
	 * @param name   name of the metric
	 * @param parent group that contains the metric
	 */
	public void register(Metric metric, String name, AbstractMetricGroup parent) {
		String metricName = reporter.generateName(name, parent.generateScope());


		this.reporter.notifyOfAddedMetric(metric, metricName);
	}

	/**
	 * Un-registers the given {@link org.apache.flink.metrics.Metric} with this registry.
	 *
	 * @param metric metric to un-register
	 * @param name   name of the metric
	 * @param parent group that contains the metric
	 */
	public void unregister(Metric metric, String name, AbstractMetricGroup parent) {
		String metricName = reporter.generateName(name, parent.generateScope());
		
		this.reporter.notifyOfRemovedMetric(metric, metricName);
	}
}
