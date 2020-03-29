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

package org.apache.flink.python.metric;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper class for forwarding Python metrics to Java accumulators and metrics.
 */
@Internal
public final class FlinkMetricContainer {

	private static final String METRIC_KEY_SEPARATOR =
		GlobalConfiguration.loadConfiguration().getString(MetricOptions.SCOPE_DELIMITER);

	private final MetricsContainerStepMap metricsContainers;
	private final MetricGroup baseMetricGroup;
	private final Map<String, Counter> flinkCounterCache;
	private final Map<String, Meter> flinkMeterCache;
	private final Map<String, FlinkDistributionGauge> flinkDistributionGaugeCache;
	private final Map<String, FlinkGauge> flinkGaugeCache;

	public FlinkMetricContainer(MetricGroup metricGroup) {
		this.baseMetricGroup = checkNotNull(metricGroup);
		this.flinkCounterCache = new HashMap<>();
		this.flinkMeterCache = new HashMap<>();
		this.flinkDistributionGaugeCache = new HashMap<>();
		this.flinkGaugeCache = new HashMap<>();
		this.metricsContainers = new MetricsContainerStepMap();
	}

	private MetricsContainerImpl getMetricsContainer(String stepName) {
		return metricsContainers.getContainer(stepName);
	}

	/**
	 * Update this container with metrics from the passed {@link MonitoringInfo}s, and send updates
	 * along to Flink's internal metrics framework.
	 */
	public void updateMetrics(String stepName, List<MonitoringInfo> monitoringInfos) {
		getMetricsContainer(stepName).update(monitoringInfos);
		updateMetrics(stepName);
	}

	/**
	 * Update Flink's internal metrics ({@link #flinkCounterCache}) with the latest metrics for
	 * a given step.
	 */
	private void updateMetrics(String stepName) {
		MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainers);
		MetricQueryResults metricQueryResults =
			metricResults.queryMetrics(MetricsFilter.builder().addStep(stepName).build());
		updateCounterOrMeter(metricQueryResults.getCounters());
		updateDistributions(metricQueryResults.getDistributions());
		updateGauge(metricQueryResults.getGauges());
	}

	private boolean isUserMetric(MetricResult metricResult) {
		MetricName metricName = metricResult.getKey().metricName();
		return (metricName instanceof MonitoringInfoMetricName) &&
			((MonitoringInfoMetricName) metricName).getUrn()
				.contains(MonitoringInfoConstants.Urns.USER_COUNTER);
	}

	private void updateCounterOrMeter(Iterable<MetricResult<Long>> counters) {
		for (MetricResult<Long> metricResult : counters) {
			if (!isUserMetric(metricResult)) {
				continue;
			}
			// get identifier
			String flinkMetricIdentifier = getFlinkMetricIdentifierString(metricResult.getKey());

			// get metric type
			ArrayList<String> scopeComponents = getNameSpaceArray(metricResult.getKey());
			if ((scopeComponents.size() % 2) != 0) {
				Meter meter = flinkMeterCache.get(flinkMetricIdentifier);
				if (null == meter) {
					int timeSpanInSeconds =
						Integer.parseInt(scopeComponents.get(scopeComponents.size() - 1));
					MetricGroup metricGroup =
						registerMetricGroup(metricResult.getKey(), baseMetricGroup);
					meter = metricGroup.meter(
						metricResult.getKey().metricName().getName(),
						new MeterView(timeSpanInSeconds));
					flinkMeterCache.put(flinkMetricIdentifier, meter);
				}

				Long update = metricResult.getAttempted();
				meter.markEvent(update);
			} else {
				Counter counter = flinkCounterCache.get(flinkMetricIdentifier);
				if (null == counter) {
					MetricGroup metricGroup =
						registerMetricGroup(metricResult.getKey(), baseMetricGroup);
					counter = metricGroup.counter(metricResult.getKey().metricName().getName());
					flinkCounterCache.put(flinkMetricIdentifier, counter);
				}

				Long update = metricResult.getAttempted();
				counter.inc(update - counter.getCount());
			}
		}
	}

	private void updateDistributions(Iterable<MetricResult<DistributionResult>> distributions) {
		for (MetricResult<DistributionResult> metricResult : distributions) {
			if (!isUserMetric(metricResult)) {
				continue;
			}
			// get identifier
			String flinkMetricIdentifier = getFlinkMetricIdentifierString(metricResult.getKey());
			DistributionResult update = metricResult.getAttempted();

			// update flink metric
			FlinkDistributionGauge gauge = flinkDistributionGaugeCache.get(flinkMetricIdentifier);
			if (gauge == null) {
				MetricGroup metricGroup =
					registerMetricGroup(metricResult.getKey(), baseMetricGroup);
				gauge = metricGroup.gauge(
					metricResult.getKey().metricName().getName(),
					new FlinkDistributionGauge(update));
				flinkDistributionGaugeCache.put(flinkMetricIdentifier, gauge);
			} else {
				gauge.update(update);
			}
		}
	}

	private void updateGauge(Iterable<MetricResult<GaugeResult>> gauges) {
		for (MetricResult<GaugeResult> metricResult : gauges) {
			if (!isUserMetric(metricResult)) {
				continue;
			}
			// get identifier
			String flinkMetricIdentifier = getFlinkMetricIdentifierString(metricResult.getKey());

			GaugeResult update = metricResult.getAttempted();

			// update flink metric
			FlinkGauge gauge = flinkGaugeCache.get(flinkMetricIdentifier);
			if (gauge == null) {
				MetricGroup metricGroup = registerMetricGroup(metricResult.getKey(), baseMetricGroup);
				gauge = metricGroup.gauge(
					metricResult.getKey().metricName().getName(),
					new FlinkGauge(update));
				flinkGaugeCache.put(flinkMetricIdentifier, gauge);
			} else {
				gauge.update(update);
			}
		}
	}

	@VisibleForTesting
	static ArrayList getNameSpaceArray(MetricKey metricKey) {
		MetricName metricName = metricKey.metricName();
		try {
			return new ObjectMapper().readValue(metricName.getNamespace(), ArrayList.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(
				String.format("Parse namespace[%s] error. ", metricName.getNamespace()), e);
		}
	}

	@VisibleForTesting
	static String getFlinkMetricIdentifierString(MetricKey metricKey) {
		MetricName metricName = metricKey.metricName();
		ArrayList<String> scopeComponents = getNameSpaceArray(metricKey);
		List<String> results = scopeComponents.subList(0, scopeComponents.size() / 2);
		results.add(metricName.getName());
		return String.join(METRIC_KEY_SEPARATOR, results);
	}

	@VisibleForTesting
	static MetricGroup registerMetricGroup(MetricKey metricKey, MetricGroup metricGroup) {
		ArrayList<String> scopeComponents = getNameSpaceArray(metricKey);
		int size = scopeComponents.size();
		List<String> metricGroupNames = scopeComponents.subList(0, size / 2);
		List<String> metricGroupTypes = scopeComponents.subList(size / 2, size);
		for (int i = 0; i < metricGroupNames.size(); ++i) {
			if (metricGroupTypes.get(i).equals("MetricGroupType.generic")) {
				metricGroup = metricGroup.addGroup(metricGroupNames.get(i));
			} else if (metricGroupTypes.get(i).equals("MetricGroupType.key")) {
				metricGroup = metricGroup.addGroup(
					metricGroupNames.get(i),
					metricGroupNames.get(++i));
			}
		}
		return metricGroup;
	}

	/**
	 * Flink {@link Gauge} for {@link DistributionResult}.
	 */
	public static class FlinkDistributionGauge implements Gauge<DistributionResult> {

		private DistributionResult data;

		FlinkDistributionGauge(DistributionResult data) {
			this.data = data;
		}

		void update(DistributionResult data) {
			this.data = data;
		}

		@Override
		public DistributionResult getValue() {
			return data;
		}
	}

	/**
	 * Flink {@link Gauge} for {@link GaugeResult}.
	 */
	public static class FlinkGauge implements Gauge<Long> {

		private GaugeResult data;

		FlinkGauge(GaugeResult data) {
			this.data = data;
		}

		void update(GaugeResult update) {
			this.data = update;
		}

		@Override
		public Long getValue() {
			return data.getValue();
		}
	}
}
