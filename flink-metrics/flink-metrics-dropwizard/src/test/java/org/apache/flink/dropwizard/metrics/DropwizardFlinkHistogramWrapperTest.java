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

package org.apache.flink.dropwizard.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.ScheduledDropwizardReporter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DropwizardFlinkHistogramWrapperTest extends TestLogger {

	/**
	 * Tests the histogram functionality of the DropwizardHistogramWrapper.
	 */
	@Test
	public void testDropwizardHistogramWrapper() {
		int size = 10;
		DropwizardHistogramWrapper histogramWrapper = new DropwizardHistogramWrapper(
			new com.codahale.metrics.Histogram(new SlidingWindowReservoir(size)));

		for (int i = 0; i < size; i++) {
			histogramWrapper.update(i);

			assertEquals(i + 1, histogramWrapper.getCount());
			assertEquals(i, histogramWrapper.getStatistics().getMax());
			assertEquals(0, histogramWrapper.getStatistics().getMin());
		}

		assertEquals(size, histogramWrapper.getStatistics().size());
		assertEquals((size - 1)/2.0, histogramWrapper.getStatistics().getQuantile(0.5), 0.001);

		for (int i = size; i < 2 * size; i++) {
			histogramWrapper.update(i);

			assertEquals(i + 1, histogramWrapper.getCount());
			assertEquals(i, histogramWrapper.getStatistics().getMax());
			assertEquals(i + 1 - size, histogramWrapper.getStatistics().getMin());
		}

		assertEquals(size, histogramWrapper.getStatistics().size());
		assertEquals(size + (size - 1)/2.0, histogramWrapper.getStatistics().getQuantile(0.5), 0.001);
	}

	/**
	 * Tests that the DropwizardHistogramWrapper reports correct dropwizard snapshots to the
	 * ScheduledReporter.
	 */
	@Test
	public void testDropwizardHistogramWrapperReporting() throws Exception {
		long reportingInterval = 1000;
		long timeout = 30000;
		int size = 10;
		String histogramMetricName = "histogram";
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_REPORTERS_LIST, "my_reporter");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "my_reporter." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestingReporter.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "my_reporter." + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, reportingInterval + " MILLISECONDS");

		MetricRegistry registry = null;

		try {
			registry = new MetricRegistry(config);
			DropwizardHistogramWrapper histogramWrapper = new DropwizardHistogramWrapper(
				new com.codahale.metrics.Histogram(new SlidingWindowReservoir(size)));

			TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(registry, "localhost", "tmId");

			metricGroup.histogram(histogramMetricName, histogramWrapper);

			String fullMetricName = metricGroup.getMetricIdentifier(histogramMetricName);

			assertTrue(registry.getReporters().size() == 1);

			MetricReporter reporter = registry.getReporters().get(0);

			assertTrue(reporter instanceof TestingReporter);

			TestingReporter testingReporter = (TestingReporter) reporter;

			TestingScheduledReporter scheduledReporter = testingReporter.scheduledReporter;

			// check that the metric has been registered
			assertEquals(1, testingReporter.getMetrics().size());

			for (int i = 0; i < size; i++) {
				histogramWrapper.update(i);
			}

			Future<Snapshot> snapshotFuture = scheduledReporter.getNextHistogramSnapshot(fullMetricName);

			Snapshot snapshot = snapshotFuture.get(timeout, TimeUnit.MILLISECONDS);

			assertEquals(0, snapshot.getMin());
			assertEquals((size - 1) / 2.0, snapshot.getMedian(), 0.001);
			assertEquals(size - 1, snapshot.getMax());
			assertEquals(size, snapshot.size());

			registry.unregister(histogramWrapper, "histogram", metricGroup);

			// check that the metric has been de-registered
			assertEquals(0, testingReporter.getMetrics().size());
		} finally {
			if (registry != null) {
				registry.shutdown();
			}
		}
	}

	public static class TestingReporter extends ScheduledDropwizardReporter {
		TestingScheduledReporter scheduledReporter = null;

		@Override
		public ScheduledReporter getReporter(MetricConfig config) {
			scheduledReporter = new TestingScheduledReporter(
				registry,
				getClass().getName(),
				null,
				TimeUnit.MILLISECONDS,
				TimeUnit.MILLISECONDS);

			return scheduledReporter;
		}

		public Map<String, com.codahale.metrics.Metric> getMetrics() {
			return registry.getMetrics();
		}
	}

	static class TestingScheduledReporter extends ScheduledReporter {

		final Map<String, Snapshot> histogramSnapshots = new HashMap<>();
		final Map<String, List<CompletableFuture<Snapshot>>> histogramSnapshotFutures = new HashMap<>();

		protected TestingScheduledReporter(com.codahale.metrics.MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
			super(registry, name, filter, rateUnit, durationUnit);
		}

		@Override
		public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, com.codahale.metrics.Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
			for (Map.Entry<String, com.codahale.metrics.Histogram> entry: histograms.entrySet()) {
				reportHistogram(entry.getKey(), entry.getValue());
			}
		}

		void reportHistogram(String name, com.codahale.metrics.Histogram histogram) {
			histogramSnapshots.put(name, histogram.getSnapshot());

			synchronized (histogramSnapshotFutures) {
				if (histogramSnapshotFutures.containsKey(name)) {
					List<CompletableFuture<Snapshot>> futures = histogramSnapshotFutures.remove(name);

					for (CompletableFuture<Snapshot> future: futures) {
						future.complete(histogram.getSnapshot());
					}
				}
			}
		}

		Future<Snapshot> getNextHistogramSnapshot(String name) {
			synchronized (histogramSnapshotFutures) {
				List<CompletableFuture<Snapshot>> futures;
				if (histogramSnapshotFutures.containsKey(name)) {
					futures = histogramSnapshotFutures.get(name);
				} else {
					futures = new ArrayList<>();
					histogramSnapshotFutures.put(name, futures);
				}

				CompletableFuture<Snapshot> future = new CompletableFuture<>();
				futures.add(future);

				return future;
			}
		}
	}

	static class CompletableFuture<T> implements Future<T> {

		private Exception exception = null;
		private T value = null;

		private Object lock = new Object();

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			synchronized (lock) {
				if (isDone()) {
					return false;
				} else {
					exception = new CancellationException("Future was cancelled.");

					lock.notifyAll();

					return true;
				}
			}
		}

		@Override
		public boolean isCancelled() {
			return exception instanceof CancellationException;
		}

		@Override
		public boolean isDone() {
			return value != null || exception != null;
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			while (!isDone() && !isCancelled()) {
				synchronized (lock) {
					lock.wait();
				}
			}

			if (exception != null) {
				throw new ExecutionException(exception);
			} else if (value != null) {
				return value;
			} else {
				throw new ExecutionException(new Exception("Future did not complete correctly."));
			}
		}

		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			long timeoutMs = unit.toMillis(timeout);
			long timeoutEnd = timeoutMs + System.currentTimeMillis();

			while (!isDone() && !isCancelled() && timeoutMs > 0) {
				synchronized (lock) {
					lock.wait(unit.toMillis(timeoutMs));
				}

				timeoutMs = timeoutEnd - System.currentTimeMillis();
			}

			if (exception != null) {
				throw new ExecutionException(exception);
			} else if (value != null) {
				return value;
			} else {
				throw new ExecutionException(new Exception("Future did not complete correctly."));
			}
		}

		public boolean complete(T value) {
			synchronized (lock) {
				if (!isDone()) {
					this.value = value;

					lock.notifyAll();

					return true;
				} else {
					return false;
				}
			}
		}

		public boolean fail(Exception exception) {
			synchronized (lock) {
				if (!isDone()) {
					this.exception = exception;

					lock.notifyAll();

					return true;
				} else {
					return false;
				}
			}
		}
	}
}
