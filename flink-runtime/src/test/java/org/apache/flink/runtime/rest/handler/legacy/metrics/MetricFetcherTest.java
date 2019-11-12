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

package org.apache.flink.runtime.rest.handler.legacy.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.dump.TestingMetricQueryServiceGateway;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for the MetricFetcher.
 */
public class MetricFetcherTest extends TestLogger {
	@Test
	public void testUpdate() {
		final Time timeout = Time.seconds(10L);

		// ========= setup TaskManager =================================================================================

		JobID jobID = new JobID();
		ResourceID tmRID = ResourceID.generate();

		// ========= setup QueryServices ================================================================================

		final MetricQueryServiceGateway jmQueryService = new TestingMetricQueryServiceGateway.Builder()
			.setQueryMetricsSupplier(() -> CompletableFuture.completedFuture(new MetricDumpSerialization.MetricSerializationResult(new byte[0], new byte[0], new byte[0], new byte[0], 0, 0, 0, 0)))
			.build();

		MetricDumpSerialization.MetricSerializationResult requestMetricsAnswer = createRequestDumpAnswer(tmRID, jobID);
		final MetricQueryServiceGateway tmQueryService = new TestingMetricQueryServiceGateway.Builder()
			.setQueryMetricsSupplier(() -> CompletableFuture.completedFuture(requestMetricsAnswer))
			.build();

		// ========= setup JobManager ==================================================================================

		final TestingRestfulGateway restfulGateway = new TestingRestfulGateway.Builder()
			.setRequestMultipleJobDetailsSupplier(() -> CompletableFuture.completedFuture(new MultipleJobsDetails(Collections.emptyList())))
			.setRequestMetricQueryServiceGatewaysSupplier(() -> CompletableFuture.completedFuture(Collections.singleton(jmQueryService.getAddress())))
			.setRequestTaskManagerMetricQueryServiceGatewaysSupplier(() -> CompletableFuture.completedFuture(Collections.singleton(Tuple2.of(tmRID, tmQueryService.getAddress()))))
			.build();

		final GatewayRetriever<RestfulGateway> retriever = () -> CompletableFuture.completedFuture(restfulGateway);

		// ========= start MetricFetcher testing =======================================================================
		MetricFetcher fetcher = new MetricFetcherImpl<>(
			retriever,
			address -> CompletableFuture.completedFuture(tmQueryService),
			Executors.directExecutor(),
			timeout,
			MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue());

		// verify that update fetches metrics and updates the store
		fetcher.update();
		MetricStore store = fetcher.getMetricStore();
		synchronized (store) {
			assertEquals("7", store.getJobManagerMetricStore().getMetric("abc.hist_min"));
			assertEquals("6", store.getJobManagerMetricStore().getMetric("abc.hist_max"));
			assertEquals("4.0", store.getJobManagerMetricStore().getMetric("abc.hist_mean"));
			assertEquals("0.5", store.getJobManagerMetricStore().getMetric("abc.hist_median"));
			assertEquals("5.0", store.getJobManagerMetricStore().getMetric("abc.hist_stddev"));
			assertEquals("0.75", store.getJobManagerMetricStore().getMetric("abc.hist_p75"));
			assertEquals("0.9", store.getJobManagerMetricStore().getMetric("abc.hist_p90"));
			assertEquals("0.95", store.getJobManagerMetricStore().getMetric("abc.hist_p95"));
			assertEquals("0.98", store.getJobManagerMetricStore().getMetric("abc.hist_p98"));
			assertEquals("0.99", store.getJobManagerMetricStore().getMetric("abc.hist_p99"));
			assertEquals("0.999", store.getJobManagerMetricStore().getMetric("abc.hist_p999"));

			assertEquals("x", store.getTaskManagerMetricStore(tmRID.toString()).metrics.get("abc.gauge"));
			assertEquals("5.0", store.getJobMetricStore(jobID.toString()).metrics.get("abc.jc"));
			assertEquals("2", store.getTaskMetricStore(jobID.toString(), "taskid").metrics.get("2.abc.tc"));
			assertEquals("1", store.getTaskMetricStore(jobID.toString(), "taskid").metrics.get("2.opname.abc.oc"));
		}
	}

	private static MetricDumpSerialization.MetricSerializationResult createRequestDumpAnswer(ResourceID tmRID, JobID jobID) {
		Map<Counter, Tuple2<QueryScopeInfo, String>> counters = new HashMap<>();
		Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();
		Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms = new HashMap<>();
		Map<Meter, Tuple2<QueryScopeInfo, String>> meters = new HashMap<>();

		SimpleCounter c1 = new SimpleCounter();
		SimpleCounter c2 = new SimpleCounter();

		c1.inc(1);
		c2.inc(2);

		counters.put(c1, new Tuple2<>(new QueryScopeInfo.OperatorQueryScopeInfo(jobID.toString(), "taskid", 2, "opname", "abc"), "oc"));
		counters.put(c2, new Tuple2<>(new QueryScopeInfo.TaskQueryScopeInfo(jobID.toString(), "taskid", 2, "abc"), "tc"));
		meters.put(new Meter() {
			@Override
			public void markEvent() {
			}

			@Override
			public void markEvent(long n) {
			}

			@Override
			public double getRate() {
				return 5;
			}

			@Override
			public long getCount() {
				return 10;
			}
		}, new Tuple2<>(new QueryScopeInfo.JobQueryScopeInfo(jobID.toString(), "abc"), "jc"));
		gauges.put(new Gauge<String>() {
			@Override
			public String getValue() {
				return "x";
			}
		}, new Tuple2<>(new QueryScopeInfo.TaskManagerQueryScopeInfo(tmRID.toString(), "abc"), "gauge"));
		histograms.put(new TestHistogram(), new Tuple2<>(new QueryScopeInfo.JobManagerQueryScopeInfo("abc"), "hist"));

		MetricDumpSerialization.MetricDumpSerializer serializer = new MetricDumpSerialization.MetricDumpSerializer();
		MetricDumpSerialization.MetricSerializationResult dump = serializer.serialize(counters, gauges, histograms, meters);
		serializer.close();

		return dump;
	}

	@Test
	public void testLongUpdateInterval() {
		final long updateInterval = 1000L;
		final AtomicInteger requestMetricQueryServiceGatewaysCounter = new AtomicInteger(0);
		final RestfulGateway restfulGateway = createRestfulGateway(requestMetricQueryServiceGatewaysCounter);

		final MetricFetcher fetcher = createMetricFetcher(updateInterval, restfulGateway);

		fetcher.update();
		fetcher.update();

		assertThat(requestMetricQueryServiceGatewaysCounter.get(), is(1));
	}

	@Test
	public void testShortUpdateInterval() throws InterruptedException {
		final long updateInterval = 1L;
		final AtomicInteger requestMetricQueryServiceGatewaysCounter = new AtomicInteger(0);
		final RestfulGateway restfulGateway = createRestfulGateway(requestMetricQueryServiceGatewaysCounter);

		final MetricFetcher fetcher = createMetricFetcher(updateInterval, restfulGateway);

		fetcher.update();

		final long start = System.currentTimeMillis();
		long difference = 0L;

		while (difference <= updateInterval) {
			Thread.sleep(2L * updateInterval);
			difference = System.currentTimeMillis() - start;
		}

		fetcher.update();

		assertThat(requestMetricQueryServiceGatewaysCounter.get(), is(2));
	}

	@Nonnull
	private MetricFetcher createMetricFetcher(long updateInterval, RestfulGateway restfulGateway) {
		return new MetricFetcherImpl<>(
			() -> CompletableFuture.completedFuture(restfulGateway),
			address -> null,
			Executors.directExecutor(),
			Time.seconds(10L),
			updateInterval);
	}

	private RestfulGateway createRestfulGateway(AtomicInteger requestMetricQueryServiceGatewaysCounter) {
		return new TestingRestfulGateway.Builder()
			.setRequestMetricQueryServiceGatewaysSupplier(() -> {
				requestMetricQueryServiceGatewaysCounter.incrementAndGet();
				return new CompletableFuture<>();
			})
			.build();
	}
}
