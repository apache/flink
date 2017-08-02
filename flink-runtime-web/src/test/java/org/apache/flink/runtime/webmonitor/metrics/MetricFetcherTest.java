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

package org.apache.flink.runtime.webmonitor.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.TestingHistogram;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaJobManagerRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the MetricFetcher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricFetcher.class)
public class MetricFetcherTest extends TestLogger {
	@Test
	public void testUpdate() throws Exception {
		final Time timeout = Time.seconds(10L);

		// ========= setup TaskManager =================================================================================
		JobID jobID = new JobID();
		InstanceID tmID = new InstanceID();
		ResourceID tmRID = new ResourceID(tmID.toString());
		TaskManagerGateway taskManagerGateway = mock(TaskManagerGateway.class);
		when(taskManagerGateway.getAddress()).thenReturn("/tm/address");

		Instance taskManager = mock(Instance.class);
		when(taskManager.getTaskManagerGateway()).thenReturn(taskManagerGateway);
		when(taskManager.getId()).thenReturn(tmID);
		when(taskManager.getTaskManagerID()).thenReturn(tmRID);

		// ========= setup JobManager ==================================================================================
		JobDetails details = mock(JobDetails.class);
		when(details.getJobId()).thenReturn(jobID);

		JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);

		when(jobManagerGateway.requestJobDetails(anyBoolean(), anyBoolean(), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(new MultipleJobsDetails(new JobDetails[0], new JobDetails[0])));
		when(jobManagerGateway.requestTaskManagerInstances(any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(Collections.singleton(taskManager)));
		when(jobManagerGateway.getAddress()).thenReturn("/jm/address");
		when(jobManagerGateway.requestWebPort(any(Time.class))).thenReturn(CompletableFuture.completedFuture(0));

		AkkaJobManagerRetriever retriever = mock(AkkaJobManagerRetriever.class);
		when(retriever.getJobManagerGatewayNow())
			.thenReturn(Optional.of(jobManagerGateway));

		// ========= setup QueryServices ================================================================================
		MetricQueryServiceGateway jmQueryService = mock(MetricQueryServiceGateway.class);
		MetricQueryServiceGateway tmQueryService = mock(MetricQueryServiceGateway.class);

		MetricDumpSerialization.MetricSerializationResult requestMetricsAnswer = createRequestDumpAnswer(tmID, jobID);

		when(jmQueryService.queryMetrics(any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(new MetricDumpSerialization.MetricSerializationResult(new byte[0], 0, 0, 0, 0)));
		when(tmQueryService.queryMetrics(any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(requestMetricsAnswer));

		MetricQueryServiceRetriever queryServiceRetriever = mock(MetricQueryServiceRetriever.class);
		when(queryServiceRetriever.retrieveService(eq("/jm/" + MetricQueryService.METRIC_QUERY_SERVICE_NAME))).thenReturn(CompletableFuture.completedFuture(jmQueryService));
		when(queryServiceRetriever.retrieveService(eq("/tm/" + MetricQueryService.METRIC_QUERY_SERVICE_NAME + "_" + tmRID.getResourceIdString()))).thenReturn(CompletableFuture.completedFuture(tmQueryService));

		// ========= start MetricFetcher testing =======================================================================
		MetricFetcher fetcher = new MetricFetcher(
			retriever,
			queryServiceRetriever,
			Executors.directExecutor(),
			timeout);

		// verify that update fetches metrics and updates the store
		fetcher.update();
		MetricStore store = fetcher.getMetricStore();
		synchronized (store) {
			assertEquals("7", store.jobManager.metrics.get("abc.hist_min"));
			assertEquals("6", store.jobManager.metrics.get("abc.hist_max"));
			assertEquals("4.0", store.jobManager.metrics.get("abc.hist_mean"));
			assertEquals("0.5", store.jobManager.metrics.get("abc.hist_median"));
			assertEquals("5.0", store.jobManager.metrics.get("abc.hist_stddev"));
			assertEquals("0.75", store.jobManager.metrics.get("abc.hist_p75"));
			assertEquals("0.9", store.jobManager.metrics.get("abc.hist_p90"));
			assertEquals("0.95", store.jobManager.metrics.get("abc.hist_p95"));
			assertEquals("0.98", store.jobManager.metrics.get("abc.hist_p98"));
			assertEquals("0.99", store.jobManager.metrics.get("abc.hist_p99"));
			assertEquals("0.999", store.jobManager.metrics.get("abc.hist_p999"));

			assertEquals("x", store.getTaskManagerMetricStore(tmID.toString()).metrics.get("abc.gauge"));
			assertEquals("5.0", store.getJobMetricStore(jobID.toString()).metrics.get("abc.jc"));
			assertEquals("2", store.getTaskMetricStore(jobID.toString(), "taskid").metrics.get("2.abc.tc"));
			assertEquals("1", store.getTaskMetricStore(jobID.toString(), "taskid").metrics.get("2.opname.abc.oc"));
		}
	}

	private static MetricDumpSerialization.MetricSerializationResult createRequestDumpAnswer(InstanceID tmID, JobID jobID) {
		Map<Counter, Tuple2<QueryScopeInfo, String>> counters = new HashMap<>();
		Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();
		Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms = new HashMap<>();
		Map<Meter, Tuple2<QueryScopeInfo, String>> meters = new HashMap<>();

		SimpleCounter c1 = new SimpleCounter();
		SimpleCounter c2 = new SimpleCounter();

		c1.inc(1);
		c2.inc(2);

		counters.put(c1, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.OperatorQueryScopeInfo(jobID.toString(), "taskid", 2, "opname", "abc"), "oc"));
		counters.put(c2, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.TaskQueryScopeInfo(jobID.toString(), "taskid", 2, "abc"), "tc"));
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
		}, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.JobQueryScopeInfo(jobID.toString(), "abc"), "jc"));
		gauges.put(new Gauge<String>() {
			@Override
			public String getValue() {
				return "x";
			}
		}, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.TaskManagerQueryScopeInfo(tmID.toString(), "abc"), "gauge"));
		histograms.put(new TestingHistogram(), new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.JobManagerQueryScopeInfo("abc"), "hist"));

		MetricDumpSerialization.MetricDumpSerializer serializer = new MetricDumpSerialization.MetricDumpSerializer();
		MetricDumpSerialization.MetricSerializationResult dump = serializer.serialize(counters, gauges, histograms, meters);
		serializer.close();

		return dump;
	}
}
