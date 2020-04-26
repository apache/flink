/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexIOMetricsInfo;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Test for the {@link JobDetailsHandler}.
 */
public class JobDetailsHandlerTest extends TestLogger {

	private static JobDetailsHandler jobDetailsHandler;

	private static IOMetrics ioMetrics;
	private static JobVertexIOMetricsInfo jobVertexMetrics;

	private static final JobID JOB_ID = JobID.generate();
	private static final JobVertexID VERTEX_ID = new JobVertexID();

	@Before
	public void setup() {
		final long bytesIn = 1L;
		final long bytesOut = 10L;
		final long recordsIn = 20L;
		final long recordsOut = 30L;
		final float usageInputFloatingBuffers = 0.1f;
		final float usageInputExclusiveBuffers = 0.1f;
		final float usageOutPool = 0.1f;
		final boolean isBackPressured = false;

		ioMetrics = new IOMetrics(
			bytesIn,
			bytesOut,
			recordsIn,
			recordsOut,
			usageInputFloatingBuffers,
			usageInputExclusiveBuffers,
			usageOutPool,
			isBackPressured);

		jobVertexMetrics = new JobVertexIOMetricsInfo(
			ioMetrics.getNumBytesIn(),
			true,
			ioMetrics.getNumBytesOut(),
			true,
			ioMetrics.getNumRecordsIn(),
			true,
			ioMetrics.getNumRecordsOut(),
			true,
			ioMetrics.getUsageInputExclusiveBuffers(),
			true,
			ioMetrics.getUsageInputFloatingBuffers(),
			true,
			ioMetrics.getUsageOutPool(),
			true,
			ioMetrics.isBackPressured(),
			true
		);

		MetricFetcher fetcher = new MetricFetcherImpl<RestfulGateway>(
			mock(GatewayRetriever.class),
			mock(MetricQueryServiceRetriever.class),
			Executors.directExecutor(),
			TestingUtils.TIMEOUT(),
			MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue());
		MetricStore store = fetcher.getMetricStore();

		Collection<MetricDump> metricDumps = getMetricDumps();
		for (MetricDump dump : metricDumps) {
			store.add(dump);
		}

		jobDetailsHandler = new JobDetailsHandler(
			() -> null,
			TestingUtils.TIMEOUT(),
			Collections.emptyMap(),
			JobDetailsHeaders.getInstance(),
			new DefaultExecutionGraphCache(TestingUtils.TIMEOUT(), TestingUtils.TIMEOUT()),
			TestingUtils.defaultExecutor(),
			fetcher);
	}

	@Test
	public void testGetJobDetailsInfo() throws Exception {

		final ArchivedExecutionGraph archivedExecutionGraph = createArchivedExecutionGraph(JOB_ID, ioMetrics);
		final TestingRestfulGateway restfulGateway = new TestingRestfulGateway.Builder()
			.setRequestJobFunction(jobId -> CompletableFuture.completedFuture(archivedExecutionGraph)).build();
		final HandlerRequest<EmptyRequestBody, JobMessageParameters> request = createRequest(
			archivedExecutionGraph.getJobID());
		final JobDetailsInfo jobDetailsInfo = jobDetailsHandler.handleRequest(request, restfulGateway).get();
		jobDetailsInfo
			.getJobVertexInfos()
			.forEach(jobVertexDetailsInfo ->
				assertThat(jobVertexDetailsInfo.getJobVertexMetrics(), is(equalTo(jobVertexMetrics)))
			);
	}

	private static ArchivedExecutionGraph createArchivedExecutionGraph(JobID jobID, IOMetrics ioMetrics) {
		Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(VERTEX_ID, createArchivedExecutionJobVertex(VERTEX_ID, ioMetrics));
		return new ArchivedExecutionGraphBuilder()
			.setTasks(tasks)
			.setJobID(jobID)
			.build();
	}

	private static ArchivedExecutionJobVertex createArchivedExecutionJobVertex(JobVertexID jobVertexID, IOMetrics ioMetrics) {
		final StringifiedAccumulatorResult[] emptyAccumulators = new StringifiedAccumulatorResult[0];
		final long[] timestamps = new long[ExecutionState.values().length];
		final ExecutionState expectedState = ExecutionState.RUNNING;

		final LocalTaskManagerLocation assignedResourceLocation = new LocalTaskManagerLocation();
		final AllocationID allocationID = new AllocationID();

		final int subtaskIndex = 0;
		final int attempt = 0;
		return new ArchivedExecutionJobVertex(
			new ArchivedExecutionVertex[]{
				new ArchivedExecutionVertex(
					subtaskIndex,
					"test task",
					new ArchivedExecution(
						new StringifiedAccumulatorResult[0],
						ioMetrics,
						new ExecutionAttemptID(),
						attempt,
						expectedState,
						null,
						assignedResourceLocation,
						allocationID,
						subtaskIndex,
						timestamps),
					new EvictingBoundedList<>(0)
				)
			},
			jobVertexID,
			jobVertexID.toString(),
			1,
			1,
			ResourceProfile.UNKNOWN,
			emptyAccumulators);
	}

	private static HandlerRequest<EmptyRequestBody, JobMessageParameters> createRequest(JobID jobId) throws HandlerRequestException {
		final Map<String, String> pathParameters = new HashMap<>();
		pathParameters.put(JobIDPathParameter.KEY, jobId.toString());

		return new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new JobExceptionsMessageParameters(),
			pathParameters,
			Collections.emptyMap());
	}

	protected static Collection<MetricDump> getMetricDumps() {
		Collection<MetricDump> dumps = new ArrayList<>(8);
		QueryScopeInfo.TaskQueryScopeInfo task1 = new QueryScopeInfo.TaskQueryScopeInfo(JOB_ID.toString(), VERTEX_ID.toString(), 0);
		MetricDump.CounterDump cd1 = new MetricDump.CounterDump(task1, MetricNames.IO_NUM_BYTES_IN, ioMetrics.getNumBytesIn());
		MetricDump.CounterDump cd2 = new MetricDump.CounterDump(task1, MetricNames.IO_NUM_BYTES_OUT, ioMetrics.getNumBytesOut());
		MetricDump.CounterDump cd3 = new MetricDump.CounterDump(task1, MetricNames.IO_NUM_RECORDS_IN, ioMetrics.getNumRecordsIn());
		MetricDump.CounterDump cd4 = new MetricDump.CounterDump(task1, MetricNames.IO_NUM_RECORDS_OUT, ioMetrics.getNumRecordsOut());
		MetricDump.GaugeDump cd5 = new MetricDump.GaugeDump(task1, MetricNames.USAGE_SHUFFLE_NETTY_INPUT_FLOATING_BUFFERS, "" + ioMetrics.getUsageInputFloatingBuffers());
		MetricDump.GaugeDump cd6 = new MetricDump.GaugeDump(task1, MetricNames.USAGE_SHUFFLE_NETTY_INPUT_EXCLUSIVE_BUFFERS, "" + ioMetrics.getUsageInputExclusiveBuffers());
		MetricDump.GaugeDump cd7 = new MetricDump.GaugeDump(task1, MetricNames.USAGE_SHUFFLE_NETTY_OUTPUT_POOL_USAGE, "" + ioMetrics.getUsageOutPool());
		MetricDump.GaugeDump cd8 = new MetricDump.GaugeDump(task1, MetricNames.IS_BACKPRESSURED, "" + ioMetrics.isBackPressured());
		dumps.add(cd1);
		dumps.add(cd2);
		dumps.add(cd3);
		dumps.add(cd4);
		dumps.add(cd5);
		dumps.add(cd6);
		dumps.add(cd7);
		dumps.add(cd8);
		return dumps;
	}
}

