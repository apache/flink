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

package org.apache.flink.runtime.webmonitor.legacy;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.handler.codec.http.router.Routed;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.ArchiveMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultStringsFound;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsErroneous;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsNotFound;
import org.apache.flink.runtime.messages.accumulators.RequestAccumulatorResultsStringified;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple3;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@ChannelHandler.Sharable
public class JobManagerInfoHandler extends SimpleChannelInboundHandler<Routed> {

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerInfoHandler.class);

	private static final Charset ENCODING = Charset.forName("UTF-8");

	/** Underlying JobManager */
	private final ActorGateway jobmanager;
	private final ActorGateway archive;
	private final FiniteDuration timeout;


	public JobManagerInfoHandler(ActorGateway jobmanager, ActorGateway archive, FiniteDuration timeout) {
		this.jobmanager = jobmanager;
		this.archive = archive;
		this.timeout = timeout;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		DefaultFullHttpResponse response;
		try {
			String result = handleRequest(routed);
			byte[] bytes = result.getBytes(ENCODING);

			response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(bytes));

			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
		}
		catch (Exception e) {
			byte[] bytes = ExceptionUtils.stringifyException(e).getBytes(ENCODING);
			response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
					HttpResponseStatus.INTERNAL_SERVER_ERROR, Unpooled.wrappedBuffer(bytes));
			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
		}

		response.headers().set(HttpHeaders.Names.CONTENT_ENCODING, "utf-8");
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

		KeepAliveWrite.flush(ctx, routed.request(), response);
	}


	@SuppressWarnings("unchecked")
	private String handleRequest(Routed routed) throws Exception {
		if ("archive".equals(routed.queryParam("get"))) {
			Future<Object> response = archive.ask(ArchiveMessages.getRequestArchivedJobs(), timeout);

			Object result = Await.result(response, timeout);

			if(!(result instanceof ArchiveMessages.ArchivedJobs)) {
				throw new RuntimeException("RequestArchiveJobs requires a response of type " +
						"ArchivedJobs. Instead the response is of type " + result.getClass() +".");
			}
			else {
				final List<ExecutionGraph> archivedJobs = new ArrayList<ExecutionGraph>(
						((ArchiveMessages.ArchivedJobs) result).asJavaCollection());

				return writeJsonForArchive(archivedJobs);
			}
		}
		else if ("jobcounts".equals(routed.queryParam("get"))) {
			Future<Object> response = archive.ask(ArchiveMessages.getRequestJobCounts(), timeout);

			Object result = Await.result(response, timeout);

			if (!(result instanceof Tuple3)) {
				throw new RuntimeException("RequestJobCounts requires a response of type " +
						"Tuple3. Instead the response is of type " + result.getClass() +
						".");
			}
			else {
				return writeJsonForJobCounts((Tuple3<Integer, Integer, Integer>) result);
			}
		}
		else if ("job".equals(routed.queryParam("get"))) {
			String jobId = routed.queryParam("job");

			Future<Object> response = archive.ask(new JobManagerMessages.RequestJob(JobID.fromHexString(jobId)),
					timeout);

			Object result = Await.result(response, timeout);

			if (!(result instanceof JobManagerMessages.JobResponse)){
				throw new RuntimeException("RequestJob requires a response of type JobResponse. " +
						"Instead the response is of type " + result.getClass());
			}
			else {
				final JobManagerMessages.JobResponse jobResponse = (JobManagerMessages.JobResponse) result;

				if (jobResponse instanceof JobManagerMessages.JobFound){
					ExecutionGraph archivedJob = ((JobManagerMessages.JobFound)result).executionGraph();
					return writeJsonForArchivedJob(archivedJob);
				}
				else {
					throw new Exception("DoGet:job: Could not find job for job ID " + jobId);
				}
			}
		}
		else if ("groupvertex".equals(routed.queryParam("get"))) {
			String jobId = routed.queryParam("job");
			String groupVertexId = routed.queryParam("groupvertex");

			// No group vertex specified
			if (groupVertexId.equals("null")) {
				throw new Exception("Found null groupVertexId");
			}

			Future<Object> response = archive.ask(new JobManagerMessages.RequestJob(JobID.fromHexString(jobId)),
					timeout);

			Object result = Await.result(response, timeout);

			if (!(result instanceof JobManagerMessages.JobResponse)){
				throw new RuntimeException("RequestJob requires a response of type JobResponse. " +
						"Instead the response is of type " + result.getClass());
			}
			else {
				final JobManagerMessages.JobResponse jobResponse = (JobManagerMessages.JobResponse) result;

				if (jobResponse instanceof JobManagerMessages.JobFound) {
					ExecutionGraph archivedJob = ((JobManagerMessages.JobFound)jobResponse).executionGraph();

					return writeJsonForArchivedJobGroupvertex(archivedJob, JobVertexID.fromHexString(groupVertexId));
				}
				else {
					throw new Exception("DoGet:groupvertex: Could not find job for job ID " + jobId);
				}
			}
		}
		else if ("taskmanagers".equals(routed.queryParam("get"))) {
			Future<Object> response = jobmanager.ask(
					JobManagerMessages.getRequestNumberRegisteredTaskManager(),
					timeout);

			Object result = Await.result(response, timeout);

			if (!(result instanceof Integer)) {
				throw new RuntimeException("RequestNumberRegisteredTaskManager requires a " +
						"response of type Integer. Instead the response is of type " +
						result.getClass() + ".");
			}
			else {
				final int numberOfTaskManagers = (Integer)result;

				final Future<Object> responseRegisteredSlots = jobmanager.ask(
						JobManagerMessages.getRequestTotalNumberOfSlots(),
						timeout);

				final Object resultRegisteredSlots = Await.result(responseRegisteredSlots,
						timeout);

				if (!(resultRegisteredSlots instanceof Integer)) {
					throw new RuntimeException("RequestTotalNumberOfSlots requires a response of " +
							"type Integer. Instaed the response of type " +
							resultRegisteredSlots.getClass() + ".");
				}
				else {
					final int numberOfRegisteredSlots = (Integer) resultRegisteredSlots;

					return "{\"taskmanagers\": " + numberOfTaskManagers + ", " +
							"\"slots\": " + numberOfRegisteredSlots + "}";
				}
			}
		}
		else if ("cancel".equals(routed.queryParam("get"))) {
			String jobId = routed.queryParam("job");

			Future<Object> response = jobmanager.ask(new JobManagerMessages.CancelJob(JobID.fromHexString(jobId)),
					timeout);

			Await.ready(response, timeout);
			return "{}";
		}
		else if ("updates".equals(routed.queryParam("get"))) {
			String jobId = routed.queryParam("job");
			return writeJsonUpdatesForJob(JobID.fromHexString(jobId));
		}
		else if ("version".equals(routed.queryParam("get"))) {
			return writeJsonForVersion();
		}
		else{
			Future<Object> response = jobmanager.ask(JobManagerMessages.getRequestRunningJobs(),
					timeout);

			Object result = Await.result(response, timeout);

			if(!(result instanceof JobManagerMessages.RunningJobs)){
				throw new RuntimeException("RequestRunningJobs requires a response of type " +
						"RunningJobs. Instead the response of type " + result.getClass() + ".");
			}
			else {
				final Iterable<ExecutionGraph> runningJobs =
						((JobManagerMessages.RunningJobs) result).asJavaIterable();

				return writeJsonForJobs(runningJobs);
			}
		}
	}

	private String writeJsonForJobs(Iterable<ExecutionGraph> graphs) {
		StringBuilder bld = new StringBuilder();
		bld.append("[");

		Iterator<ExecutionGraph> it = graphs.iterator();
		// Loop Jobs
		while(it.hasNext()){
			ExecutionGraph graph = it.next();

			writeJsonForJob(bld, graph);

			//Write seperator between json objects
			if(it.hasNext()) {
				bld.append(",");
			}
		}
		bld.append("]");

		return bld.toString();
	}

	private void writeJsonForJob(StringBuilder bld, ExecutionGraph graph) {
		//Serialize job to json
		bld.append("{");
		bld.append("\"jobid\": \"").append(graph.getJobID()).append("\",");
		bld.append("\"jobname\": \"").append(graph.getJobName()).append("\",");
		bld.append("\"status\": \"").append(graph.getState()).append("\",");
		bld.append("\"time\": ").append(graph.getStatusTimestamp(graph.getState())).append(",");

		// Serialize ManagementGraph to json
		bld.append("\"groupvertices\": [");
		boolean first = true;

		for (ExecutionJobVertex groupVertex : graph.getVerticesTopologically()) {
			//Write seperator between json objects
			if (first) {
				first = false;
			} else {
				bld.append(",");
			}
			bld.append(JsonFactory.toJson(groupVertex));
		}
		bld.append("]");
		bld.append("}");
	}

	private String writeJsonForArchive(List<ExecutionGraph> graphs) {
		StringBuilder bld = new StringBuilder();
		bld.append("[");

		// sort jobs by time
		Collections.sort(graphs, new Comparator<ExecutionGraph>() {
			@Override
			public int compare(ExecutionGraph o1, ExecutionGraph o2) {
				if (o1.getStatusTimestamp(o1.getState()) < o2.getStatusTimestamp(o2.getState())) {
					return 1;
				} else {
					return -1;
				}
			}

		});

		// Loop Jobs
		for (int i = 0; i < graphs.size(); i++) {
			ExecutionGraph graph = graphs.get(i);

			//Serialize job to json
			bld.append("{");
			bld.append("\"jobid\": \"").append(graph.getJobID()).append("\",");
			bld.append("\"jobname\": \"").append(graph.getJobName()).append("\",");
			bld.append("\"status\": \"").append(graph.getState()).append("\",");
			bld.append("\"time\": ").append(graph.getStatusTimestamp(graph.getState()));

			bld.append("}");

			//Write seperator between json objects
			if(i != graphs.size() - 1) {
				bld.append(",");
			}
		}
		bld.append("]");
		return bld.toString();
	}

	private String writeJsonForJobCounts(Tuple3<Integer, Integer, Integer> jobCounts) {
		return "{\"finished\": " + jobCounts._1() + ",\"canceled\": " + jobCounts._2() + ",\"failed\": "
				+ jobCounts._3() + "}";
	}


	private String writeJsonForArchivedJob(ExecutionGraph graph) {
		StringBuilder bld = new StringBuilder();

		bld.append("[");
		bld.append("{");
		bld.append("\"jobid\": \"").append(graph.getJobID()).append("\",");
		bld.append("\"jobname\": \"").append(graph.getJobName()).append("\",");
		bld.append("\"status\": \"").append(graph.getState()).append("\",");
		bld.append("\"SCHEDULED\": ").append(graph.getStatusTimestamp(JobStatus.CREATED)).append(",");
		bld.append("\"RUNNING\": ").append(graph.getStatusTimestamp(JobStatus.RUNNING)).append(",");
		bld.append("\"FINISHED\": ").append(graph.getStatusTimestamp(JobStatus.FINISHED)).append(",");
		bld.append("\"FAILED\": ").append(graph.getStatusTimestamp(JobStatus.FAILED)).append(",");
		bld.append("\"CANCELED\": ").append(graph.getStatusTimestamp(JobStatus.CANCELED)).append(",");

		if (graph.getState() == JobStatus.FAILED) {
			bld.append("\"failednodes\": [");
			boolean first = true;
			for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
				if (vertex.getExecutionState() == ExecutionState.FAILED) {
					InstanceConnectionInfo location = vertex.getCurrentAssignedResourceLocation();
					Throwable failureCause = vertex.getFailureCause();
					if (location != null || failureCause != null) {
						if (first) {
							first = false;
						} else {
							bld.append(",");
						}
						bld.append("{");
						bld.append("\"node\": \"").append(location == null ? "(none)" : location.getFQDNHostname()).append("\",");
						bld.append("\"message\": \"").append(failureCause == null ? "" : StringUtils.escapeHtml(ExceptionUtils.stringifyException(failureCause))).append("\"");
						bld.append("}");
					}
				}
			}
			bld.append("],");
		}

		// Serialize ManagementGraph to json
		bld.append("\"groupvertices\": [");
		boolean first = true;
		for (ExecutionJobVertex groupVertex : graph.getVerticesTopologically()) {
			//Write seperator between json objects
			if (first) {
				first = false;
			} else {
				bld.append(",");
			}

			bld.append(JsonFactory.toJson(groupVertex));

		}
		bld.append("],");

		// write user config
		ExecutionConfig ec = graph.getExecutionConfig();
		if(ec != null) {
			bld.append("\"executionConfig\": {");
			bld.append("\"Execution Mode\": \"").append(ec.getExecutionMode()).append("\",");
			bld.append("\"Number of execution retries\": \"").append(ec.getNumberOfExecutionRetries()).append("\",");
			bld.append("\"Job parallelism\": \"").append(ec.getParallelism()).append("\",");
			bld.append("\"Object reuse mode\": \"").append(ec.isObjectReuseEnabled()).append("\"");
			ExecutionConfig.GlobalJobParameters uc = ec.getGlobalJobParameters();
			if(uc != null) {
				Map<String, String> ucVals = uc.toMap();
				if (ucVals != null) {
					String ucString = "{";
					int i = 0;
					for (Map.Entry<String, String> ucVal : ucVals.entrySet()) {
						ucString += "\"" + ucVal.getKey() + "\":\"" + ucVal.getValue() + "\"";
						if (++i < ucVals.size()) {
							ucString += ",\n";
						}
					}
					bld.append(", \"userConfig\": ").append(ucString).append("}");
				}
				else {
					LOG.debug("GlobalJobParameters.toMap() did not return anything");
				}
			}
			else {
				LOG.debug("No GlobalJobParameters were set in the execution config");
			}
			bld.append("},");
		}
		else {
			LOG.warn("Unable to retrieve execution config from execution graph");
		}

		// write accumulators
		final Future<Object> response = jobmanager.ask(
				new RequestAccumulatorResultsStringified(graph.getJobID()),
				timeout);

		Object result;
		try {
			result = Await.result(response, timeout);
		}
		catch (Exception ex) {
			throw new RuntimeException("Could not retrieve the accumulator results from the job manager.", ex);
		}

		if (result instanceof AccumulatorResultStringsFound) {
			StringifiedAccumulatorResult[] accumulators = ((AccumulatorResultStringsFound) result).result();

			bld.append("\n\"accumulators\": [");
			int i = 0;
			for (StringifiedAccumulatorResult accumulator : accumulators) {
				bld.append("{ \"name\": \"").append(accumulator.getName()).append(" (").append(accumulator.getType()).append(")\",").append(" \"value\": \"").append(accumulator.getValue()).append("\"}\n");
				if (++i < accumulators.length) {
					bld.append(",");
				}
			}
			bld.append("],\n");
		}
		else if (result instanceof AccumulatorResultsNotFound) {
			bld.append("\n\"accumulators\": [],");
		}
		else if (result instanceof AccumulatorResultsErroneous) {
			LOG.error("Could not obtain accumulators for job " + graph.getJobID(),
					((AccumulatorResultsErroneous) result).cause());
		}
		else {
			throw new RuntimeException("RequestAccumulatorResults requires a response of type " +
					"AccumulatorResultStringsFound. Instead the response is of type " +
					result.getClass() + ".");
		}

		bld.append("\"groupverticetimes\": {");
		first = true;

		for (ExecutionJobVertex groupVertex : graph.getVerticesTopologically()) {
			if (first) {
				first = false;
			} else {
				bld.append(",");
			}

			// Calculate start and end time for groupvertex
			long started = Long.MAX_VALUE;
			long ended = 0;

			// Take earliest running state and latest endstate of groupmembers
			for (ExecutionVertex vertex : groupVertex.getTaskVertices()) {

				long running = vertex.getStateTimestamp(ExecutionState.RUNNING);
				if (running != 0 && running < started) {
					started = running;
				}

				long finished = vertex.getStateTimestamp(ExecutionState.FINISHED);
				long canceled = vertex.getStateTimestamp(ExecutionState.CANCELED);
				long failed = vertex.getStateTimestamp(ExecutionState.FAILED);

				if (finished != 0 && finished > ended) {
					ended = finished;
				}

				if (canceled != 0 && canceled > ended) {
					ended = canceled;
				}

				if (failed != 0 && failed > ended) {
					ended = failed;
				}

			}

			bld.append("\"").append(groupVertex.getJobVertexId()).append("\": {");
			bld.append("\"groupvertexid\": \"").append(groupVertex.getJobVertexId()).append("\",");
			bld.append("\"groupvertexname\": \"").append(groupVertex).append("\",");
			bld.append("\"STARTED\": ").append(started).append(",");
			bld.append("\"ENDED\": ").append(ended);
			bld.append("}");

		}

		bld.append("}");
		bld.append("}");
		bld.append("]");

		return bld.toString();
	}


	private String writeJsonUpdatesForJob(JobID jobId) {
		final Future<Object> responseArchivedJobs = jobmanager.ask(
				JobManagerMessages.getRequestRunningJobs(),
				timeout);

		Object resultArchivedJobs;
		try{
			resultArchivedJobs = Await.result(responseArchivedJobs, timeout);
		}
		catch (Exception ex) {
			throw new RuntimeException("Could not retrieve archived jobs from the job manager.", ex);
		}

		if(!(resultArchivedJobs instanceof JobManagerMessages.RunningJobs)){
			throw new RuntimeException("RequestArchivedJobs requires a response of type " +
					"RunningJobs. Instead the response is of type " +
					resultArchivedJobs.getClass() + ".");
		}
		else {
			final Iterable<ExecutionGraph> graphs = ((JobManagerMessages.RunningJobs)resultArchivedJobs).
					asJavaIterable();

			//Serialize job to json
			final StringBuilder bld = new StringBuilder();

			bld.append("{");
			bld.append("\"jobid\": \"").append(jobId).append("\",");
			bld.append("\"timestamp\": \"").append(System.currentTimeMillis()).append("\",");
			bld.append("\"recentjobs\": [");

			boolean first = true;

			for (ExecutionGraph g : graphs){
				if (first) {
					first = false;
				} else {
					bld.append(",");
				}

				bld.append("\"").append(g.getJobID()).append("\"");
			}
			bld.append("],");

			final Future<Object> responseJob = jobmanager.ask(
					new JobManagerMessages.RequestJob(jobId),
					timeout);

			Object resultJob;
			try{
				resultJob = Await.result(responseJob, timeout);
			}
			catch (Exception ex){
				throw new RuntimeException("Could not retrieve the job with jobID " + jobId +
						"from the job manager.", ex);
			}

			if (!(resultJob instanceof JobManagerMessages.JobResponse)) {
				throw new RuntimeException("RequestJob requires a response of type JobResponse. " +
						"Instead the response is of type " + resultJob.getClass() + ".");
			}
			else {
				final JobManagerMessages.JobResponse response = (JobManagerMessages.JobResponse) resultJob;

				if (response instanceof JobManagerMessages.JobFound){
					ExecutionGraph graph = ((JobManagerMessages.JobFound)response).executionGraph();

					bld.append("\"vertexevents\": [");

					first = true;
					for (ExecutionVertex ev : graph.getAllExecutionVertices()) {
						if (first) {
							first = false;
						} else {
							bld.append(",");
						}

						bld.append("{");
						bld.append("\"vertexid\": \"").append(ev.getCurrentExecutionAttempt().getAttemptId()).append("\",");
						bld.append("\"newstate\": \"").append(ev.getExecutionState()).append("\",");
						bld.append("\"timestamp\": \"").append(ev.getStateTimestamp(ev.getExecutionState())).append("\"");
						bld.append("}");
					}

					bld.append("],");

					bld.append("\"jobevents\": [");

					bld.append("{");
					bld.append("\"newstate\": \"").append(graph.getState()).append("\",");
					bld.append("\"timestamp\": \"").append(graph.getStatusTimestamp(graph.getState())).append("\"");
					bld.append("}");

					bld.append("]");

					bld.append("}");
				}
				else {
					bld.append("\"vertexevents\": [],");
					bld.append("\"jobevents\": [");
					bld.append("{");
					bld.append("\"newstate\": \"").append(JobStatus.FINISHED.toString()).append("\",");
					bld.append("\"timestamp\": \"").append(System.currentTimeMillis()).append("\"");
					bld.append("}");
					bld.append("]");
					bld.append("}");
				}
			}

			return bld.toString();
		}
	}

	private String writeJsonForArchivedJobGroupvertex(ExecutionGraph graph, JobVertexID vertexId) {
		ExecutionJobVertex jobVertex = graph.getJobVertex(vertexId);
		StringBuilder bld = new StringBuilder();

		bld.append("{\"groupvertex\": ").append(JsonFactory.toJson(jobVertex)).append(",");

		bld.append("\"verticetimes\": {");
		boolean first = true;
		for (ExecutionJobVertex groupVertex : graph.getAllVertices().values()) {

			for (ExecutionVertex vertex : groupVertex.getTaskVertices()) {

				Execution exec = vertex.getCurrentExecutionAttempt();

				if(first) {
					first = false;
				} else {
					bld.append(","); }

				bld.append("\"").append(exec.getAttemptId()).append("\": {");
				bld.append("\"vertexid\": \"").append(exec.getAttemptId()).append("\",");
				bld.append("\"vertexname\": \"").append(vertex).append("\",");
				bld.append("\"CREATED\": ").append(vertex.getStateTimestamp(ExecutionState.CREATED)).append(",");
				bld.append("\"SCHEDULED\": ").append(vertex.getStateTimestamp(ExecutionState.SCHEDULED)).append(",");
				bld.append("\"DEPLOYING\": ").append(vertex.getStateTimestamp(ExecutionState.DEPLOYING)).append(",");
				bld.append("\"RUNNING\": ").append(vertex.getStateTimestamp(ExecutionState.RUNNING)).append(",");
				bld.append("\"FINISHED\": ").append(vertex.getStateTimestamp(ExecutionState.FINISHED)).append(",");
				bld.append("\"CANCELING\": ").append(vertex.getStateTimestamp(ExecutionState.CANCELING)).append(",");
				bld.append("\"CANCELED\": ").append(vertex.getStateTimestamp(ExecutionState.CANCELED)).append(",");
				bld.append("\"FAILED\": ").append(vertex.getStateTimestamp(ExecutionState.FAILED)).append("");
				bld.append("}");
			}

		}
		bld.append("}}");
		return bld.toString();
	}


	private String writeJsonForVersion() {
		return "{\"version\": \"" + EnvironmentInformation.getVersion() + "\",\"revision\": \"" +
				EnvironmentInformation.getRevisionInformation().commitId + "\"}";
	}
}