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

package org.apache.flink.runtime.jobmanager.rest;

import akka.actor.ActorRef;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.web.JsonFactory;
import org.apache.flink.runtime.messages.ArchiveMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.StringUtils;
import org.eclipse.jetty.io.EofException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Path("/")
/**
 * Service that acts as a REST interface for the Web server; Information is provided in the form of JSON Responses
 */
public class JobManagerInfoService {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerInfoService.class);

	@GET
	@Produces("application/json")
	public String getJob(HttpServletRequest request, @QueryParam("get") String jobManagerQuery, @QueryParam("job") String jobId, @QueryParam("groupvertex") String groupVertexId) throws IOException {
		/** Underlying JobManager */
		JobManager jobMgr = new JobManager(new Configuration());

		ActorRef jobManager = jobMgr.self();
		ActorRef archive = jobMgr.archive();
		FiniteDuration timeout = jobMgr.timeout();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintWriter writer = new PrintWriter(baos);

		if ("archive".equals(jobManagerQuery)) {
			List<ExecutionGraph> archivedJobs = new ArrayList<ExecutionGraph>(AkkaUtils
					.<ArchiveMessages.ArchivedJobs>ask(archive, ArchiveMessages.RequestArchivedJobs$.MODULE$, timeout)
					.asJavaCollection());

			writeJsonForArchive(writer, archivedJobs);
		} else if ("job".equals(jobManagerQuery)) {
			JobManagerMessages.JobResponse response = AkkaUtils.ask(archive,
					new JobManagerMessages.RequestJob(JobID.fromHexString(jobId)), timeout);

			if (response instanceof JobManagerMessages.JobFound) {
				ExecutionGraph archivedJob = ((JobManagerMessages.JobFound) response).executionGraph();
				writeJsonForArchivedJob(writer, archivedJob, jobManager, timeout);
			} else {
				LOG.warn("Getting job: Could not find job for job ID " + jobId);
			}

		} else if ("groupvertex".equals(jobManagerQuery)) {
			JobManagerMessages.JobResponse response = AkkaUtils.ask(archive,
					new JobManagerMessages.RequestJob(JobID.fromHexString(jobId)), timeout);
			if (response instanceof JobManagerMessages.JobFound && groupVertexId != null) {
				ExecutionGraph archivedJob = ((JobManagerMessages.JobFound) response).executionGraph();

				writeJsonForArchivedJobGroupvertex(writer, archivedJob,
						JobVertexID.fromHexString(groupVertexId));
			} else {
				LOG.warn("DoGet:groupvertex: Could not find job for job ID " + jobId);
			}
		} else if ("taskmanagers".equals(jobManagerQuery)) {
			int numberOfTaskManagers = AkkaUtils.<Integer>ask(jobManager,
					JobManagerMessages.RequestNumberRegisteredTaskManager$.MODULE$, timeout);
			int numberOfRegisteredSlots = AkkaUtils.<Integer>ask(jobManager,
					JobManagerMessages.RequestTotalNumberOfSlots$.MODULE$, timeout);

			writer.write("{\"taskmanagers\": " + numberOfTaskManagers + ", " +
					"\"slots\": " + numberOfRegisteredSlots + "}");

		} else if ("cancel".equals(jobManagerQuery)) {
			AkkaUtils.<JobManagerMessages.CancellationResponse>ask(jobManager,
					new JobManagerMessages.CancelJob(JobID.fromHexString(jobId)), timeout);

		} else if ("updates".equals(jobManagerQuery)) {
			writeJsonUpdatesForJob(writer, JobID.fromHexString(jobId), jobManager, timeout);
		} else if ("version".equals(jobManagerQuery)) {
			writeJsonForVersion(writer);
		} else {
			Iterable<ExecutionGraph> runningJobs = AkkaUtils.<JobManagerMessages.RunningJobs>ask
					(jobManager, JobManagerMessages.RequestRunningJobs$.MODULE$, timeout).asJavaIterable();
			writeJsonForJobs(writer, runningJobs);
		}

		return new String(baos.toByteArray());
	}

	/**
	 * Writes ManagementGraph as Json for all recent jobs
	 *
	 * @param wrt
	 * @param graphs
	 */
	private void writeJsonForJobs(PrintWriter wrt, Iterable<ExecutionGraph> graphs) {
		try {
			wrt.write("[");

			Iterator<ExecutionGraph> it = graphs.iterator();
			// Loop Jobs
			while (it.hasNext()) {
				ExecutionGraph graph = it.next();
				writeJsonForJob(wrt, graph);

				//Write seperator between json objects
				if (it.hasNext()) {
					wrt.write(",");
				}
			}
			wrt.write("]");

		} catch (EofException eof) { // Connection closed by client
			LOG.info("Info server for jobmanager: Connection closed by client, EofException");
		} catch (IOException ioe) { // Connection closed by client
			LOG.info("Info server for jobmanager: Connection closed by client, IOException");
		}
	}

	private void writeJsonForJob(PrintWriter wrt, ExecutionGraph graph) throws IOException {
		//Serialize job to json
		wrt.write("{");
		wrt.write("\"jobid\": \"" + graph.getJobID() + "\",");
		wrt.write("\"jobname\": \"" + graph.getJobName() + "\",");
		wrt.write("\"status\": \"" + graph.getState() + "\",");
		wrt.write("\"time\": " + graph.getStatusTimestamp(graph.getState()) + ",");

		// Serialize ManagementGraph to json
		wrt.write("\"groupvertices\": [");
		boolean first = true;

		for (ExecutionJobVertex groupVertex : graph.getVerticesTopologically()) {
			//Write seperator between json objects
			if (first) {
				first = false;
			} else {
				wrt.write(",");
			}

			wrt.write(JsonFactory.toJson(groupVertex));
		}
		wrt.write("]");
		wrt.write("}");

	}

	/**
	 * Writes Json with a list of currently archived jobs, sorted by time
	 *
	 * @param wrt
	 * @param graphs
	 */
	private void writeJsonForArchive(PrintWriter wrt, List<ExecutionGraph> graphs) {

		wrt.write("[");

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
			wrt.write("{");
			wrt.write("\"jobid\": \"" + graph.getJobID() + "\",");
			wrt.write("\"jobname\": \"" + graph.getJobName() + "\",");
			wrt.write("\"status\": \"" + graph.getState() + "\",");
			wrt.write("\"time\": " + graph.getStatusTimestamp(graph.getState()));

			wrt.write("}");

			//Write seperator between json objects
			if (i != graphs.size() - 1) {
				wrt.write(",");
			}
		}
		wrt.write("]");

	}

	/**
	 * Writes infos about archived job in Json format, including groupvertices and groupverticetimes
	 *
	 * @param wrt
	 * @param graph
	 * @param jobmanager
	 * @param timeout
	 */
	private void writeJsonForArchivedJob(PrintWriter wrt, ExecutionGraph graph, ActorRef jobmanager, FiniteDuration timeout) {

		try {

			wrt.write("[");

			//Serialize job to json
			wrt.write("{");
			wrt.write("\"jobid\": \"" + graph.getJobID() + "\",");
			wrt.write("\"jobname\": \"" + graph.getJobName() + "\",");
			wrt.write("\"status\": \"" + graph.getState() + "\",");
			wrt.write("\"SCHEDULED\": " + graph.getStatusTimestamp(JobStatus.CREATED) + ",");
			wrt.write("\"RUNNING\": " + graph.getStatusTimestamp(JobStatus.RUNNING) + ",");
			wrt.write("\"FINISHED\": " + graph.getStatusTimestamp(JobStatus.FINISHED) + ",");
			wrt.write("\"FAILED\": " + graph.getStatusTimestamp(JobStatus.FAILED) + ",");
			wrt.write("\"CANCELED\": " + graph.getStatusTimestamp(JobStatus.CANCELED) + ",");

			if (graph.getState() == JobStatus.FAILED) {
				wrt.write("\"failednodes\": [");
				boolean first = true;
				for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
					if (vertex.getExecutionState() == ExecutionState.FAILED) {
						AllocatedSlot slot = vertex.getCurrentAssignedResource();
						Throwable failureCause = vertex.getFailureCause();
						if (slot != null || failureCause != null) {
							if (first) {
								first = false;
							} else {
								wrt.write(",");
							}
							wrt.write("{");
							wrt.write("\"node\": \"" + (slot == null ? "(none)" : slot
									.getInstance().getInstanceConnectionInfo().getFQDNHostname()) + "\",");
							wrt.write("\"message\": \"" + (failureCause == null ? "" : StringUtils.escapeHtml(ExceptionUtils.stringifyException(failureCause))) + "\"");
							wrt.write("}");
						}
					}
				}
				wrt.write("],");
			}

			// Serialize ManagementGraph to json
			wrt.write("\"groupvertices\": [");
			boolean first = true;
			for (ExecutionJobVertex groupVertex : graph.getVerticesTopologically()) {
				//Write seperator between json objects
				if (first) {
					first = false;
				} else {
					wrt.write(",");
				}

				wrt.write(JsonFactory.toJson(groupVertex));

			}
			wrt.write("],");

			// write accumulators
			JobManagerMessages.AccumulatorResultsResponse response = AkkaUtils.ask(jobmanager,
					new JobManagerMessages.RequestAccumulatorResults(graph.getJobID()), timeout);

			if (response instanceof JobManagerMessages.AccumulatorResultsFound) {
				Map<String, Object> accMap = ((JobManagerMessages.AccumulatorResultsFound) response).asJavaMap();

				wrt.write("\n\"accumulators\": [");
				int i = 0;
				for (Map.Entry<String, Object> accumulator : accMap.entrySet()) {
					wrt.write("{ \"name\": \"" + accumulator.getKey() + " (" + accumulator.getValue().getClass().getName() + ")\","
							+ " \"value\": \"" + accumulator.getValue().toString() + "\"}\n");
					if (++i < accMap.size()) {
						wrt.write(",");
					}
				}
				wrt.write("],\n");

				wrt.write("\"groupverticetimes\": {");
				first = true;
				for (ExecutionJobVertex groupVertex : graph.getVerticesTopologically()) {

					if (first) {
						first = false;
					} else {
						wrt.write(",");
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

					wrt.write("\"" + groupVertex.getJobVertexId() + "\": {");
					wrt.write("\"groupvertexid\": \"" + groupVertex.getJobVertexId() + "\",");
					wrt.write("\"groupvertexname\": \"" + groupVertex + "\",");
					wrt.write("\"STARTED\": " + started + ",");
					wrt.write("\"ENDED\": " + ended);
					wrt.write("}");

				}
			} else {
				LOG.warn("Could not find accumulator results for job ID " + graph.getJobID());
			}

			wrt.write("}");

			wrt.write("}");


			wrt.write("]");

		} catch (EofException eof) { // Connection closed by client
			LOG.info("Info server for jobmanager: Connection closed by client, EofException");
		} catch (IOException ioe) { // Connection closed by client
			LOG.info("Info server for jobmanager: Connection closed by client, IOException");
		}

	}


	/**
	 * Writes all updates (events) for a given job since a given time
	 *
	 * @param wrt
	 * @param jobId
	 * @param jobmanager
	 * @param timeout
	 */
	private void writeJsonUpdatesForJob(PrintWriter wrt, JobID jobId, ActorRef jobmanager, FiniteDuration timeout) {

		try {
			Iterable<ExecutionGraph> graphs = AkkaUtils.<JobManagerMessages.RunningJobs>ask(jobmanager,
					JobManagerMessages.RequestRunningJobs$.MODULE$, timeout).asJavaIterable();

			//Serialize job to json
			wrt.write("{");
			wrt.write("\"jobid\": \"" + jobId + "\",");
			wrt.write("\"timestamp\": \"" + System.currentTimeMillis() + "\",");
			wrt.write("\"recentjobs\": [");

			boolean first = true;

			for (ExecutionGraph g : graphs) {
				if (first) {
					first = false;
				} else {
					wrt.write(",");
				}

				wrt.write("\"" + g.getJobID() + "\"");
			}

			wrt.write("],");

			JobManagerMessages.JobResponse response = AkkaUtils.ask(jobmanager, new JobManagerMessages.RequestJob(jobId), timeout);

			if (response instanceof JobManagerMessages.JobFound) {
				ExecutionGraph graph = ((JobManagerMessages.JobFound) response).executionGraph();

				wrt.write("\"vertexevents\": [");

				first = true;
				for (ExecutionVertex ev : graph.getAllExecutionVertices()) {
					if (first) {
						first = false;
					} else {
						wrt.write(",");
					}

					wrt.write("{");
					wrt.write("\"vertexid\": \"" + ev.getCurrentExecutionAttempt().getAttemptId()
							+ "\",");
					wrt.write("\"newstate\": \"" + ev.getExecutionState() + "\",");
					wrt.write("\"timestamp\": \"" + ev.getStateTimestamp(ev.getExecutionState())
							+ "\"");
					wrt.write("}");
				}

				wrt.write("],");

				wrt.write("\"jobevents\": [");

				wrt.write("{");
				wrt.write("\"newstate\": \"" + graph.getState() + "\",");
				wrt.write("\"timestamp\": \"" + graph.getStatusTimestamp(graph.getState()) + "\"");
				wrt.write("}");

				wrt.write("]");

				wrt.write("}");
			} else {
				wrt.write("\"vertexevents\": [],");
				wrt.write("\"jobevents\": [");
				wrt.write("{");
				wrt.write("\"newstate\": \"" + JobStatus.FINISHED + "\",");
				wrt.write("\"timestamp\": \"" + System.currentTimeMillis() + "\"");
				wrt.write("}");
				wrt.write("]");
				wrt.write("}");
				LOG.warn("WriteJsonUpdatesForJob: Could not find job with job ID " + jobId);
			}
		} catch (EofException eof) { // Connection closed by client
			LOG.info("Info server for jobmanager: Connection closed by client, EofException");
		} catch (IOException ioe) { // Connection closed by client
			LOG.info("Info server for jobmanager: Connection closed by client, IOException");
		}

	}

	/**
	 * Writes info about one particular archived JobVertex in a job, including all member execution vertices, their times and statuses.
	 */
	private void writeJsonForArchivedJobGroupvertex(PrintWriter wrt, ExecutionGraph graph,
													JobVertexID vertexId) {
		ExecutionJobVertex jobVertex = graph.getJobVertex(vertexId);

		// Serialize ManagementGraph to json
		wrt.write("{\"groupvertex\": " + JsonFactory.toJson(jobVertex) + ",");

		wrt.write("\"verticetimes\": {");
		boolean first = true;
		for (ExecutionJobVertex groupVertex : graph.getAllVertices().values()) {

			for (ExecutionVertex vertex : groupVertex.getTaskVertices()) {

				Execution exec = vertex.getCurrentExecutionAttempt();

				if (first) {
					first = false;
				} else {
					wrt.write(",");
				}

				wrt.write("\"" + exec.getAttemptId() + "\": {");
				wrt.write("\"vertexid\": \"" + exec.getAttemptId() + "\",");
				wrt.write("\"vertexname\": \"" + vertex + "\",");
				wrt.write("\"CREATED\": " + vertex.getStateTimestamp(ExecutionState.CREATED) + ",");
				wrt.write("\"SCHEDULED\": " + vertex.getStateTimestamp(ExecutionState.SCHEDULED) + ",");
				wrt.write("\"DEPLOYING\": " + vertex.getStateTimestamp(ExecutionState.DEPLOYING) + ",");
				wrt.write("\"RUNNING\": " + vertex.getStateTimestamp(ExecutionState.RUNNING) + ",");
				wrt.write("\"FINISHED\": " + vertex.getStateTimestamp(ExecutionState.FINISHED) + ",");
				wrt.write("\"CANCELING\": " + vertex.getStateTimestamp(ExecutionState.CANCELING) + ",");
				wrt.write("\"CANCELED\": " + vertex.getStateTimestamp(ExecutionState.CANCELED) + ",");
				wrt.write("\"FAILED\": " + vertex.getStateTimestamp(ExecutionState.FAILED) + "");
				wrt.write("}");
			}

		}
		wrt.write("}}");
	}

	/**
	 * Writes the version and the revision of Flink.
	 *
	 * @param wrt
	 */
	private void writeJsonForVersion(PrintWriter wrt) {
		wrt.write("{");
		wrt.write("\"version\": \"" + EnvironmentInformation.getVersion() + "\",");
		wrt.write("\"revision\": \"" + EnvironmentInformation.getRevisionInformation().commitId + "\"");
		wrt.write("}");
	}

}
