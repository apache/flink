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

package org.apache.flink.runtime.jobmanager.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akka.actor.ActorRef;

import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.messages.ArchiveMessages.ArchivedJobs;
import org.apache.flink.runtime.messages.ArchiveMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.RunningJobs;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestJob;
import org.apache.flink.runtime.messages.JobManagerMessages.JobResponse;
import org.apache.flink.runtime.messages.JobManagerMessages.JobFound;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultStringsFound;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsErroneous;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsNotFound;
import org.apache.flink.runtime.messages.accumulators.RequestAccumulatorResultsStringified;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.StringUtils;
import org.eclipse.jetty.io.EofException;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

public class JobManagerInfoServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerInfoServlet.class);

	/** Underlying JobManager */
	private final ActorRef jobmanager;
	private final ActorRef archive;
	private final FiniteDuration timeout;


	public JobManagerInfoServlet(ActorRef jobmanager, ActorRef archive, FiniteDuration timeout) {
		this.jobmanager = jobmanager;
		this.archive = archive;
		this.timeout = timeout;
	}


	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
			IOException {

		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("application/json");

		Future<Object> response;
		Object result;

		try {
			if("archive".equals(req.getParameter("get"))) {
				response = Patterns.ask(archive, ArchiveMessages.getRequestArchivedJobs(),
						new Timeout(timeout));

				result = Await.result(response, timeout);

				if(!(result instanceof ArchivedJobs)) {
					throw new RuntimeException("RequestArchiveJobs requires a response of type " +
							"ArchivedJobs. Instead the response is of type " + result.getClass() +
							".");
				} else {
					final List<ExecutionGraph> archivedJobs = new ArrayList<ExecutionGraph>(
							((ArchivedJobs) result).asJavaCollection());

					writeJsonForArchive(resp.getWriter(), archivedJobs);
				}
			}
			else if("job".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");

				response = Patterns.ask(archive, new RequestJob(JobID.fromHexString(jobId)),
						new Timeout(timeout));

				result = Await.result(response, timeout);

				if(!(result instanceof JobResponse)){
					throw new RuntimeException("RequestJob requires a response of type JobResponse. " +
							"Instead the response is of type " + result.getClass());
				}else {
					final JobResponse jobResponse = (JobResponse) result;

					if(jobResponse instanceof JobFound){
						ExecutionGraph archivedJob = ((JobFound)result).executionGraph();
						writeJsonForArchivedJob(resp.getWriter(), archivedJob);
					} else {
						LOG.warn("DoGet:job: Could not find job for job ID " + jobId);
					}
				}
			}
			else if("groupvertex".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				String groupvertexId = req.getParameter("groupvertex");

				response = Patterns.ask(archive, new RequestJob(JobID.fromHexString(jobId)),
						new Timeout(timeout));

				result = Await.result(response, timeout);

				if(!(result instanceof JobResponse)){
					throw new RuntimeException("RequestJob requires a response of type JobResponse. " +
							"Instead the response is of type " + result.getClass());
				}else {
					final JobResponse jobResponse = (JobResponse) result;

					if(jobResponse instanceof JobFound && groupvertexId != null){
						ExecutionGraph archivedJob = ((JobFound)jobResponse).executionGraph();

						writeJsonForArchivedJobGroupvertex(resp.getWriter(), archivedJob,
								JobVertexID.fromHexString(groupvertexId));
					} else {
						LOG.warn("DoGet:groupvertex: Could not find job for job ID " + jobId);
					}
				}
			}
			else if("taskmanagers".equals(req.getParameter("get"))) {

				response = Patterns.ask(jobmanager,
						JobManagerMessages.getRequestNumberRegisteredTaskManager(),
						new Timeout(timeout));

				result = Await.result(response, timeout);

				if(!(result instanceof Integer)) {
					throw new RuntimeException("RequestNumberRegisteredTaskManager requires a " +
							"response of type Integer. Instead the response is of type " +
							result.getClass() + ".");
				} else {
					final int numberOfTaskManagers = (Integer)result;

					final Future<Object> responseRegisteredSlots = Patterns.ask(jobmanager,
							JobManagerMessages.getRequestTotalNumberOfSlots(),
							new Timeout(timeout));

					final Object resultRegisteredSlots = Await.result(responseRegisteredSlots,
							timeout);

					if(!(resultRegisteredSlots instanceof Integer)) {
						throw new RuntimeException("RequestTotalNumberOfSlots requires a response of " +
								"type Integer. Instaed the response of type " +
								resultRegisteredSlots.getClass() + ".");
					} else {
						final int numberOfRegisteredSlots = (Integer) resultRegisteredSlots;

						resp.getWriter().write("{\"taskmanagers\": " + numberOfTaskManagers +", " +
								"\"slots\": "+numberOfRegisteredSlots+"}");
					}
				}
			}
			else if("cancel".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");

				response = Patterns.ask(jobmanager, new CancelJob(JobID.fromHexString(jobId)),
						new Timeout(timeout));

				Await.ready(response, timeout);
			}
			else if("updates".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				writeJsonUpdatesForJob(resp.getWriter(), JobID.fromHexString(jobId));
			} else if ("version".equals(req.getParameter("get"))) {
				writeJsonForVersion(resp.getWriter());
			}
			else{
				response = Patterns.ask(jobmanager, JobManagerMessages.getRequestRunningJobs(),
						new Timeout(timeout));

				result = Await.result(response, timeout);

				if(!(result instanceof RunningJobs)){
					throw new RuntimeException("RequestRunningJobs requires a response of type " +
							"RunningJobs. Instead the response of type " + result.getClass() + ".");
				} else {
					final Iterable<ExecutionGraph> runningJobs =
							((RunningJobs) result).asJavaIterable();

					writeJsonForJobs(resp.getWriter(), runningJobs);
				}
			}

		} catch (Exception e) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.getWriter().print(e.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(e));
			}
		}
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
			while(it.hasNext()){
				ExecutionGraph graph = it.next();

				writeJsonForJob(wrt, graph);

				//Write seperator between json objects
				if(it.hasNext()) {
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
		wrt.write("\"jobname\": \"" + graph.getJobName()+"\",");
		wrt.write("\"status\": \""+ graph.getState() + "\",");
		wrt.write("\"time\": " + graph.getStatusTimestamp(graph.getState())+",");

		// Serialize ManagementGraph to json
		wrt.write("\"groupvertices\": [");
		boolean first = true;

		for (ExecutionJobVertex groupVertex : graph.getVerticesTopologically()) {
			//Write seperator between json objects
			if(first) {
				first = false;
			} else {
				wrt.write(","); }

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
		Collections.sort(graphs,  new Comparator<ExecutionGraph>() {
			@Override
			public int compare(ExecutionGraph o1, ExecutionGraph o2) {
				if(o1.getStatusTimestamp(o1.getState()) < o2.getStatusTimestamp(o2.getState())) {
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
			wrt.write("\"jobname\": \"" + graph.getJobName()+"\",");
			wrt.write("\"status\": \""+ graph.getState() + "\",");
			wrt.write("\"time\": " + graph.getStatusTimestamp(graph.getState()));

			wrt.write("}");

			//Write seperator between json objects
			if(i != graphs.size() - 1) {
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
	 */
	private void writeJsonForArchivedJob(PrintWriter wrt, ExecutionGraph graph) {
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
						InstanceConnectionInfo location = vertex.getCurrentAssignedResourceLocation();
						Throwable failureCause = vertex.getFailureCause();
						if (location != null || failureCause != null) {
							if (first) {
								first = false;
							} else {
								wrt.write(",");
							}
							wrt.write("{");
							wrt.write("\"node\": \"" + (location == null ? "(none)" : location.getFQDNHostname()) + "\",");
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

			// write user config
			ExecutionConfig ec = graph.getExecutionConfig();
			if(ec != null) {
				wrt.write("\"executionConfig\": {");
				wrt.write("\"Execution Mode\": \""+ec.getExecutionMode()+"\",");
				wrt.write("\"Number of execution retries\": \""+ec.getNumberOfExecutionRetries()+"\",");
				wrt.write("\"Job parallelism\": \""+ec.getParallelism()+"\",");
				wrt.write("\"Object reuse mode\": \""+ec.isObjectReuseEnabled()+"\"");
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
						wrt.write(", \"userConfig\": " + ucString + "}");
					}
					else {
						LOG.info("GlobalJobParameters.toMap() did not return anything");
					}
				}
				else {
					LOG.info("No GlobalJobParameters were set in the execution config");
				}
				wrt.write("},");
			} else {
				LOG.warn("Unable to retrieve execution config from execution graph");
			}

			// write accumulators
			final Future<Object> response = Patterns.ask(jobmanager,
					new RequestAccumulatorResultsStringified(graph.getJobID()), new Timeout(timeout));

			Object result;
			try {
				result = Await.result(response, timeout);
			} catch (Exception ex) {
				throw new IOException("Could not retrieve the accumulator results from the job manager.", ex);
			}

			if (result instanceof AccumulatorResultStringsFound) {
				StringifiedAccumulatorResult[] accumulators = ((AccumulatorResultStringsFound) result).result();

				wrt.write("\n\"accumulators\": [");
				int i = 0;
				for (StringifiedAccumulatorResult accumulator : accumulators) {
					wrt.write("{ \"name\": \"" + accumulator.getName() + " (" + accumulator.getType() + ")\","
							+ " \"value\": \"" + accumulator.getValue() + "\"}\n");
					if (++i < accumulators.length) {
						wrt.write(",");
					}
				}
				wrt.write("],\n");
			}
			else if (result instanceof AccumulatorResultsNotFound) {
				wrt.write("\n\"accumulators\": [],");
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

			wrt.write("}");
			wrt.write("}");
			wrt.write("]");
		}
		catch (Exception ex) { // Connection closed by client
			LOG.error("Info server for JobManager: Failed to write json for archived jobs", ex);
		}
	}

	/**
	 * Writes all updates (events) for a given job since a given time
	 *
	 * @param wrt
	 * @param jobId
	 */
	private void writeJsonUpdatesForJob(PrintWriter wrt, JobID jobId) {

		try {
			final Future<Object> responseArchivedJobs = Patterns.ask(jobmanager,
					JobManagerMessages.getRequestRunningJobs(),
					new Timeout(timeout));

			Object resultArchivedJobs = null;

			try{
				resultArchivedJobs = Await.result(responseArchivedJobs, timeout);
			} catch (Exception ex) {
				throw new IOException("Could not retrieve archived jobs from the job manager.", ex);
			}

			if(!(resultArchivedJobs instanceof RunningJobs)){
				throw new RuntimeException("RequestArchivedJobs requires a response of type " +
						"RunningJobs. Instead the response is of type " +
						resultArchivedJobs.getClass() + ".");
			} else {
				final Iterable<ExecutionGraph> graphs = ((RunningJobs)resultArchivedJobs).
						asJavaIterable();

				//Serialize job to json
				wrt.write("{");
				wrt.write("\"jobid\": \"" + jobId + "\",");
				wrt.write("\"timestamp\": \"" + System.currentTimeMillis() + "\",");
				wrt.write("\"recentjobs\": [");

				boolean first = true;

				for(ExecutionGraph g : graphs){
					if (first) {
						first = false;
					} else {
						wrt.write(",");
					}

					wrt.write("\"" + g.getJobID() + "\"");
				}

				wrt.write("],");

				final Future<Object> responseJob = Patterns.ask(jobmanager, new RequestJob(jobId),
						new Timeout(timeout));

				Object resultJob = null;

				try{
					resultJob = Await.result(responseJob, timeout);
				} catch (Exception ex){
					throw new IOException("Could not retrieve the job with jobID " + jobId +
							"from the job manager.", ex);
				}

				if(!(resultJob instanceof JobResponse)) {
					throw new RuntimeException("RequestJob requires a response of type JobResponse. " +
							"Instead the response is of type " + resultJob.getClass() + ".");
				} else {
					final JobResponse response = (JobResponse) resultJob;

					if(response instanceof JobFound){
						ExecutionGraph graph = ((JobFound)response).executionGraph();

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
				}
			}

		} catch (Exception exception) { // Connection closed by client
			LOG.info("Info server for jobmanager: Failed to write json updates for job {}, " +
					"because {}.", jobId, StringUtils.stringifyException(exception));
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

				if(first) {
					first = false;
				} else {
					wrt.write(","); }

				wrt.write("\""+exec.getAttemptId() +"\": {");
				wrt.write("\"vertexid\": \"" + exec.getAttemptId() + "\",");
				wrt.write("\"vertexname\": \"" + vertex + "\",");
				wrt.write("\"CREATED\": "+ vertex.getStateTimestamp(ExecutionState.CREATED) + ",");
				wrt.write("\"SCHEDULED\": "+ vertex.getStateTimestamp(ExecutionState.SCHEDULED) + ",");
				wrt.write("\"DEPLOYING\": "+ vertex.getStateTimestamp(ExecutionState.DEPLOYING) + ",");
				wrt.write("\"RUNNING\": "+ vertex.getStateTimestamp(ExecutionState.RUNNING) + ",");
				wrt.write("\"FINISHED\": "+ vertex.getStateTimestamp(ExecutionState.FINISHED) + ",");
				wrt.write("\"CANCELING\": "+ vertex.getStateTimestamp(ExecutionState.CANCELING) + ",");
				wrt.write("\"CANCELED\": "+ vertex.getStateTimestamp(ExecutionState.CANCELED) + ",");
				wrt.write("\"FAILED\": "+ vertex.getStateTimestamp(ExecutionState.FAILED) + "");
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
