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
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akka.actor.ActorRef;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.messages.ArchiveMessages.ArchivedJobs;
import org.apache.flink.runtime.messages.ArchiveMessages.RequestArchivedJobs$;
import org.apache.flink.runtime.messages.JobManagerMessages.AccumulatorResultsResponse;
import org.apache.flink.runtime.messages.JobManagerMessages.AccumulatorResultsFound;
import org.apache.flink.runtime.messages.JobManagerMessages.RunningJobs;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestRunningJobs$;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestTotalNumberOfSlots$;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestNumberRegisteredTaskManager$;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationResponse;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestAccumulatorResults;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestJob;
import org.apache.flink.runtime.messages.JobManagerMessages.JobResponse;
import org.apache.flink.runtime.messages.JobManagerMessages.JobFound;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.StringUtils;
import org.eclipse.jetty.io.EofException;
import scala.concurrent.duration.FiniteDuration;


public class JobmanagerInfoServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(JobmanagerInfoServlet.class);
	
	/** Underlying JobManager */
	private final ActorRef jobmanager;
	private final ActorRef archive;
	private final FiniteDuration timeout;
	
	
	public JobmanagerInfoServlet(ActorRef jobmanager, ActorRef archive, FiniteDuration timeout) {
		this.jobmanager = jobmanager;
		this.archive = archive;
		this.timeout = timeout;
	}
	
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("application/json");
		
		try {
			if("archive".equals(req.getParameter("get"))) {
				List<ExecutionGraph> archivedJobs = new ArrayList<ExecutionGraph>(AkkaUtils
						.<ArchivedJobs>ask(archive,RequestArchivedJobs$.MODULE$, timeout)
						.asJavaCollection());

				writeJsonForArchive(resp.getWriter(), archivedJobs);
			}
			else if("job".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				JobResponse response = AkkaUtils.ask(archive,
						new RequestJob(JobID.fromHexString(jobId)), timeout);

				if(response instanceof JobFound){
					ExecutionGraph archivedJob = ((JobFound)response).executionGraph();
					writeJsonForArchivedJob(resp.getWriter(), archivedJob);
				}else{
					LOG.warn("DoGet:job: Could not find job for job ID " + jobId);
				}
			}
			else if("groupvertex".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				String groupvertexId = req.getParameter("groupvertex");

				JobResponse response = AkkaUtils.ask(archive,
						new RequestJob(JobID.fromHexString(jobId)), timeout);

				if(response instanceof JobFound && groupvertexId != null){
					ExecutionGraph archivedJob = ((JobFound)response).executionGraph();

					writeJsonForArchivedJobGroupvertex(resp.getWriter(), archivedJob,
							JobVertexID.fromHexString(groupvertexId));
				}else{
					LOG.warn("DoGet:groupvertex: Could not find job for job ID " + jobId);
				}
			}
			else if("taskmanagers".equals(req.getParameter("get"))) {
				int numberOfTaskManagers = AkkaUtils.<Integer>ask(jobmanager,
						RequestNumberRegisteredTaskManager$.MODULE$, timeout);
				int numberOfRegisteredSlots = AkkaUtils.<Integer>ask(jobmanager,
						RequestTotalNumberOfSlots$.MODULE$, timeout);

				resp.getWriter().write("{\"taskmanagers\": " + numberOfTaskManagers +", " +
						"\"slots\": "+numberOfRegisteredSlots+"}");
			}
			else if("cancel".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				AkkaUtils.<CancellationResponse>ask(jobmanager,
						new CancelJob(JobID.fromHexString(jobId)), timeout);
			}
			else if("updates".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				writeJsonUpdatesForJob(resp.getWriter(), JobID.fromHexString(jobId));
			} else if ("version".equals(req.getParameter("get"))) {
				writeJsonForVersion(resp.getWriter());
			}
			else{
				Iterable<ExecutionGraph> runningJobs = AkkaUtils.<RunningJobs>ask
						(jobmanager, RequestRunningJobs$.MODULE$, timeout).asJavaIterable();
				writeJsonForJobs(resp.getWriter(), runningJobs);
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
			wrt.write("\"jobname\": \"" + graph.getJobName()+"\",");
			wrt.write("\"status\": \""+ graph.getState() + "\",");
			wrt.write("\"SCHEDULED\": "+ graph.getStatusTimestamp(JobStatus.CREATED) + ",");
			wrt.write("\"RUNNING\": "+ graph.getStatusTimestamp(JobStatus.RUNNING) + ",");
			wrt.write("\"FINISHED\": "+ graph.getStatusTimestamp(JobStatus.FINISHED) + ",");
			wrt.write("\"FAILED\": "+ graph.getStatusTimestamp(JobStatus.FAILED) + ",");
			wrt.write("\"CANCELED\": "+ graph.getStatusTimestamp(JobStatus.CANCELED) + ",");

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
				if(first) {
					first = false;
				} else {
					wrt.write(","); }
				
				wrt.write(JsonFactory.toJson(groupVertex));
				
			}
			wrt.write("],");
			
			// write accumulators
			AccumulatorResultsResponse response = AkkaUtils.ask(jobmanager,
					new RequestAccumulatorResults(graph.getJobID()), timeout);

			if(response instanceof AccumulatorResultsFound){
				Map<String, Object> accMap = ((AccumulatorResultsFound)response).asJavaMap();

				wrt.write("\n\"accumulators\": [");
				int i = 0;
				for( Entry<String, Object> accumulator : accMap.entrySet()) {
					wrt.write("{ \"name\": \""+accumulator.getKey()+" (" + accumulator.getValue().getClass().getName()+")\","
							+ " \"value\": \""+accumulator.getValue().toString()+"\"}\n");
					if(++i < accMap.size()) {
						wrt.write(",");
					}
				}
				wrt.write("],\n");

				wrt.write("\"groupverticetimes\": {");
				first = true;
				for (ExecutionJobVertex groupVertex : graph.getVerticesTopologically()) {

					if(first) {
						first = false;
					} else {
						wrt.write(","); }

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

						if(finished != 0 && finished > ended) {
							ended = finished;
						}

						if(canceled != 0 && canceled > ended) {
							ended = canceled;
						}

						if(failed != 0 && failed > ended) {
							ended = failed;
						}

					}

					wrt.write("\""+groupVertex.getJobVertexId()+"\": {");
					wrt.write("\"groupvertexid\": \"" + groupVertex.getJobVertexId() + "\",");
					wrt.write("\"groupvertexname\": \"" + groupVertex + "\",");
					wrt.write("\"STARTED\": "+ started + ",");
					wrt.write("\"ENDED\": "+ ended);
					wrt.write("}");

				}
			}else{
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
	 */
	private void writeJsonUpdatesForJob(PrintWriter wrt, JobID jobId) {
		
		try {
			Iterable<ExecutionGraph> graphs = AkkaUtils.<RunningJobs>ask(jobmanager,
					RequestRunningJobs$.MODULE$, timeout).asJavaIterable();
			
			//Serialize job to json
			wrt.write("{");
			wrt.write("\"jobid\": \"" + jobId + "\",");
			wrt.write("\"timestamp\": \"" + System.currentTimeMillis() + "\",");
			wrt.write("\"recentjobs\": [");

			boolean first = true;

			for(ExecutionGraph g : graphs){
				if(first){
					first = false;
				}else{
					wrt.write(",");
				}

				wrt.write("\"" + g.getJobID() + "\"");
			}

			wrt.write("],");

			JobResponse response = AkkaUtils.ask(jobmanager, new RequestJob(jobId), timeout);

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
			}else{
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
