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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.ExecutionStateChangeEvent;
import org.apache.flink.runtime.event.job.JobEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
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
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.StringUtils;
import org.eclipse.jetty.io.EofException;


public class JobmanagerInfoServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(JobmanagerInfoServlet.class);
	
	/** Underlying JobManager */
	private final JobManager jobmanager;
	
	
	public JobmanagerInfoServlet(JobManager jobmanager) {
		this.jobmanager = jobmanager;
	}
	
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("application/json");
		
		try {
			if("archive".equals(req.getParameter("get"))) {
				writeJsonForArchive(resp.getWriter(), jobmanager.getOldJobs());
			}
			else if("job".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				writeJsonForArchivedJob(resp.getWriter(), jobmanager.getArchive().getJob(JobID.fromHexString(jobId)));
			}
			else if("groupvertex".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				String groupvertexId = req.getParameter("groupvertex");
				writeJsonForArchivedJobGroupvertex(resp.getWriter(), jobmanager.getArchive().getJob(JobID.fromHexString(jobId)), JobVertexID.fromHexString(groupvertexId));
			}
			else if("taskmanagers".equals(req.getParameter("get"))) {
				resp.getWriter().write("{\"taskmanagers\": " + jobmanager.getNumberOfTaskManagers() +", \"slots\": "+jobmanager.getTotalNumberOfRegisteredSlots()+"}");
			}
			else if("cancel".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				jobmanager.cancelJob(JobID.fromHexString(jobId));
			}
			else if("updates".equals(req.getParameter("get"))) {
				String jobId = req.getParameter("job");
				writeJsonUpdatesForJob(resp.getWriter(), JobID.fromHexString(jobId));
			} else if ("version".equals(req.getParameter("get"))) {
				writeJsonForVersion(resp.getWriter());
			}
			else{
				writeJsonForJobs(resp.getWriter(), jobmanager.getRecentJobs());
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
	 * @param jobs
	 */
	private void writeJsonForJobs(PrintWriter wrt, List<RecentJobEvent> jobs) {
		
		try {
		
			wrt.write("[");
			
			// Loop Jobs
			for (int i = 0; i < jobs.size(); i++) {
				RecentJobEvent jobEvent = jobs.get(i);
	
				writeJsonForJob(wrt, jobEvent);
	
				//Write seperator between json objects
				if(i != jobs.size() - 1) {
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
	
	private void writeJsonForJob(PrintWriter wrt, RecentJobEvent jobEvent) throws IOException {
		
		ExecutionGraph graph = jobmanager.getRecentExecutionGraph(jobEvent.getJobID());
		
		//Serialize job to json
		wrt.write("{");
		wrt.write("\"jobid\": \"" + jobEvent.getJobID() + "\",");
		wrt.write("\"jobname\": \"" + jobEvent.getJobName()+"\",");
		wrt.write("\"status\": \""+ jobEvent.getJobStatus() + "\",");
		wrt.write("\"time\": " + jobEvent.getTimestamp()+",");
		
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
	 * @param jobs
	 */
	private void writeJsonForArchive(PrintWriter wrt, List<RecentJobEvent> jobs) {
		
		wrt.write("[");
		
		// sort jobs by time
		Collections.sort(jobs,  new Comparator<RecentJobEvent>() {
			@Override
			public int compare(RecentJobEvent o1, RecentJobEvent o2) {
				if(o1.getTimestamp() < o2.getTimestamp()) {
					return 1;
				} else {
					return -1;
				}
			}
			
		});
		
		// Loop Jobs
		for (int i = 0; i < jobs.size(); i++) {
			RecentJobEvent jobEvent = jobs.get(i);
			
			//Serialize job to json
			wrt.write("{");
			wrt.write("\"jobid\": \"" + jobEvent.getJobID() + "\",");
			wrt.write("\"jobname\": \"" + jobEvent.getJobName()+"\",");
			wrt.write("\"status\": \""+ jobEvent.getJobStatus() + "\",");
			wrt.write("\"time\": " + jobEvent.getTimestamp());
			
			wrt.write("}");
			
			//Write seperator between json objects
			if(i != jobs.size() - 1) {
				wrt.write(",");
			}
		}
		wrt.write("]");
		
	}
	
	/**
	 * Writes infos about archived job in Json format, including groupvertices and groupverticetimes
	 * 
	 * @param wrt
	 * @param jobEvent
	 */
	private void writeJsonForArchivedJob(PrintWriter wrt, RecentJobEvent jobEvent) {
		
		try {
		
			wrt.write("[");
		
			ExecutionGraph graph = jobmanager.getRecentExecutionGraph(jobEvent.getJobID());
			
			//Serialize job to json
			wrt.write("{");
			wrt.write("\"jobid\": \"" + jobEvent.getJobID() + "\",");
			wrt.write("\"jobname\": \"" + jobEvent.getJobName()+"\",");
			wrt.write("\"status\": \""+ jobEvent.getJobStatus() + "\",");
			wrt.write("\"SCHEDULED\": "+ graph.getStatusTimestamp(JobStatus.CREATED) + ",");
			wrt.write("\"RUNNING\": "+ graph.getStatusTimestamp(JobStatus.RUNNING) + ",");
			wrt.write("\"FINISHED\": "+ graph.getStatusTimestamp(JobStatus.FINISHED) + ",");
			wrt.write("\"FAILED\": "+ graph.getStatusTimestamp(JobStatus.FAILED) + ",");
			wrt.write("\"CANCELED\": "+ graph.getStatusTimestamp(JobStatus.CANCELED) + ",");

			if (jobEvent.getJobStatus() == JobStatus.FAILED) {
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
							wrt.write("\"node\": \"" + (slot == null ? "(none)" : slot.getInstance().getInstanceConnectionInfo().getFQDNHostname()) + "\",");
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
			Map<String, Object> accMap = AccumulatorHelper.toResultMap(jobmanager.getAccumulators(jobEvent.getJobID()));
			
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
			
			List<AbstractEvent> events = jobmanager.getEvents(jobId);
			
			//Serialize job to json
			wrt.write("{");
			wrt.write("\"jobid\": \"" + jobId + "\",");
			wrt.write("\"timestamp\": \"" + System.currentTimeMillis() + "\",");
			wrt.write("\"recentjobs\": [");
				
			boolean first = true;
			for(RecentJobEvent rje: jobmanager.getRecentJobs()) {
				if(first) {
					first = false;
				} else {
					wrt.write(","); }
				
				wrt.write("\""+rje.getJobID().toString()+"\""); 
			}
					
			wrt.write("],");
			
			wrt.write("\"vertexevents\": [");
		
			first = true;
			for (AbstractEvent event: events) {
				
				if (event instanceof ExecutionStateChangeEvent) {
					
					if(first) {
						first = false;
					} else {
						wrt.write(","); }
					
					ExecutionStateChangeEvent vertexevent = (ExecutionStateChangeEvent) event;
					wrt.write("{");
					wrt.write("\"vertexid\": \"" + vertexevent.getExecutionAttemptID() + "\",");
					wrt.write("\"newstate\": \"" + vertexevent.getNewExecutionState() + "\",");
					wrt.write("\"timestamp\": \"" + vertexevent.getTimestamp() + "\"");
					wrt.write("}");
				}
			}
			
			wrt.write("],");
			
			wrt.write("\"jobevents\": [");
			
			first = true;
			for(AbstractEvent event: events) {
				
				if( event instanceof JobEvent) {
					
					if(first) {
						first = false;
					} else {
						wrt.write(","); }
					
					JobEvent jobevent = (JobEvent) event;
					wrt.write("{");
					wrt.write("\"newstate\": \"" + jobevent.getCurrentJobStatus() + "\",");
					wrt.write("\"timestamp\": \"" + jobevent.getTimestamp() + "\"");
					wrt.write("}");
				}
			}
			
			wrt.write("]");
			
			wrt.write("}");
			
		
		} catch (EofException eof) { // Connection closed by client
			LOG.info("Info server for jobmanager: Connection closed by client, EofException");
		} catch (IOException ioe) { // Connection closed by client	
			LOG.info("Info server for jobmanager: Connection closed by client, IOException");
		} 
		
	}
	
	/**
	 * Writes info about one particular archived JobVertex in a job, including all member execution vertices, their times and statuses.
	 */
	private void writeJsonForArchivedJobGroupvertex(PrintWriter wrt, RecentJobEvent jobEvent, JobVertexID vertexId) {
		try {
			ExecutionGraph graph = jobmanager.getRecentExecutionGraph(jobEvent.getJobID());
			
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
		catch (IOException ioe) { // Connection closed by client
			String message = "Info server for jobmanager: Connection closed by client - " + ioe.getClass().getSimpleName();

			if (LOG.isDebugEnabled()) {
				LOG.debug(message, ioe);
			}
			else if (LOG.isInfoEnabled()) {
				LOG.info(message);
			}
		} 
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
