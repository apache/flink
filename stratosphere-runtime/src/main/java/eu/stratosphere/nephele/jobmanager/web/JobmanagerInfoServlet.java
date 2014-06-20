/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.io.EofException;

import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.ExecutionStateChangeEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.JobManagerUtils;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertexID;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.services.accumulators.AccumulatorEvent;
import eu.stratosphere.util.StringUtils;


public class JobmanagerInfoServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(JobmanagerInfoServlet.class);
	
	/**
	 * Underlying JobManager
	 */
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
				writeJsonForArchivedJobGroupvertex(resp.getWriter(), jobmanager.getArchive().getJob(JobID.fromHexString(jobId)), ManagementGroupVertexID.fromHexString(groupvertexId));
			}
			else if("taskmanagers".equals(req.getParameter("get"))) {
				resp.getWriter().write("{\"taskmanagers\": " + jobmanager.getNumberOfTaskTrackers() +"}");
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
		
		ManagementGraph jobManagementGraph = jobmanager.getManagementGraph(jobEvent.getJobID());
		
		//Serialize job to json
		wrt.write("{");
		wrt.write("\"jobid\": \"" + jobEvent.getJobID() + "\",");
		wrt.write("\"jobname\": \"" + jobEvent.getJobName()+"\",");
		wrt.write("\"status\": \""+ jobEvent.getJobStatus() + "\",");
		wrt.write("\"time\": " + jobEvent.getTimestamp()+",");
		
		// Serialize ManagementGraph to json
		wrt.write("\"groupvertices\": [");
		boolean first = true;
		for(ManagementGroupVertex groupVertex : jobManagementGraph.getGroupVerticesInTopologicalOrder()) {
			//Write seperator between json objects
			if(first) {
				first = false;
			} else {
				wrt.write(","); }
			
			wrt.write(groupVertex.toJson());
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
		
			ManagementGraph jobManagementGraph = jobmanager.getManagementGraph(jobEvent.getJobID());
			
			//Serialize job to json
			wrt.write("{");
			wrt.write("\"jobid\": \"" + jobEvent.getJobID() + "\",");
			wrt.write("\"jobname\": \"" + jobEvent.getJobName()+"\",");
			wrt.write("\"status\": \""+ jobEvent.getJobStatus() + "\",");
			wrt.write("\"SCHEDULED\": "+ jobmanager.getArchive().getJobTime(jobEvent.getJobID(), JobStatus.SCHEDULED) + ",");
			wrt.write("\"RUNNING\": "+ jobmanager.getArchive().getJobTime(jobEvent.getJobID(), JobStatus.RUNNING) + ",");
			wrt.write("\"FINISHED\": "+ jobmanager.getArchive().getJobTime(jobEvent.getJobID(), JobStatus.FINISHED) + ",");
			wrt.write("\"FAILED\": "+ jobmanager.getArchive().getJobTime(jobEvent.getJobID(), JobStatus.FAILED) + ",");
			wrt.write("\"CANCELED\": "+ jobmanager.getArchive().getJobTime(jobEvent.getJobID(), JobStatus.CANCELED) + ",");
			wrt.write("\"CREATED\": " + jobmanager.getArchive().getJobTime(jobEvent.getJobID(), JobStatus.CREATED)+",");

			if (jobEvent.getJobStatus() == JobStatus.FAILED) {
			ManagementGraphIterator managementGraphIterator =  new ManagementGraphIterator(jobManagementGraph,true);
			wrt.write("\"failednodes\": [");
			HashSet<String> map = new HashSet<String>();
			boolean first = true;
			while (managementGraphIterator.hasNext()) {
				ManagementVertex managementVertex = managementGraphIterator.next();
				String instanceName = managementVertex.getInstanceName();
				if (managementVertex.getExecutionState() == ExecutionState.FAILED && !map.contains(instanceName)) {
					if (first) {
						first = false;
					} else {
						wrt.write(",");
					}
					wrt.write("{");
					wrt.write("\"node\": \"" + instanceName + "\",");
					wrt.write("\"message\": \"" + StringUtils.escapeHtml(managementVertex.getOptMessage()) + "\"");
					wrt.write("}");
					map.add(instanceName);
				}
			}
			wrt.write("],");
			}

			// Serialize ManagementGraph to json
			wrt.write("\"groupvertices\": [");
			boolean first = true;
			for(ManagementGroupVertex groupVertex : jobManagementGraph.getGroupVerticesInTopologicalOrder()) {
				//Write seperator between json objects
				if(first) {
					first = false;
				} else {
					wrt.write(","); }
				
				wrt.write(groupVertex.toJson());
				
			}
			wrt.write("],");
			
			// write accumulators
			AccumulatorEvent accumulators = jobmanager.getAccumulatorResults(jobEvent.getJobID());
			Map<String, Object> accMap = AccumulatorHelper.toResultMap(accumulators.getAccumulators());
			
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
			for(ManagementGroupVertex groupVertex : jobManagementGraph.getGroupVerticesInTopologicalOrder()) {
				
				if(first) {
					first = false;
				} else {
					wrt.write(","); }
				
				// Calculate start and end time for groupvertex
				long started = Long.MAX_VALUE;
				long ended = 0;
				
				// Take earliest running state and latest endstate of groupmembers
				for(int j = 0; j < groupVertex.getNumberOfGroupMembers(); j++) {
					ManagementVertex vertex = groupVertex.getGroupMember(j);
					
					long running = jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.RUNNING);
					if(running != 0 && running < started) {
						started = running;
					}
					
					long finished = jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.FINISHED);
					long canceled = jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.CANCELED);
					long failed = jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.FAILED);
					
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
				
				wrt.write("\""+groupVertex.getID()+"\": {");
				wrt.write("\"groupvertexid\": \"" + groupVertex.getID() + "\",");
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
			for(AbstractEvent event: events) {
				
				if(event instanceof ExecutionStateChangeEvent) {
					
					if(first) {
						first = false;
					} else {
						wrt.write(","); }
					
					ExecutionStateChangeEvent vertexevent = (ExecutionStateChangeEvent) event;
					wrt.write("{");
					wrt.write("\"vertexid\": \"" + vertexevent.getVertexID() + "\",");
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
	 * Writes infos about one particular archived groupvertex in a job, including all groupmembers, their times and status
	 * 
	 * @param wrt
	 * @param jobEvent
	 * @param groupvertexId
	 */
	private void writeJsonForArchivedJobGroupvertex(PrintWriter wrt, RecentJobEvent jobEvent, ManagementGroupVertexID groupvertexId) {
		
		
		try {
		
		ManagementGraph jobManagementGraph = jobmanager.getManagementGraph(jobEvent.getJobID());
		
		ManagementGroupVertex groupvertex = jobManagementGraph.getGroupVertexByID(groupvertexId);
		
		// Serialize ManagementGraph to json
		wrt.write("{\"groupvertex\": "+groupvertex.toJson()+",");
		
		wrt.write("\"verticetimes\": {");
		boolean first = true;
		for(ManagementGroupVertex groupVertex : jobManagementGraph.getGroupVerticesInTopologicalOrder()) {
			
			for(int j = 0; j < groupVertex.getNumberOfGroupMembers(); j++) {
				ManagementVertex vertex = groupVertex.getGroupMember(j);
				
				if(first) {
					first = false;
				} else {
					wrt.write(","); }
				
				wrt.write("\""+vertex.getID()+"\": {");
				wrt.write("\"vertexid\": \"" + vertex.getID() + "\",");
				wrt.write("\"vertexname\": \"" + vertex + "\",");
				wrt.write("\"CREATED\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.CREATED) + ",");
				wrt.write("\"SCHEDULED\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.SCHEDULED) + ",");
				wrt.write("\"ASSIGNED\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.ASSIGNED) + ",");
				wrt.write("\"READY\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.READY) + ",");
				wrt.write("\"STARTING\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.STARTING) + ",");
				wrt.write("\"RUNNING\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.RUNNING) + ",");
				wrt.write("\"FINISHING\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.FINISHING) + ",");
				wrt.write("\"FINISHED\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.FINISHED) + ",");
				wrt.write("\"CANCELING\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.CANCELING) + ",");
				wrt.write("\"CANCELED\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.CANCELED) + ",");
				wrt.write("\"FAILED\": "+ jobmanager.getArchive().getVertexTime(jobEvent.getJobID(), vertex.getID(), ExecutionState.FAILED) + "");
				wrt.write("}");
			}
			
		}
		wrt.write("}}");
		
	} catch (EofException eof) { // Connection closed by client
		LOG.info("Info server for jobmanager: Connection closed by client, EofException");
	} catch (IOException ioe) { // Connection closed by client	
		LOG.info("Info server for jobmanager: Connection closed by client, IOException");
	} 
		
	}
	
	/**
	 * Writes the version and the revision of Stratosphere.
	 * 
	 * @param wrt
	 */
	private void writeJsonForVersion(PrintWriter wrt) {
		wrt.write("{");
		wrt.write("\"version\": \"" + JobManagerUtils.getVersion() + "\",");
		wrt.write("\"revision\": \"" + JobManagerUtils.getRevision() + "\"");
		wrt.write("}");
	}
}
