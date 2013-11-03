package eu.stratosphere.nephele.jobmanager.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.io.EofException;

import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.util.StringUtils;

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
		
		try {
			List<RecentJobEvent> jobs = jobmanager.getRecentJobs();
			
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.setContentType("application/json");
			PrintWriter wrt = resp.getWriter();
			
			wrt.write("[");
			
			// Loop Jobs
			for (int i = 0; i < jobs.size(); i++) {
				RecentJobEvent jobEvent = jobs.get(i);
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
		} catch (Exception e) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.getWriter().print(e.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(e));
			}
		}
	}
}
