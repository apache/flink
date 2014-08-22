/**
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
import java.util.Date;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.profiling.types.ProfilingEvent;
import org.apache.flink.util.StringUtils;

public class ResourceUsageServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(ResourceUsageServlet.class);

	private JobManager jobManager;

	public ResourceUsageServlet(JobManager jobManager) {
		this.jobManager = jobManager;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			JobID jobID = getJobID(req);

			if (jobID == null) {
				resp.setStatus(HttpServletResponse.SC_OK);
				resp.setContentType("text/html");
				resp.getWriter().write("<p>No job found.</p>");
				return;
			}

			List<AbstractEvent> allJobEvents = this.jobManager.getEvents(jobID);
			List<ProfilingEvent> profilingEvents = new ArrayList<ProfilingEvent>(allJobEvents.size());
			for (AbstractEvent jobEvent : allJobEvents) {
				if (jobEvent instanceof ProfilingEvent) {
					profilingEvents.add((ProfilingEvent) jobEvent);
				}
			}

			resp.setStatus(HttpServletResponse.SC_OK);
			resp.setContentType("text/html");
			PrintWriter writer = resp.getWriter();
			writer.write("<p>Profiling events</p>");
			writer.write("<ol>");
			for (ProfilingEvent profilingEvent : profilingEvents) {
				writer.write("<li>");
				writer.write(profilingEvent.getJobID().toString());
				writer.write(" - ");
				writer.write(new Date(profilingEvent.getTimestamp()).toString());
				writer.write(" - ");
				writer.write(profilingEvent.toString());
				writer.write("</li>");
			}
			writer.write("</ol>");

		} catch (Exception e) {
			resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			resp.setContentType("text/html");
			resp.getWriter().print(e.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(e));
			}
		}
	}

	/** Loads the job ID from the request or selects the latest submitted job. */
	private JobID getJobID(HttpServletRequest req) throws IOException {
		String jobIdParameter = req.getParameter("jobid");
		JobID jobID = jobIdParameter == null ? loadLatestJobID() : JobID.fromHexString(jobIdParameter);
		return jobID;
	}

	/**
	 * @return the latest job ID from the {@link #jobManager}.
	 * @throws IOException
	 *             if there is a problem with retrieving the job list
	 */
	private JobID loadLatestJobID() throws IOException {
		List<RecentJobEvent> recentJobEvents = this.jobManager.getRecentJobs();
		RecentJobEvent mostRecentJobEvent = null;
		for (RecentJobEvent jobEvent : recentJobEvents) {
			if (mostRecentJobEvent == null
					|| mostRecentJobEvent.getSubmissionTimestamp() < jobEvent.getSubmissionTimestamp()) {
				mostRecentJobEvent = jobEvent;
			}
		}
		return mostRecentJobEvent == null ? null : mostRecentJobEvent.getJobID();
	}

}
