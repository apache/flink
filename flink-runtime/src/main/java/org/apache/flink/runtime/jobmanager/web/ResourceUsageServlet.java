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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.flink.runtime.profiling.types.InstanceSummaryProfilingEvent;
import org.apache.flink.runtime.profiling.types.ProfilingEvent;
import org.apache.flink.util.StringUtils;

public class ResourceUsageServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory
			.getLog(ResourceUsageServlet.class);

	private final Map<Class<? extends ProfilingEvent>, ProfilingEventSerializer<? extends ProfilingEvent>> jsonSerializers = new HashMap<Class<? extends ProfilingEvent>, ResourceUsageServlet.ProfilingEventSerializer<?>>();

	private final JobManager jobManager;

	public ResourceUsageServlet(JobManager jobManager) {
		this.jobManager = jobManager;

		this.jsonSerializers.put(InstanceSummaryProfilingEvent.class,
				new InstanceSummaryProfilingEventSerializer());
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		try {
			JobID jobID = getJobID(req);

			if (jobID == null) {
				resp.setStatus(HttpServletResponse.SC_OK);
				resp.setContentType("application/json");
//				resp.getWriter().write("[{type:\"dummy\",timestamp:123456789,jobid:\"abcdef0123456789\"}]");
//				resp.getWriter().write(this.jobManager.getRecentJobs().toString());
				resp.getWriter().write("[]");
				return;
			}

			resp.setStatus(HttpServletResponse.SC_OK);
			resp.setContentType("application/json");
			resp.getWriter().write("[");
			
			List<AbstractEvent> allJobEvents = this.jobManager.getEvents(jobID);
			String separator = "";
			for (AbstractEvent jobEvent : allJobEvents) {
				if (jobEvent instanceof ProfilingEvent) {
					ProfilingEvent profilingEvent = (ProfilingEvent) jobEvent;
					ProfilingEventSerializer<ProfilingEvent> jsonSerializer = getSerializer(profilingEvent);
					if (jsonSerializer != null) {
						resp.getWriter().write(separator);
						jsonSerializer.write(profilingEvent, resp.getWriter());
						separator = ",";
					} else {
						resp.getWriter().write(separator);
						new ProfilingEventSerializer<ProfilingEvent>().write(profilingEvent, resp.getWriter());
						separator = ",";
					}
				}

			}
			
			resp.getWriter().write("]");

//			resp.setStatus(HttpServletResponse.SC_OK);
//			resp.setContentType("text/html");
//			PrintWriter writer = resp.getWriter();
//			writer.write("<p>Profiling events</p>");
//			writer.write("<ol>");
//			for (ProfilingEvent profilingEvent : profilingEvents) {
//				writer.write("<li>");
//				writer.write(profilingEvent.getJobID().toString());
//				writer.write(" - ");
//				writer.write(new Date(profilingEvent.getTimestamp()).toString());
//				writer.write(" - ");
//				writer.write(profilingEvent.toString());
//				writer.write("</li>");
//			}
//			writer.write("</ol>");

		} catch (Exception e) {
			resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			resp.setContentType("text/html");
			resp.getWriter().print(e.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(e));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private  ProfilingEventSerializer<ProfilingEvent> getSerializer(ProfilingEvent profilingEvent) {
		return (ProfilingEventSerializer<ProfilingEvent>) this.jsonSerializers.get(profilingEvent.getClass());
	}
	
	/** Loads the job ID from the request or selects the latest submitted job. */
	private JobID getJobID(HttpServletRequest req) throws IOException {
		String jobIdParameter = req.getParameter("jobid");
		JobID jobID = jobIdParameter == null ? loadLatestJobID() : JobID
				.fromHexString(jobIdParameter);
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
					|| mostRecentJobEvent.getSubmissionTimestamp() < jobEvent
							.getSubmissionTimestamp()) {
				mostRecentJobEvent = jobEvent;
			}
		}
		return mostRecentJobEvent == null ? null : mostRecentJobEvent
				.getJobID();
	}

	private static class ProfilingEventSerializer<T extends ProfilingEvent> {

		private PrintWriter writer;
		private String separator;

		protected void writeField(String name, String value) {
			this.writer.write(separator);
			this.writer.write("\"");
			this.writer.write(name);
			this.writer.write("\":\"");
			this.writer.write(value);
			this.writer.write("\"");
			this.separator = ",";
		}

		protected void writeField(String name, long value) {
			this.writer.write(separator);
			this.writer.write("\"");
			this.writer.write(name);
			this.writer.write("\":");
			this.writer.write(Long.toString(value));
			this.separator = ",";
		}

		protected void writeFields(T profilingEvent) {
			writeField("type", profilingEvent.getClass().getSimpleName());
			writeField("jobID", profilingEvent.getJobID().toString());
			writeField("timestamp", profilingEvent.getTimestamp());
		}

		public synchronized void write(T profilingEvent, PrintWriter writer)
				throws IOException {
			this.writer = writer; // cache the writer for convenience -- we are
									// synchronized here
			this.writer.write("{");
			this.separator = "";
			writeFields(profilingEvent);
			this.writer.write("}");
			this.writer = null;
		}

	}

	private static class InstanceSummaryProfilingEventSerializer extends
			ProfilingEventSerializer<InstanceSummaryProfilingEvent> {

		@Override
		protected void writeFields(InstanceSummaryProfilingEvent profilingEvent) {
			super.writeFields(profilingEvent);
			writeField("userCpu", profilingEvent.getUserCPU());
			writeField("totalMemory", profilingEvent.getTotalMemory());
			writeField("freeMemory", profilingEvent.getFreeMemory());
			writeField("transmittedBytes", profilingEvent.getTransmittedBytes());
		}

	}

}
