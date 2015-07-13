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
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.Instance;

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.RegisteredTaskManagers;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestStackTrace;
import org.apache.flink.runtime.messages.TaskManagerMessages.StackTrace;
import org.apache.flink.util.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * A Servlet that displays the Configuration in the web interface.
 */
public class SetupInfoServlet extends HttpServlet {

	/** Serial UID for serialization interoperability. */
	private static final long serialVersionUID = 3704963598772630435L;

	/** The log for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(SetupInfoServlet.class);


	final private Configuration configuration;
	final private ActorRef jobmanager;
	final private FiniteDuration timeout;


	public SetupInfoServlet(Configuration conf, ActorRef jm, FiniteDuration timeout) {
		configuration = conf;
		this.jobmanager = jm;
		this.timeout = timeout;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("application/json");

		if ("globalC".equals(req.getParameter("get"))) {
			writeGlobalConfiguration(resp);
		} else if ("taskmanagers".equals(req.getParameter("get"))) {
			writeTaskmanagers(resp);
		} else if ("stackTrace".equals(req.getParameter("get"))) {
			String instanceId = req.getParameter("instanceID");
			writeStackTraceOfTaskManager(instanceId, resp);
		}
	}

	private void writeGlobalConfiguration(HttpServletResponse resp) throws IOException {
		Set<String> keys = configuration.keySet();
		List<String> list = new ArrayList<String>(keys);
		Collections.sort(list);

		JSONObject obj = new JSONObject();
		for (String k : list) {
			try {

				obj.put(k, configuration.getString(k, ""));
			} catch (JSONException e) {
				LOG.warn("Json object creation failed", e);
			}
		}

		PrintWriter w = resp.getWriter();
		w.write(obj.toString());
	}

	private void writeTaskmanagers(HttpServletResponse resp) throws IOException {

		final Future<Object> response = Patterns.ask(jobmanager,
				JobManagerMessages.getRequestRegisteredTaskManagers(),
				new Timeout(timeout));

		Object obj = null;

		try{
			obj = Await.result(response, timeout);
		} catch (Exception ex) {
			throw new IOException("Could not retrieve all registered task managers from the " +
					"job manager.", ex);
		}

		if(!(obj instanceof RegisteredTaskManagers)){
			throw new RuntimeException("RequestRegisteredTaskManagers should return a response of " +
					"type RegisteredTaskManagers. Instead the respone is of type " +
					obj.getClass() + ".");
		} else {

			final List<Instance> instances = new ArrayList<Instance>(
					((RegisteredTaskManagers) obj).asJavaCollection());

			Collections.sort(instances, INSTANCE_SORTER);

			JSONObject jsonObj = new JSONObject();
			JSONArray array = new JSONArray();
			for (Instance instance : instances) {
				JSONObject objInner = new JSONObject();

				long time = new Date().getTime() - instance.getLastHeartBeat();

				try {
					objInner.put("path", instance.getInstanceGateway().path());
					objInner.put("dataPort", instance.getInstanceConnectionInfo().dataPort());
					objInner.put("timeSinceLastHeartbeat", time / 1000);
					objInner.put("slotsNumber", instance.getTotalNumberOfSlots());
					objInner.put("freeSlots", instance.getNumberOfAvailableSlots());
					objInner.put("cpuCores", instance.getResources().getNumberOfCPUCores());
					objInner.put("physicalMemory", instance.getResources().getSizeOfPhysicalMemory() >>> 20);
					objInner.put("freeMemory", instance.getResources().getSizeOfJvmHeap() >>> 20);
					objInner.put("managedMemory", instance.getResources().getSizeOfManagedMemory() >>> 20);
					objInner.put("instanceID", instance.getId());
					byte[] report = instance.getLastMetricsReport();
					if(report != null) {
						objInner.put("metrics", new JSONObject(new String(report, "utf-8")));
					}
					array.put(objInner);
				} catch (JSONException e) {
					LOG.warn("Json object creation failed", e);
				}

			}
			try {
				jsonObj.put("taskmanagers", array);
			} catch (JSONException e) {
				LOG.warn("Json object creation failed", e);
			}

			PrintWriter w = resp.getWriter();
			w.write(jsonObj.toString());
		}
	}


	private void writeStackTraceOfTaskManager(String instanceIdStr, HttpServletResponse resp) throws IOException {
		InstanceID instanceID = new InstanceID(StringUtils.hexStringToByte(instanceIdStr));
		StackTrace message = null;
		Throwable exception = null;

		final Future<Object> response = Patterns.ask(jobmanager,
				new RequestStackTrace(instanceID),
				new Timeout(timeout));

		try {
			message = (StackTrace) Await.result(response, timeout);
		} catch (Exception ex) {
			exception = ex;
		}

		JSONObject obj = new JSONObject();
		try {
			if (message != null) {
				obj.put("stackTrace", message.stackTrace());
			} else if (exception != null) {
				obj.put("errorMessage", exception.getMessage());
			}
		} catch (JSONException e) {
			LOG.warn("Json object creation failed", e);
		}

		PrintWriter writer = resp.getWriter();
		writer.write(obj.toString());
	}
	// --------------------------------------------------------------------------------------------

	private static final Comparator<Instance> INSTANCE_SORTER = new Comparator<Instance>() {
		@Override
		public int compare(Instance o1, Instance o2) {
		return o1.getInstanceConnectionInfo().compareTo(o2.getInstanceConnectionInfo());
		}
	};
}
