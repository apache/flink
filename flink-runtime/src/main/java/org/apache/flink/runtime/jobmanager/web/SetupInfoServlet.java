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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.Instance;

import org.apache.flink.runtime.messages.JobManagerMessages.RequestRegisteredTaskManagers$;
import org.apache.flink.runtime.messages.JobManagerMessages.RegisteredTaskManagers;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

/**
 * A Servlet that displays the Configuration in the web interface.
 */
public class SetupInfoServlet extends HttpServlet {

	/** Serial UID for serialization interoperability. */
	private static final long serialVersionUID = 3704963598772630435L;
	
	/** The log for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(SetupInfoServlet.class);
	
	
	final private Configuration globalC;
	final private ActorRef jobmanager;
	final private FiniteDuration timeout;
	
	
	public SetupInfoServlet(ActorRef jm, FiniteDuration timeout) {
		globalC = GlobalConfiguration.getConfiguration();
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
		}
	}
	
	private void writeGlobalConfiguration(HttpServletResponse resp) throws IOException {
		
		Set<String> keys = globalC.keySet();
		List<String> list = new ArrayList<String>(keys);
		Collections.sort(list);
		
		JSONObject obj = new JSONObject();
		for (String k : list) {
			try {
				obj.put(k, globalC.getString(k, ""));
			} catch (JSONException e) {
				LOG.warn("Json object creation failed", e);
			}
		}
		
		PrintWriter w = resp.getWriter();
		w.write(obj.toString());
	}
	
	private void writeTaskmanagers(HttpServletResponse resp) throws IOException {

		List<Instance> instances = new ArrayList<Instance>(AkkaUtils.<RegisteredTaskManagers>ask
				(jobmanager, RequestRegisteredTaskManagers$.MODULE$, timeout).asJavaCollection());

		Collections.sort(instances, INSTANCE_SORTER);
				
		JSONObject obj = new JSONObject();
		JSONArray array = new JSONArray();
		for (Instance instance : instances) {
			JSONObject objInner = new JSONObject();
				
			long time = new Date().getTime() - instance.getLastHeartBeat();
	
			try {
				objInner.put("inetAdress", instance.getInstanceConnectionInfo().getInetAdress());
				objInner.put("ipcPort", instance.getTaskManager().path().address().hostPort());
				objInner.put("dataPort", instance.getInstanceConnectionInfo().dataPort());
				objInner.put("timeSinceLastHeartbeat", time / 1000);
				objInner.put("slotsNumber", instance.getTotalNumberOfSlots());
				objInner.put("freeSlots", instance.getNumberOfAvailableSlots());
				objInner.put("cpuCores", instance.getResources().getNumberOfCPUCores());
				objInner.put("physicalMemory", instance.getResources().getSizeOfPhysicalMemory() >>> 20);
				objInner.put("freeMemory", instance.getResources().getSizeOfJvmHeap() >>> 20);
				objInner.put("managedMemory", instance.getResources().getSizeOfManagedMemory() >>> 20);
				array.put(objInner);
			}
			catch (JSONException e) {
				LOG.warn("Json object creation failed", e);
			}
			
		}
		try {
			obj.put("taskmanagers", array);
		} catch (JSONException e) {
			LOG.warn("Json object creation failed", e);
		}
		
		PrintWriter w = resp.getWriter();
		w.write(obj.toString());
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final Comparator<Instance> INSTANCE_SORTER = new Comparator<Instance>() {
		@Override
		public int compare(Instance o1, Instance o2) {
			return o1.getInstanceConnectionInfo().compareTo(o2.getInstanceConnectionInfo());
		}
	};
}
