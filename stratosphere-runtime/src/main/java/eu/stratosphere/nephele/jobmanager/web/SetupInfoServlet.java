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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.Instance;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobmanager.JobManager;

/**
 * A Servlet that displays the Configruation in the webinterface.
 *
 */
public class SetupInfoServlet extends HttpServlet {

	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = 3704963598772630435L;
	
	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(SetupInfoServlet.class);
	
	private Configuration globalC;
	private JobManager jobmanager;
	
	public SetupInfoServlet(JobManager jm) {
		globalC = GlobalConfiguration.getConfiguration();
		this.jobmanager = jm;
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
		
		Set<InstanceConnectionInfo> keys = jobmanager.getInstances().keySet();
		List<InstanceConnectionInfo> list = new ArrayList<InstanceConnectionInfo>(keys);
		Collections.sort(list);
				
		JSONObject obj = new JSONObject();
		JSONArray array = new JSONArray();
		for (InstanceConnectionInfo k : list) {
			JSONObject objInner = new JSONObject();
			
			Instance instance = jobmanager.getInstances().get(k);	
			long time = new Date().getTime() - instance.getLastHeartBeat();
	
			try {
				objInner.put("inetAdress", k.getInetAdress());
				objInner.put("ipcPort", k.ipcPort());
				objInner.put("dataPort", k.dataPort());
				objInner.put("timeSinceLastHeartbeat", time / 1000);
				objInner.put("slotsNumber", instance.getNumberOfSlots());
				objInner.put("freeSlots", instance.getNumberOfAvailableSlots());
				objInner.put("cpuCores", instance.getHardwareDescription().getNumberOfCPUCores());
				objInner.put("physicalMemory", instance.getHardwareDescription().getSizeOfPhysicalMemory() / 1048576);
				objInner.put("freeMemory", instance.getHardwareDescription().getSizeOfFreeMemory() / 1048576);
				array.put(objInner);
			} catch (JSONException e) {
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
	
}
