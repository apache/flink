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
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;

/**
 * A Servlet that displays the Configruation in the webinterface.
 *
 */
public class ConfigurationServlet extends HttpServlet {

	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = 3704963598772630435L;
	
	private Configuration globalC;

	
	public ConfigurationServlet() {
		globalC = GlobalConfiguration.getConfiguration();
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("application/json");
		
		Set<String> keys = globalC.keySet();
		List<String> list = new ArrayList<String>(keys);
		Collections.sort(list);
		
		JSONObject obj = new JSONObject();
		for (String k : list) {
			try {
				obj.put(k, globalC.getString(k, ""));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
		PrintWriter w = resp.getWriter();
		w.write(obj.toString());

	}
	
}
