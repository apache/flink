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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Servlet that displays the Configruation in the webinterface.
 *
 */
public class MenuServlet extends HttpServlet {

	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = 117543213991787547L;
	
	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(MenuServlet.class);
	
	/**
	 * Array of possible menu entries on the left
	 */
	private static final String[] entries =  {
		"index", "history", "configuration", "taskmanagers"
	};
	
	/**
	 * The names of the menu entries shown in the browser
	 */
	private static final String[] names = {
		"Dashboard", "History", "Configuration", "Task Managers"
	};
	
	/**
	 * The classes of the icons shown next to the names in the browser
	 */
	private static final String[] classes = {
		"fa fa-dashboard", "fa fa-bar-chart-o", "fa fa-keyboard-o", "fa fa-building-o"
	};
	
	public MenuServlet() {
		if (names.length != entries.length || names.length != classes.length) {
			LOG.fatal("The Arrays 'entries', 'classes' and 'names' differ in thier length. This is not allowed!");
		}
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("application/json");
		
		if ("index".equals(req.getParameter("get"))) {
			writeMenu("index", resp);
		} else if ("analyze".equals(req.getParameter("get"))) {
			writeMenu("analyze", resp);
		} else if ("history".equals(req.getParameter("get"))) {
			writeMenu("history", resp);
		} else if ("configuration".equals(req.getParameter("get"))) {
			writeMenu("configuration", resp);
		} else if ("taskmanagers".equals(req.getParameter("get"))) {
			writeMenu("taskmanagers", resp);
		}

	}
	
	private void writeMenu(String me, HttpServletResponse resp) throws IOException {
		
		String r = "";
		
		for (int i = 0; i < entries.length; i++) {
			if (entries[i].equals(me)) {
				r += writeLine(3, "<li class='active'><a href='"+ entries[i] +".html'><i class='"+ classes[i] +"'></i> "+ names[i] +"</a></li>");
			} else {
				r += writeLine(3, "<li><a href='"+ entries[i] +".html'><i class='"+ classes[i] +"'></i> "+ names[i] +"</a></li>");
			}
		}
		
		resp.getWriter().write(r);
	}
	
	private String writeLine(int tab, String line) {
		String s = "";
		for (int i = 0; i < tab; i++) {
			s += "\t";
		}
		s+= " " + line + " \n";
		return s;
	}
	
}
