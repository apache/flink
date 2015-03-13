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

package org.apache.flink.client.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

/**
 * Simple servlet that displays the visualization of a data flow plan.
 */
public class PlanDisplayServlet extends GUIServletStub {

	private static final long serialVersionUID = 3610115341264927614L;
	
	
	private final int runtimeVisualizationPort;
	
	private String runtimeVisURL;

	/**
	 * Default constructor. Sets up all CSS and JS files for the header.
	 */
	public PlanDisplayServlet(int runtimePort) {
		super("Flink Query Interface - Query Plan");
		
		this.runtimeVisualizationPort = runtimePort;

		addStyleSheet("css/nephelefrontend.css");
		addStyleSheet("css/pactgraphs.css");
		addStyleSheet("css/graph.css");
		addStyleSheet("css/overlay.css");
		addStyleSheet("css/bootstrap.css");

		addJavascriptFile("js/jquery-2.1.0.js");
		addJavascriptFile("js/graphCreator.js");
		addJavascriptFile("js/d3.js");
		addJavascriptFile("js/dagre-d3.js");
		addJavascriptFile("js/bootstrap.min.js");
		addJavascriptFile("js/jquery.tools.min.js");

	}

	@Override
	public void printPage(PrintWriter writer, Map<String, String[]> parameters, HttpServletRequest req) throws IOException {
		String[] x = parameters.get("id");
		String uid = (x != null && x.length >= 1) ? x[0] : null;

		x = parameters.get("suspended");
		String suspend = (x != null && x.length >= 1) ? x[0] : null;

		// check, if all parameters are there
		if (uid == null || suspend == null) {
			writer.println("    <div class=\"error_text\" style=\"margin-top: 50px; font-size: 18px;\">");
			writer.println("      <p>Parameters identifying the plan and the suspension strategy are missing.</p>");
			writer.println("    </div>");
			return;
		}
		
		if (this.runtimeVisURL == null) {
			try {
				URI request = new URI(req.getRequestURL().toString());
				URI vizURI = new URI(request.getScheme(), null, request.getHost(), runtimeVisualizationPort, null, null, null);
				this.runtimeVisURL = vizURI.toString();
			} catch (URISyntaxException e) {
				; // ignore and simply do not forward
			}
		}
		
		boolean suspended = Boolean.parseBoolean(suspend);

		// write the canvas for the graph area
		writer.println("    <div style=\"position: relative;\">\n"
					+ "      <div id=\"mainCanvas\" class=\"canvas boxed\">\n"
					+ "      <div id=\"attach\"><svg id=\"svg-main\" width=500 height=500><g transform=\"translate(20, 20)\"/></svg></div>"
					+ "      </div>\n"
					+ "      <div style=\"position: absolute; right: 20px; bottom: 20px;\">\n"
					+ "        <input id=\"back_button\" type=\"button\" value=\"&lt; Back\"/>");
		if (suspended) {
			writer.println("        <input id=\"run_button\" type=\"button\" value=\"Continue &gt;\"/>");
		}
		writer.println("      </div>\n" + "    </div>");

		// write the canvas for the properties area
		writer.println("    <div class=\"simple_overlay\" id=\"propertyO\">"
				+ "<div id=\"propertyCanvas\" class=\"propertyCanvas\"></div>\n"
				+ "    </div>");

		// write the page initialization code
		writer.println("    <script type=\"text/javascript\">\n" + "    <!--\n" + "      var maxColumnWidth = 350;\n"
			+ "      var minColumnWidth = 150;\n\n" + "      $(document).ready(function() {\n");

		writer.println("        // register the event handler for the 'run' button and activate zoom Buttons\n"
					+ " activateZoomButtons();"
					+ "        $('#run_button').click(function () {\n" + "          $('#run_button').remove();\n"
					+ "          $.ajax( {" + " url: '/runJob'," + " data: { action: 'runsubmitted', id: '" + uid + "' },"
					+ " success: function () { alert('Job succesfully submitted');"
					+ (this.runtimeVisURL != null ? (" window.location = \"" + this.runtimeVisURL + "\"; },") : " },")
					+ " error: function (xhr, ajaxOptions, thrownError) { alert(xhr.responseText); }" + "          });\n"
					+ "        });\n");

		writer.println("        // register the event handler for the 'back' button\n"
					+ "        $('#back_button').click(function () {\n"
		+ (suspended ? "          var url = \"/runJob?\" + $.param({action: \"back\", id: \"" + uid + "\" });\n"
					: "          var url = \"" + JobSubmissionServlet.START_PAGE_URL + "\";\n")
					+ "          window.location = url;\n" + "        });\n");

		// writer.println(
		// "        // register an ajax error handler\n" +
		// "        $(document).ajaxError(function(e, xhr, settings, exception) {\n" +
		// "          var str = \"An error occurred while getting the PACT plan:\\n\";\n" +
		// "          str += \"\\nUrl: \" + settings.url;\n" +
		// "          str += \"\\nException: \" + exception;\n" +
		// "          alert(str);\n" +
		// "        });\n");

		writer.println("        //change height of mainCanvas to maximum"
				+ "        $(\"#mainCanvas\").css(\"height\", $(document).height() - 15 - 105);\n"
				+ "        // use jquery to asynchronously load the pact plan description\n"
			+ "        $.getJSON(\"ajax-plans/" + uid
			+ ".json\", function(data) { drawGraph(data, \"#svg-main\"); });" + "      });\n" + "    //-->\n"
			+ "    </script>");
	}
}
