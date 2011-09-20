/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.client.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * @author Stephan Ewen (stephan.ewen@tu-berlin.com)
 */
public class PlanDisplayServlet extends GUIServletStub {
	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = 3610115341264927614L;

	/**
	 * Default constructor. Sets up all CSS and JS files for the header.
	 */
	public PlanDisplayServlet() {
		super("Nephele/PACT Query Interface - Query Plan");

		addStyleSheet("css/js-graph-it.css");
		addStyleSheet("css/pactgraphs.css");

		addJavascriptFile("js/js-graph-it.js");
		addJavascriptFile("js/progressbar.js");
		addJavascriptFile("js/pactgraph.js");
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.stratosphere.desktop.ServletStub#printPage(java.io.PrintWriter)
	 */
	@Override
	public void printPage(PrintWriter writer, Map<String, String[]> parameters) throws IOException {
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

		boolean suspended = Boolean.parseBoolean(suspend);

		// write the canvas for the graph area
		writer
			.println("    <div style=\"position: relative;\">\n"
				+ "      <div id=\"mainCanvas\" class=\"canvas boxed\" style=\"height: 500px;\">\n"
				+ "        <div align=\"center\" id=\"progressContainer\" style=\"margin: auto; margin-top: 200px;\"></div>\n"
				+ "      </div>\n" + "      <div style=\"position: absolute; right: 20px; bottom: 20px;\">\n"
				+ "        <input id=\"back_button\" type=\"button\" value=\"&lt; Back\"/>");
		if (suspended) {
			writer.println("        <input id=\"run_button\" type=\"button\" value=\"Run\"/>");
		}
		writer.println("      </div>\n" + "    </div>");

		// write the canvas for the properties area
		writer.println("    <div id=\"propertyCanvas\" class=\"propertyCanvas\">\n"
			+ "      <p class=\"fadedPropertiesText\">Click a node to show the properties...</p>\n" + "    </div>");

		// write the page initialization code
		writer.println("    <script type=\"text/javascript\">\n" + "    <!--\n" + "      var maxColumnWidth = 350;\n"
			+ "      var minColumnWidth = 150;\n\n" + "      $(document).ready(function() {\n"
			+ "        // create the progress bar that animates the waiting until the plan is\n"
			+ "        // retrieved and displayed\n"
			+ "        progBar = new ProgressBar(\"progressContainer\", 10);\n" + "        progBar.Init();\n"
			+ "        progBar.Start();\n");

		writer.println("        // register the event handler for the 'continue' button\n"
			+ "        $('#run_button').click(function () {\n" + "          $('#run_button').remove();\n"
			+ "          $.ajax( {" + " url: '/runJob'," + " data: { action: 'runsubmitted', id: '" + uid + "' },"
			+ " success: function () { alert('Job succesfully submitted');},"
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

		writer.println("        // use jquery to asynchronously load the pact plan description\n"
			+ "        $.getJSON(\"ajax-plans/" + uid
			+ ".json\", function(data) { drawPactPlan(data, true, \"arr.gif\"); });" + "      });\n" + "    //-->\n"
			+ "    </script>");
	}
}
