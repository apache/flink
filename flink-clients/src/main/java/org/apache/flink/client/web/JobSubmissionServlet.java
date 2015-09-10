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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.configuration.GlobalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobSubmissionServlet extends HttpServlet {

	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = 8447312301029847397L;

	// ------------------------------------------------------------------------

	public static final String START_PAGE_URL = "launch.html";

	private static final String ACTION_PARAM_NAME = "action";

	private static final String ACTION_SUBMIT_VALUE = "submit";

	private static final String ACTION_RUN_SUBMITTED_VALUE = "runsubmitted";

	private static final String ACTION_BACK_VALUE = "back";

	private static final String OPTIONS_PARAM_NAME = "options";

	private static final String JOB_PARAM_NAME = "job";

	private static final String CLASS_PARAM_NAME = "assemblerClass";

	private static final String ARGUMENTS_PARAM_NAME = "arguments";

	private static final String SHOW_PLAN_PARAM_NAME = "show_plan";

	private static final String SUSPEND_PARAM_NAME = "suspend";

	private static final Logger LOG = LoggerFactory.getLogger(JobSubmissionServlet.class);

	// ------------------------------------------------------------------------

	private final File jobStoreDirectory;										// the directory containing the uploaded jobs

	private final File planDumpDirectory;										// the directory to dump the optimizer plans to

	private final Map<Long, Tuple2<PackagedProgram, FlinkPlan>> submittedJobs;	// map from UIDs to the submitted jobs

	private final Random rand;													// random number generator for UID

	private final CliFrontend cli;



	public JobSubmissionServlet(CliFrontend cli, File jobDir, File planDir) {
		this.cli = cli;
		this.jobStoreDirectory = jobDir;
		this.planDumpDirectory = planDir;

		this.submittedJobs = Collections.synchronizedMap(new HashMap<Long, Tuple2<PackagedProgram, FlinkPlan>>());

		this.rand = new Random(System.currentTimeMillis());
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String action = req.getParameter(ACTION_PARAM_NAME);
		if (checkParameterSet(resp, action, "action")) {
			return;
		}

		// decide according to the action
		if (action.equals(ACTION_SUBMIT_VALUE)) {
			// --------------- submit a job -------------------

			// get the parameters
			String options = req.getParameter(OPTIONS_PARAM_NAME);
			String jobName = req.getParameter(JOB_PARAM_NAME);
			String assemblerClass = req.getParameter(CLASS_PARAM_NAME);
			String arguments = req.getParameter(ARGUMENTS_PARAM_NAME);
			String showPlan = req.getParameter(SHOW_PLAN_PARAM_NAME);
			String suspendPlan = req.getParameter(SUSPEND_PARAM_NAME);

			// check that parameters are set
			// do NOT check 'options' or 'assemblerClass' -> it is OK if not set
			if (checkParameterSet(resp, jobName, JOB_PARAM_NAME)
				|| checkParameterSet(resp, arguments, ARGUMENTS_PARAM_NAME)
				|| checkParameterSet(resp, showPlan, SHOW_PLAN_PARAM_NAME)
				|| checkParameterSet(resp, suspendPlan, SUSPEND_PARAM_NAME))
			{
				return;
			}

			boolean show = Boolean.parseBoolean(showPlan);
			boolean suspend = Boolean.parseBoolean(suspendPlan);

			List<String> cliOptions;
			try {
				cliOptions = tokenizeArguments(options);
			} catch (IllegalArgumentException iaex) {
				showErrorPage(resp, "Flink options contain an unterminated quoted string.");
				return;
			}

			List<String> cliArguments;
			try {
				cliArguments = tokenizeArguments(arguments);
			} catch (IllegalArgumentException iaex) {
				showErrorPage(resp, "Program arguments contain an unterminated quoted string.");
				return;
			}

			String[] args = new String[1 + (assemblerClass == null ? 0 : 2) + cliOptions.size() + 1 + cliArguments.size()];

			List<String> parameters = new ArrayList<String>(args.length);
			parameters.add(CliFrontend.ACTION_INFO);
			parameters.addAll(cliOptions);
			if (assemblerClass != null) {
				parameters.add("-" + CliFrontendParser.CLASS_OPTION.getOpt());
				parameters.add(assemblerClass);
			}
			parameters.add(jobStoreDirectory + File.separator + jobName);
			parameters.addAll(cliArguments);

			FlinkPlan optPlan;
			try {
				this.cli.parseParameters(parameters.toArray(args));

				optPlan = this.cli.getFlinkPlan();
				if (optPlan == null) {
					// wrapping hack to get this exception handled correctly by following catch block
					throw new RuntimeException(new Exception("The optimized plan could not be produced."));
				}
			}
			catch (RuntimeException e) {
				Throwable t = e.getCause();

				if(t instanceof ProgramInvocationException) {
					// collect the stack trace
					StringWriter sw = new StringWriter();
					PrintWriter w = new PrintWriter(sw);

					if (t.getCause() == null) {
						t.printStackTrace(w);
					} else {
						t.getCause().printStackTrace(w);
					}

					String message = sw.toString();
					message = StringEscapeUtils.escapeHtml4(message);

					showErrorPage(resp, "An error occurred while invoking the program:<br/><br/>"
							+ t.getMessage() + "<br/>"
							+ "<br/><br/><pre>" + message + "</pre>");
					return;
				} else if (t instanceof CompilerException) {
					// collect the stack trace
					StringWriter sw = new StringWriter();
					PrintWriter w = new PrintWriter(sw);
					t.printStackTrace(w);

					String message = sw.toString();
					message = StringEscapeUtils.escapeHtml4(message);

					showErrorPage(resp, "An error occurred in the compiler:<br/><br/>"
							+ t.getMessage() + "<br/>"
							+ (t.getCause() != null ? "Caused by: " + t.getCause().getMessage():"")
							+ "<br/><br/><pre>" + message + "</pre>");
					return;
				} else {
					// collect the stack trace
					StringWriter sw = new StringWriter();
					PrintWriter w = new PrintWriter(sw);
					t.printStackTrace(w);

					String message = sw.toString();
					message = StringEscapeUtils.escapeHtml4(message);

					showErrorPage(resp, "An unexpected error occurred:<br/><br/>" + t.getMessage() + "<br/><br/><pre>"
							+ message + "</pre>");
					return;
				}
			}

			// redirect according to our options
			if (show) {
				// we have a request to show the plan

				// create a UID for the job
				Long uid;
				do {
					uid = Math.abs(this.rand.nextLong());
				} while (this.submittedJobs.containsKey(uid));

				// dump the job to a JSON file
				String planName = uid + ".json";
				File jsonFile = new File(this.planDumpDirectory, planName);

				if (optPlan instanceof StreamingPlan) {
					((StreamingPlan) optPlan).dumpStreamingPlanAsJSON(jsonFile);
				}
				else {
					PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
					jsonGen.setEncodeForHTML(true);
					jsonGen.dumpOptimizerPlanAsJSON((OptimizedPlan) optPlan, jsonFile);
				}

				// submit the job only, if it should not be suspended
				if (!suspend) {
					parameters.set(0, CliFrontend.ACTION_RUN);
					try {
						this.cli.parseParameters(parameters.toArray(args));
					} catch(RuntimeException e) {
						LOG.error("Error submitting job to the job-manager.", e.getCause());
						showErrorPage(resp, e.getCause().getMessage());
						return;
					}
				}
				else {
					this.submittedJobs.put(uid, new Tuple2<PackagedProgram, FlinkPlan>(this.cli.getPackagedProgram(), optPlan));
				}

				// redirect to the plan display page
				resp.sendRedirect("showPlan?id=" + uid + "&suspended=" + (suspend ? "true" : "false"));
			}
			else {
				// don't show any plan. directly submit the job and redirect to the
				// runtime monitor
				parameters.set(0, CliFrontend.ACTION_RUN);
				try {
					this.cli.parseParameters(parameters.toArray(args));
				}
				catch (RuntimeException e) {
					LOG.error("Error submitting job to the job-manager.", e.getCause());
					// HACK: Is necessary because Message contains whole stack trace
					String errorMessage = e.getCause().getMessage().split("\n")[0];
					showErrorPage(resp, errorMessage);
					return;
				}
				resp.sendRedirect(START_PAGE_URL);
			}
		}
		else if (action.equals(ACTION_RUN_SUBMITTED_VALUE)) {
			// --------------- run a job that has been submitted earlier, but was -------------------
			// --------------- not executed because of a plan display -------------------

			String id = req.getParameter("id");
			if (checkParameterSet(resp, id, "id")) {
				return;
			}

			Long uid = null;
			try {
				uid = Long.parseLong(id);
			} catch (NumberFormatException nfex) {
				showErrorPage(resp, "An invalid id for the job was provided.");
				return;
			}

			// get the retained job
			Tuple2<PackagedProgram, FlinkPlan> job = submittedJobs.remove(uid);
			if (job == null) {
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"No job with the given uid was retained for later submission.");
				return;
			}

			// submit the job
			try {
				Client client = new Client(GlobalConfiguration.getConfiguration(), job.f0.getUserCodeClassLoader());
				client.run(client.getJobGraph(job.f0, job.f1), false);
			}
			catch (Exception ex) {
				LOG.error("Error submitting job to the job-manager.", ex);
				resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				// HACK: Is necessary because Message contains whole stack trace
				String errorMessage = ex.getMessage().split("\n")[0];
				resp.getWriter().print(errorMessage);
				// resp.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
				return;
			}

			// redirect to the start page
			resp.sendRedirect(START_PAGE_URL);
		} else if (action.equals(ACTION_BACK_VALUE)) {
			// remove the job from the map

			String id = req.getParameter("id");
			if (checkParameterSet(resp, id, "id")) {
				return;
			}

			Long uid = null;
			try {
				uid = Long.parseLong(id);
			} catch (NumberFormatException nfex) {
				showErrorPage(resp, "An invalid id for the job was provided.");
				return;
			}

			// remove the retained job
			submittedJobs.remove(uid);

			// redirect to the start page
			resp.sendRedirect(START_PAGE_URL);
		} else {
			showErrorPage(resp, "Invalid action specified.");
			return;
		}
	}

	/**
	 * Prints the error page, containing the given message.
	 * 
	 * @param resp
	 *        The response handler.
	 * @param message
	 *        The message to display.
	 * @throws IOException
	 *         Thrown, if the error page could not be printed due to an I/O problem.
	 */
	private void showErrorPage(HttpServletResponse resp, String message) throws IOException {
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType(GUIServletStub.CONTENT_TYPE_HTML);

		PrintWriter writer = resp.getWriter();

		writer.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"\n        \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
		writer.println("<html>");
		writer.println("<head>");
		writer.println("  <title>Launch Job - Error</title>");
		writer.println("  <meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\" />");
		writer.println("  <link rel=\"stylesheet\" type=\"text/css\" href=\"css/nephelefrontend.css\" />");
		writer.println("</head>");

		writer.println("<body>");
		writer.println("  <div class=\"mainHeading\">");
		writer.println("    <h1><img src=\"img/flink-logo.png\" width=\"100\" height=\"100\" alt=\"Flink Logo\" align=\"middle\"/>Flink Web Submission Client</h1>");
		writer.println("  </div>");
		writer.println("  <div style=\"margin-top: 50px; text-align: center;\">");
		writer.println("    <p class=\"error_text\" style=\"font-size: 18px;\">");
		writer.println(message);
		writer.println("    </p><br/><br/>");
		writer.println("    <form action=\"launch.html\" method=\"GET\">");
		writer.println("      <input type=\"submit\" value=\"back\">");
		writer.println("    </form>");
		writer.println("  </div>");
		writer.println("</body>");
		writer.println("</html>");
	}

	/**
	 * Checks the given parameter. If it is null, it prints the error page.
	 * 
	 * @param resp
	 *        The response handler.
	 * @param parameter
	 *        The parameter to check.
	 * @param parameterName
	 *        The name of the parameter, to describe it in the error message.
	 * @return True, if the parameter is null, false otherwise.
	 * @throws IOException
	 *         Thrown, if the error page could not be printed.
	 */
	private boolean checkParameterSet(HttpServletResponse resp, String parameter, String parameterName)
			throws IOException {
		if (parameter == null) {
			showErrorPage(resp, "The parameter '" + parameterName + "' is not set.");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Utility method that takes the given arguments, splits them at the whitespaces (space and tab) and
	 * turns them into an array of Strings. Other than the <tt>StringTokenizer</tt>, this method
	 * takes care of quotes, such that quoted passages end up being one string.
	 * 
	 * @param args
	 *        The string to be split.
	 * @return The array of split strings.
	 */
	private static final List<String> tokenizeArguments(String args) {
		List<String> list = new ArrayList<String>();
		StringBuilder curr = new StringBuilder();

		int pos = 0;
		boolean quoted = false;

		while (pos < args.length()) {
			char c = args.charAt(pos);
			if ((c == ' ' || c == '\t') && !quoted) {
				if (curr.length() > 0) {
					list.add(curr.toString());
					curr.setLength(0);
				}
			} else if (c == '"') {
				quoted = !quoted;
			} else {
				curr.append(c);
			}

			pos++;
		}

		if (quoted) {
			throw new IllegalArgumentException("Unterminated quoted string.");
		}

		if (curr.length() > 0) {
			list.add(curr.toString());
		}

		return list;
	}
}
