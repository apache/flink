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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * This class sets up the web-server that serves the web client. It instantiates and
 * configures an embedded jetty server.
 */
public class WebInterfaceServer {

	private static final String WEB_ROOT_DIR = "web-docs";

	/**
	 * The log for this class.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(WebInterfaceServer.class);

	/**
	 * The jetty server serving all requests.
	 */
	private final Server server;

	/**
	 * Creates a new web interface server. The server runs the servlets that implement the logic
	 * to upload, list, delete and submit jobs, to compile them and to show the optimizer plan.
	 * It serves the asynchronous requests for the plans and all other static resources, like
	 * static web pages, stylesheets or javascript files.
	 * 
	 * @param configDir
	 *        The path to the configuration directory.
	 * @param config
	 *        The configuration for the JobManager. All jobs will be sent
	 *        to the JobManager described by this configuration.
	 * @param port
	 *        The port to launch the server on.
	 * @throws Exception
	 *         Thrown, if the server setup failed.
	 */
	public WebInterfaceServer(String configDir, Configuration config, int port) throws Exception {
		// if no explicit configuration is given, use the global configuration
		if (config == null) {
			config = GlobalConfiguration.getConfiguration();
		}
		
		// get base path of Flink installation
		String basePath = config.getString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY,"");

		File tmpDir;
		File uploadDir;
		File planDumpDir;
		
		URL webRootDir = this.getClass().getClassLoader().getResource(WEB_ROOT_DIR);

		if(webRootDir == null){
			throw new FileNotFoundException("Cannot start web interface server because the web " +
					"root dir " + WEB_ROOT_DIR + " is not included in the jar.");
		}

		String tmpDirPath = config.getString(ConfigConstants.WEB_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_WEB_TMP_DIR);
		
		tmpDir = new File(tmpDirPath);
		if (!tmpDir.isAbsolute()) {
			// path relative to base dir
			tmpDir = new File(basePath+"/"+tmpDirPath);
		}
		
		String uploadDirPath = config.getString(ConfigConstants.WEB_JOB_UPLOAD_DIR_KEY,
				ConfigConstants.DEFAULT_WEB_JOB_STORAGE_DIR);
		
		uploadDir = new File(uploadDirPath);
		if (!uploadDir.isAbsolute()) {
			// path relative to base dir
			uploadDir = new File(basePath+"/"+uploadDirPath);
		}

		String planDumpDirPath = config.getString(ConfigConstants.WEB_PLAN_DUMP_DIR_KEY,
				ConfigConstants.DEFAULT_WEB_PLAN_DUMP_DIR);
		
		planDumpDir = new File(planDumpDirPath);
		if (!planDumpDir.isAbsolute()) {
			// path relative to base dir
			planDumpDir = new File(basePath+"/"+planDumpDirPath);
		}
		
		if (LOG.isInfoEnabled()) {
			LOG.info("Setting up web client server, using web-root directory '" +
					webRootDir.toExternalForm() + "'.");
			LOG.info("Web frontend server will store temporary files in '" + tmpDir.getAbsolutePath()
				+ "', uploaded jobs in '" + uploadDir.getAbsolutePath() + "', plan-json-dumps in '"
				+ planDumpDir.getAbsolutePath() + "'.");
	
			LOG.info("Web client will submit jobs to JobManager at "
				+ config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null) + ", port "
				+ config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)
				+ ".");
		}

		server = new Server(port);

		// ensure, that all the directories exist
		checkAndCreateDirectories(tmpDir, true);
		checkAndCreateDirectories(uploadDir, true);
		checkAndCreateDirectories(planDumpDir, true);
		
		int jobManagerWebPort = config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
												ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT);

		// ----- the handlers for the servlets -----
		CliFrontend cli = new CliFrontend(configDir);
		ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
		servletContext.setContextPath("/");
		servletContext.addServlet(new ServletHolder(new JobJSONServlet(uploadDir)), "/pactPlan");
		servletContext.addServlet(new ServletHolder(new PlanDisplayServlet(jobManagerWebPort)), "/showPlan");
		servletContext.addServlet(new ServletHolder(new JobsServlet(uploadDir, tmpDir, "launch.html")), "/jobs");
		servletContext.addServlet(new ServletHolder(new JobSubmissionServlet(cli, uploadDir, planDumpDir)), "/runJob");

		// ----- the hander serving the written pact plans -----
		ResourceHandler pactPlanHandler = new ResourceHandler();
		pactPlanHandler.setDirectoriesListed(false);
		pactPlanHandler.setResourceBase(planDumpDir.getAbsolutePath());
		ContextHandler pactPlanContext = new ContextHandler();
		pactPlanContext.setContextPath("/ajax-plans");
		pactPlanContext.setHandler(pactPlanHandler);

		// ----- the handler serving all the static files -----
		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(false);
		resourceHandler.setResourceBase(webRootDir.toExternalForm());

		// ----- add the handlers to the list handler -----
		HandlerList handlers = new HandlerList();
		handlers.addHandler(servletContext);
		handlers.addHandler(pactPlanContext);
		handlers.addHandler(resourceHandler);

		// ----- create the login module with http authentication -----

		File af = null;
		String authFile = config.getString(ConfigConstants.WEB_ACCESS_FILE_KEY,
			ConfigConstants.DEFAULT_WEB_ACCESS_FILE_PATH);
		if (authFile != null) {
			af = new File(authFile);
			if (!af.exists()) {
				LOG.error("The specified file '" + af.getAbsolutePath()
					+ "' with the authentication information is missing. Starting server without HTTP authentication.");
				af = null;
			}
		}
		server.setHandler(handlers);
	}

	/**
	 * Starts the web frontend server.
	 * 
	 * @throws Exception
	 *         Thrown, if the start fails.
	 */
	public void start() throws Exception {
		server.start();
	}

	/**
	 * Lets the calling thread wait until the server terminates its operation.
	 * 
	 * @throws InterruptedException
	 *         Thrown, if the calling thread is interrupted.
	 */
	public void join() throws InterruptedException {
		server.join();
	}

	/**
	 * Checks and creates the directory described by the abstract directory path. This function checks
	 * if the directory exists and creates it if necessary. It also checks read permissions and
	 * write permission, if necessary.
	 * 
	 * @param f
	 *        The file describing the directory path.
	 * @param needWritePermission
	 *        A flag indicating whether to check write access.
	 * @throws IOException
	 *         Thrown, if the directory could not be created, or if one of the checks failed.
	 */
	private void checkAndCreateDirectories(File f, boolean needWritePermission) throws IOException {
		String dir = f.getAbsolutePath();

		// check if it exists and it is not a directory
		if (f.exists() && !f.isDirectory()) {
			throw new IOException("A none directory file with the same name as the configured directory '" + dir
				+ "' already exists.");
		}

		// try to create the directory
		if (!f.exists()) {
			if (!f.mkdirs()) {
				throw new IOException("Could not create the directory '" + dir + "'.");
			}
		}

		// check the read and execute permission
		if (!(f.canRead() && f.canExecute())) {
			throw new IOException("The directory '" + dir + "' cannot be read and listed.");
		}

		// check the write permission
		if (needWritePermission && !f.canWrite()) {
			throw new IOException("No write access could be obtained on directory '" + dir + "'.");
		}
	}
}
