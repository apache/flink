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

package eu.stratosphere.client.web;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.http.security.Constraint;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.configuration.PactConfigConstants;

/**
 * This class sets up the web-server that serves the web frontend. It instantiates and
 * configures an embedded jetty server.
 */
public class WebInterfaceServer {
	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(WebInterfaceServer.class);

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
	 * @param nepheleConfig
	 *        The configuration for the nephele job manager. All compiled jobs will be sent
	 *        to the manager described by this configuration.
	 * @param port
	 *        The port to launch the server on.
	 * @throws IOException
	 *         Thrown, if the server setup failed for an I/O related reason.
	 */
	public WebInterfaceServer(Configuration nepheleConfig, int port)
																	throws IOException {
		Configuration config = GlobalConfiguration.getConfiguration();

		// if no explicit configuration is given, use the global configuration
		if (nepheleConfig == null) {
			nepheleConfig = config;
		}
		
		// get base path of Stratosphere installation
		String basePath = nepheleConfig.getString(PactConfigConstants.STRATOSPHERE_BASE_DIR_PATH_KEY,"");

		File webDir;
		File tmpDir;
		File uploadDir;
		File planDumpDir;
		
		String webDirPath = config.getString(PactConfigConstants.WEB_ROOT_PATH_KEY,
				PactConfigConstants.DEFAULT_WEB_ROOT_DIR);
		
		if(webDirPath.startsWith("/")) {
			// absolute path
			webDir = new File(webDirPath);
		} else {
			// path relative to base dir
			webDir = new File(basePath+"/"+webDirPath);
		}
		
		String tmpDirPath = config.getString(PactConfigConstants.WEB_TMP_DIR_KEY,
				PactConfigConstants.DEFAULT_WEB_TMP_DIR);
		
		tmpDir = new File(tmpDirPath);
		if(tmpDir.isAbsolute()) {
			// absolute path, everything all right
		} else {
			// path relative to base dir
			tmpDir = new File(basePath+"/"+tmpDirPath);
		}
		
		String uploadDirPath = config.getString(PactConfigConstants.WEB_JOB_UPLOAD_DIR_KEY,
				PactConfigConstants.DEFAULT_WEB_JOB_STORAGE_DIR);
		
		uploadDir = new File(uploadDirPath);
		if(uploadDir.isAbsolute()) {
			// absolute path, everything peachy
		} else {
			// path relative to base dir
			uploadDir = new File(basePath+"/"+uploadDirPath);
		}

		String planDumpDirPath = config.getString(PactConfigConstants.WEB_PLAN_DUMP_DIR_KEY,
				PactConfigConstants.DEFAULT_WEB_PLAN_DUMP_DIR);
		
		planDumpDir = new File(planDumpDirPath);
		if(planDumpDir.isAbsolute()) {
			// absolute path, nice and dandy
		} else {
			// path relative to base dir
			planDumpDir = new File(basePath+"/"+planDumpDirPath);
		}
		
		if (LOG.isInfoEnabled()) {
			LOG.info("Setting up web frontend server, using web-root directory '" + webDir.getAbsolutePath() + "'.");
			LOG.info("Web frontend server will store temporary files in '" + tmpDir.getAbsolutePath()
				+ "', uploaded jobs in '" + uploadDir.getAbsolutePath() + "', plan-json-dumps in '"
				+ planDumpDir.getAbsolutePath() + "'.");
	
			LOG.info("Web-frontend will submit jobs to nephele job-manager on "
				+ config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null) + ", port "
				+ config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)
				+ ".");
		}

		server = new Server(port);

		// ensure that the directory with the web documents exists
		if (!webDir.exists()) {
			throw new FileNotFoundException("The directory containing the web documents does not exist: "
				+ webDir.getAbsolutePath());
		}

		// ensure, that all the directories exist
		checkAndCreateDirectories(tmpDir, true);
		checkAndCreateDirectories(uploadDir, true);
		checkAndCreateDirectories(planDumpDir, true);
		
		int jobManagerWebPort = config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_WEB_FRONTEND_PORT);

		// ----- the handlers for the servlets -----
		ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
		servletContext.setContextPath("/");
		servletContext.addServlet(new ServletHolder(new PactJobJSONServlet(uploadDir)), "/pactPlan");
		servletContext.addServlet(new ServletHolder(new JobsInfoServlet(nepheleConfig)), "/jobsInfo");
		servletContext.addServlet(new ServletHolder(new PlanDisplayServlet(jobManagerWebPort)), "/showPlan");
		servletContext.addServlet(new ServletHolder(new JobsServlet(uploadDir, tmpDir, "launch.html")), "/jobs");
		servletContext.addServlet(new ServletHolder(new JobSubmissionServlet(nepheleConfig, uploadDir, planDumpDir)),
			"/runJob");

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
		resourceHandler.setResourceBase(webDir.getAbsolutePath());

		// ----- add the handlers to the list handler -----
		HandlerList handlers = new HandlerList();
		handlers.addHandler(servletContext);
		handlers.addHandler(pactPlanContext);
		handlers.addHandler(resourceHandler);

		// ----- create the login module with http authentication -----

		File af = null;
		String authFile = config.getString(PactConfigConstants.WEB_ACCESS_FILE_KEY,
			PactConfigConstants.DEFAULT_WEB_ACCESS_FILE_PATH);
		if (authFile != null) {
			af = new File(authFile);
			if (!af.exists()) {
				LOG.error("The specified file '" + af.getAbsolutePath()
					+ "' with the authentication information is missing. Starting server without HTTP authentication.");
				af = null;
			}
		}
		if (af != null) {
			HashLoginService loginService = new HashLoginService("Stratosphere Query Engine Interface", authFile);
			server.addBean(loginService);

			Constraint constraint = new Constraint();
			constraint.setName(Constraint.__BASIC_AUTH);
			constraint.setAuthenticate(true);
			constraint.setRoles(new String[] { "user" });

			ConstraintMapping mapping = new ConstraintMapping();
			mapping.setPathSpec("/*");
			mapping.setConstraint(constraint);

			ConstraintSecurityHandler sh = new ConstraintSecurityHandler();
			sh.addConstraintMapping(mapping);
			sh.setAuthenticator(new BasicAuthenticator());
			sh.setLoginService(loginService);
			sh.setStrict(true);

			// set the handers: the server hands the request to the security handler,
			// which hands the request to the other handlers when authenticated
			sh.setHandler(handlers);
			server.setHandler(sh);
		} else {
			server.setHandler(handlers);
		}
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
	 * @param dir
	 *        The String describing the directory path.
	 * @param needWritePermission
	 *        A flag indicating whether to check write access.
	 * @throws IOException
	 *         Thrown, if the directory could not be created, or if one of the checks failed.
	 */
	private final void checkAndCreateDirectories(File f, boolean needWritePermission) throws IOException {
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
