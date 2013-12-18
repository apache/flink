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
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobmanager.JobManager;

/**
 * This class sets up a web-server that contains a web frontend to display information about running jobs.
 * It instantiates and configures an embedded jetty server.
 */
public class WebInfoServer {
	
	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(WebInfoServer.class);
	
	/**
	 * The jetty server serving all requests.
	 */
	private final Server server;
	
	/**
	 * Port for info server
	 */
	private int port;
	
	/**
	 * Creates a new web info server. The server runs the servlets that implement the logic
	 * to list all present information concerning the job manager 
	 * 
	 * @param nepheleConfig
	 *        The configuration for the nephele job manager. 
	 * @param port
	 *        The port to launch the server on.
	 * @throws IOException
	 *         Thrown, if the server setup failed for an I/O related reason.
	 */
	public WebInfoServer(Configuration nepheleConfig, int port, JobManager jobmanager) throws IOException {
		this.port = port;
		
		// if no explicit configuration is given, use the global configuration
		if (nepheleConfig == null) {
			nepheleConfig = GlobalConfiguration.getConfiguration();
		}
		
		// get base path of Stratosphere installation
		String basePath = nepheleConfig.getString(ConfigConstants.STRATOSPHERE_BASE_DIR_PATH_KEY, "");
		String webDirPath = nepheleConfig.getString(ConfigConstants.JOB_MANAGER_WEB_ROOT_PATH_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ROOT_PATH);
		
		File webDir;
		if(webDirPath.startsWith("/")) {
			// absolute path
			webDir = new File(webDirPath);
		} else {
			// path relative to base dir
			webDir = new File(basePath+"/"+webDirPath);
		}
		
		
		if (LOG.isInfoEnabled()) {
			LOG.info("Setting up web info server, using web-root directory '" + webDir.getAbsolutePath() + "'.");
			//LOG.info("Web info server will store temporary files in '" + tmpDir.getAbsolutePath());
	
			LOG.info("Web info server will display information about nephele job-manager on "
				+ nepheleConfig.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null) + ", port "
				+ port
				+ ".");
		}

		// ensure that the directory with the web documents exists
		if (!webDir.exists()) {
			throw new FileNotFoundException("Cannot start jobmanager web info server. The directory containing the web documents does not exist: " 
				+ webDir.getAbsolutePath());
		}
		
		server = new Server(port);

		// ----- the handlers for the servlets -----
		ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
		servletContext.setContextPath("/");
		servletContext.addServlet(new ServletHolder(new JobmanagerInfoServlet(jobmanager)), "/jobsInfo");
		servletContext.addServlet(new ServletHolder(new LogfileInfoServlet(new File(basePath+"/log"))), "/logInfo");


		// ----- the handler serving all the static files -----
		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(false);
		resourceHandler.setResourceBase(webDir.getAbsolutePath());

		// ----- add the handlers to the list handler -----
		HandlerList handlers = new HandlerList();
		handlers.addHandler(servletContext);
		handlers.addHandler(resourceHandler);

		// ----- create the login module with http authentication -----

		File af = null;
		String authFile = nepheleConfig.getString(ConfigConstants.JOB_MANAGER_WEB_ACCESS_FILE_KEY, null);
		if (authFile != null) {
			af = new File(authFile);
			if (!af.exists()) {
				LOG.error("The specified file '" + af.getAbsolutePath()
					+ "' with the authentication information is missing. Starting server without HTTP authentication.");
				af = null;
			}
		}
		if (af != null) {
			HashLoginService loginService = new HashLoginService("Stratosphere Jobmanager Interface", authFile);
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
		LOG.info("Starting web info server for JobManager on port " + this.port);
		server.start();
	}

}
