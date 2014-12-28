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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import akka.actor.ActorRef;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.eclipse.jetty.http.security.Constraint;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import scala.concurrent.duration.FiniteDuration;


/**
 * This class sets up a web-server that contains a web frontend to display information about running jobs.
 * It instantiates and configures an embedded jetty server.
 */
public class WebInfoServer {

	/**
	 * Web root dir in the jar
	 */
	private static final String WEB_ROOT_DIR = "web-docs-infoserver";

	/**
	 * The log for this class.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(WebInfoServer.class);

	/**
	 * The jetty server serving all requests.
	 */
	private final Server server;

	/**
	 * Timeout for akka requests
	 */
	private final FiniteDuration timeout;

	/**
	 * Port for info server
	 */
	private int port;

	/**
	 * Creates a new web info server. The server runs the servlets that implement the logic
	 * to list all present information concerning the job manager
	 *
	 * @param config
	 *        The configuration for the flink job manager.
	 * @throws IOException
	 *         Thrown, if the server setup failed for an I/O related reason.
	 */
	public WebInfoServer(Configuration config, ActorRef jobmanager,
						ActorRef archive, FiniteDuration timeout) throws IOException {
		
		// if no explicit configuration is given, use the global configuration
		if (config == null) {
			config = GlobalConfiguration.getConfiguration();
		}
		
		this.port = config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT);
		
		this.timeout = timeout;

		// get base path of Flink installation
		final String basePath = config.getString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, "");
		final String[] logDirPaths = config.getString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY,
				basePath+"/log").split(","); // YARN allows to specify multiple log directories

		URL webRootDir = this.getClass().getClassLoader().getResource(WEB_ROOT_DIR);

		if(webRootDir == null) {
			throw new FileNotFoundException("Cannot start jobmanager web info server. The " +
					"resource " + WEB_ROOT_DIR + " is not included in the jar.");
		}

		final File[] logDirFiles = new File[logDirPaths.length];
		int i = 0;
		for(String path : logDirPaths) {
			logDirFiles[i++] = new File(path);
		}


		if (LOG.isInfoEnabled()) {
			LOG.info("Setting up web info server, using web-root directory" +
					webRootDir.toExternalForm()	+ ".");

			LOG.info("Web info server will display information about flink job-manager on "
				+ config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null) + ", port "
				+ port
				+ ".");
		}

		server = new Server(port);

		// ----- the handlers for the servlets -----
		ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
		servletContext.setContextPath("/");
		servletContext.addServlet(new ServletHolder(new JobmanagerInfoServlet(jobmanager,
				archive, timeout)), "/jobsInfo");
		servletContext.addServlet(new ServletHolder(new LogfileInfoServlet(logDirFiles)), "/logInfo");
		servletContext.addServlet(new ServletHolder(new SetupInfoServlet(jobmanager, timeout)),
				"/setupInfo");
		servletContext.addServlet(new ServletHolder(new MenuServlet()), "/menu");


		// ----- the handler serving all the static files -----
		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(false);
		resourceHandler.setResourceBase(webRootDir.toExternalForm());

		// ----- add the handlers to the list handler -----
		HandlerList handlers = new HandlerList();
		handlers.addHandler(servletContext);
		handlers.addHandler(resourceHandler);

		// ----- create the login module with http authentication -----

		File af = null;
		String authFile = config.getString(ConfigConstants.JOB_MANAGER_WEB_ACCESS_FILE_KEY, null);
		if (authFile != null) {
			af = new File(authFile);
			if (!af.exists()) {
				LOG.error("The specified file '" + af.getAbsolutePath()
					+ "' with the authentication information is missing. Starting server without HTTP authentication.");
				af = null;
			}
		}
		if (af != null) {
			HashLoginService loginService = new HashLoginService("Flink Jobmanager Interface", authFile);
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

	/**
	 * Stop the webserver
	 */
	public void stop() throws Exception {
		server.stop();
	}

}
