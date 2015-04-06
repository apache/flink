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

import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

/**
 * This class sets up a web-server that contains a web frontend to display information about running jobs.
 * It instantiates and configures an embedded jetty server.
 */
public class WebInfoServer {

	/** Web root dir in the jar */
	private static final String WEB_ROOT_DIR = "web-docs-infoserver";

	/** The log for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(WebInfoServer.class);

	/** The jetty server serving all requests. */
	private final Server server;

	/** The assigned port where jetty is running. */
	private int assignedPort = -1;

	/**
	 * Creates a new web info server. The server runs the servlets that implement the logic
	 * to list all present information concerning the job manager
	 *
	 * @param config The Flink configuration.
	 * @param jobmanager The ActorRef to the JobManager actor
	 * @param archive The ActorRef to the archive for old jobs
	 *
	 * @throws IOException
	 *         Thrown, if the server setup failed for an I/O related reason.
	 */
	public WebInfoServer(Configuration config, ActorRef jobmanager, ActorRef archive) throws IOException {
		if (config == null) {
			throw new IllegalArgumentException("No Configuration has been passed to the web server");
		}
		if (jobmanager == null || archive == null) {
			throw new NullPointerException();
		}

		// if port == 0, jetty will assign an available port.
		int port = config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT);
		if (port < 0) {
			throw new IllegalArgumentException("Invalid port for the webserver: " + port);
		}

		final FiniteDuration timeout = AkkaUtils.getTimeout(config);

		// get base path of Flink installation
		final String basePath = config.getString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, "");
		final String[] logDirPaths = config.getString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY,
				basePath+"/log").split(","); // YARN allows to specify multiple log directories

		URL webRootDir = this.getClass().getClassLoader().getResource(WEB_ROOT_DIR);

		if(webRootDir == null) {
			throw new FileNotFoundException("Cannot start JobManager web info server. The " +
					"resource " + WEB_ROOT_DIR + " is not included in the jar.");
		}

		final File[] logDirFiles = new File[logDirPaths.length];
		int i = 0;
		for(String path : logDirPaths) {
			logDirFiles[i++] = new File(path);
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Setting up web info server, using web-root directory " +
					webRootDir.toExternalForm()	+ ".");

		}

		server = new Server(port);

		// ----- the handlers for the servlets -----
		ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
		servletContext.setContextPath("/");
		servletContext.addServlet(new ServletHolder(new JobManagerInfoServlet(jobmanager, archive, timeout)), "/jobsInfo");
		servletContext.addServlet(new ServletHolder(new LogfileInfoServlet(logDirFiles)), "/logInfo");
		servletContext.addServlet(new ServletHolder(new SetupInfoServlet(config, jobmanager, timeout)), "/setupInfo");
		servletContext.addServlet(new ServletHolder(new MenuServlet()), "/menu");


		// ----- the handler serving all the static files -----
		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(false);
		resourceHandler.setResourceBase(webRootDir.toExternalForm());

		// ----- add the handlers to the list handler -----
		HandlerList handlers = new HandlerList();
		handlers.addHandler(resourceHandler);
		handlers.addHandler(servletContext);
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
		
		final Connector[] connectors = server.getConnectors();
		if (connectors != null && connectors.length > 0) {
			Connector conn = connectors[0];

			// we have to use getLocalPort() instead of getPort() http://stackoverflow.com/questions/8884865/how-to-discover-jetty-7-running-port
			this.assignedPort = conn.getLocalPort(); 
			String host = conn.getHost();
			if (host == null) { // as per method documentation
				host = "0.0.0.0";
			}
			LOG.info("Started web info server for JobManager on {}:{}", host, assignedPort);
		}
		else {
			LOG.warn("Unable to determine local endpoint of web frontend server");
		}
	}

	/**
	 * Stop the webserver
	 */
	public void stop() throws Exception {
		server.stop();
		assignedPort = -1;
	}

	public int getServerPort() {
		return this.assignedPort;
	}
}
