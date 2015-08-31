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
import java.util.UUID;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.common.base.Preconditions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * This class sets up a web-server that contains a web frontend to display information about running jobs.
 * It instantiates and configures an embedded jetty server.
 */
public class WebInfoServer implements WebMonitor, LeaderRetrievalListener {

	/** Web root dir in the jar */
	private static final String WEB_ROOT_DIR = "web-docs-infoserver";

	/** The log for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(WebInfoServer.class);

	/** The jetty server serving all requests. */
	private final Server server;

	/** Retrieval service for the current leading JobManager */
	private final LeaderRetrievalService leaderRetrievalService;

	/** ActorSystem used to retrieve the ActorRefs */
	private final ActorSystem actorSystem;

	/** Collection for the registered jetty handlers */
	private final HandlerCollection handlers;

	/** Associated configuration */
	private final Configuration config;

	/** Timeout for the servlets */
	private final FiniteDuration timeout;

	/** Actor look up timeout */
	private final FiniteDuration lookupTimeout;

	/** Default jetty handler responsible for serving static content */
	private final ResourceHandler resourceHandler;

	/** File paths to log dirs */
	final File[] logDirFiles;

	/** The assigned port where jetty is running. */
	private int assignedPort = -1;

	/**
	 * Creates a new web info server. The server runs the servlets that implement the logic
	 * to list all present information concerning the job manager
	 *
	 * @param config The Flink configuration.
	 * @param leaderRetrievalService Retrieval service to obtain the current leader
	 *
	 * @throws IOException
	 *         Thrown, if the server setup failed for an I/O related reason.
	 */
	public WebInfoServer(
			Configuration config,
			LeaderRetrievalService leaderRetrievalService,
			ActorSystem actorSystem)
		throws IOException {
		if (config == null) {
			throw new IllegalArgumentException("No Configuration has been passed to the web server");
		}

		this.config = config;

		this.leaderRetrievalService = Preconditions.checkNotNull(leaderRetrievalService);

		// if port == 0, jetty will assign an available port.
		int port = config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT);
		if (port < 0) {
			throw new IllegalArgumentException("Invalid port for the webserver: " + port);
		}

		timeout = AkkaUtils.getTimeout(config);
		lookupTimeout = AkkaUtils.getLookupTimeout(config);

		this.actorSystem = actorSystem;

		// get base path of Flink installation
		final String basePath = config.getString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, "");
		final String[] logDirPaths = config.getString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY,
				basePath+"/log").split(","); // YARN allows to specify multiple log directories

		URL webRootDir = this.getClass().getClassLoader().getResource(WEB_ROOT_DIR);

		if(webRootDir == null) {
			throw new FileNotFoundException("Cannot start JobManager web info server. The " +
					"resource " + WEB_ROOT_DIR + " is not included in the jar.");
		}

		logDirFiles = new File[logDirPaths.length];
		int i = 0;
		for(String path : logDirPaths) {
			logDirFiles[i++] = new File(path);
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Setting up web info server, using web-root directory " +
					webRootDir.toExternalForm()	+ ".");

		}

		server = new Server(port);

		// ----- the handler serving all the static files -----
		resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(false);
		resourceHandler.setResourceBase(webRootDir.toExternalForm());

		// ----- add the handlers to the list handler -----

		// make the HandlerCollection mutable so that we can update it later on
		handlers = new HandlerCollection(true);
		handlers.addHandler(resourceHandler);
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

		leaderRetrievalService.start(this);
	}

	/**
	 * Stop the webserver
	 */
	public void stop() throws Exception {
		leaderRetrievalService.stop();
		server.stop();
		assignedPort = -1;
	}

	public int getServerPort() {
		return this.assignedPort;
	}

	@Override
	public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {

		if(leaderAddress != null && !leaderAddress.equals("")) {
			try {
				ActorRef jobManager = AkkaUtils.getActorRef(
					leaderAddress,
					actorSystem,
					lookupTimeout);
				ActorGateway jobManagerGateway = new AkkaActorGateway(jobManager, leaderSessionID);

				Future<Object> archiveFuture = jobManagerGateway.ask(
					JobManagerMessages.getRequestArchive(),
					timeout);

				ActorRef archive = ((JobManagerMessages.ResponseArchive) Await.result(
					archiveFuture,
					timeout)).actor();

				ActorGateway archiveGateway = new AkkaActorGateway(archive, leaderSessionID);

				updateHandler(jobManagerGateway, archiveGateway);
			} catch (Exception e) {
				handleError(e);
			}
		}
	}

	@Override
	public void handleError(Exception exception) {
		LOG.error("Received error from LeaderRetrievalService.", exception);

		try{
			// stop the whole web server
			stop();
		} catch (Exception e) {
			LOG.error("Error while stopping the web server due to a LeaderRetrievalService error.", e);
		}
	}

	/**
	 * Updates the Flink handlers with the current leading JobManager and archive
	 *
	 * @param jobManager ActorGateway to the current JobManager leader
	 * @param archive ActorGateway to the current archive of the leading JobManager
	 * @throws Exception
	 */
	private void updateHandler(ActorGateway jobManager, ActorGateway archive) throws Exception {
		// ----- the handlers for the servlets -----
		ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
		servletContext.setContextPath("/");
		servletContext.addServlet(
				new ServletHolder(
						new JobManagerInfoServlet(
								jobManager,
								archive,
								timeout)),
				"/jobsInfo");
		servletContext.addServlet(
				new ServletHolder(
						new LogfileInfoServlet(
								logDirFiles)),
				"/logInfo");
		servletContext.addServlet(
				new ServletHolder(
						new SetupInfoServlet(
								config,
								jobManager,
								timeout)),
				"/setupInfo");
		servletContext.addServlet(
				new ServletHolder(
						new MenuServlet()),
				"/menu");

		// replace old handlers with new ones
		handlers.setHandlers(new Handler[]{resourceHandler, servletContext});

		// start new handler
		servletContext.start();
	}
}
