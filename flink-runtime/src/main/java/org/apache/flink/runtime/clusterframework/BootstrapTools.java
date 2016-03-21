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

package org.apache.flink.runtime.clusterframework;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import com.typesafe.config.Config;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;

import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.Iterator;

/**
 * Tools for starting JobManager and TaskManager processes, including the
 * Actor Systems used to run the JobManager and TaskManager actors.
 */
public class BootstrapTools {

	/**
	 * Starts an ActorSystem with the given configuration listening at the address/ports.
	 * @param configuration The Flink configuration
	 * @param listeningAddress The address to listen at.
	 * @param portRangeDefinition The port range to choose a port from.
	 * @param logger The logger to output log information.
	 * @return The ActorSystem which has been started
	 * @throws Exception
	 */
	public static ActorSystem startActorSystem(
				Configuration configuration,
				String listeningAddress,
				String portRangeDefinition,
				Logger logger) throws Exception {

		// parse port range definition and create port iterator
		Iterator<Integer> portsIterator;
		try {
			portsIterator = NetUtils.getPortRangeFromString(portRangeDefinition);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid port range definition: " + portRangeDefinition);
		}

		while (portsIterator.hasNext()) {
			// first, we check if the port is available by opening a socket
			// if the actor system fails to start on the port, we try further
			ServerSocket availableSocket = NetUtils.createSocketFromPorts(
				portsIterator,
				new NetUtils.SocketFactory() {
					@Override
					public ServerSocket createSocket(int port) throws IOException {
						return new ServerSocket(port);
					}
				});

			int port;
			if (availableSocket == null) {
				throw new BindException("Unable to allocate further port in port range: " + portRangeDefinition);
			} else {
				port = availableSocket.getLocalPort();
				try {
					availableSocket.close();
				} catch (IOException ignored) {}
			}

			try {
				return startActorSystem(configuration, listeningAddress, port, logger);
			}
			catch (Exception e) {
				// we can continue to try if this contains a netty channel exception
				Throwable cause = e.getCause();
				if (!(cause instanceof org.jboss.netty.channel.ChannelException ||
						cause instanceof java.net.BindException)) {
					throw e;
				} // else fall through the loop and try the next port
			}
		}

		// if we come here, we have exhausted the port range
		throw new BindException("Could not start actor system on any port in port range "
			+ portRangeDefinition);
	}

	/**
	 * Starts an Actor System at a specific port.
	 * @param configuration The Flink configuration.
	 * @param listeningAddress The address to listen at.
	 * @param listeningPort The port to listen at.
	 * @param logger the logger to output log information.
	 * @return The ActorSystem which has been started.
	 * @throws Exception
	 */
	public static ActorSystem startActorSystem(
				Configuration configuration,
				String listeningAddress,
				int listeningPort,
				Logger logger) throws Exception {

		String hostPortUrl = NetUtils.hostAndPortToUrlString(listeningAddress, listeningPort);
		logger.info("Trying to start actor system at {}", hostPortUrl);

		try {
			Config akkaConfig = AkkaUtils.getAkkaConfig(
				configuration,
				new scala.Some<>(new scala.Tuple2<String, Object>(listeningAddress, listeningPort))
			);

			logger.debug("Using akka configuration\n {}", akkaConfig);

			ActorSystem actorSystem = AkkaUtils.createActorSystem(akkaConfig);
			logger.info("Actor system started at {}", hostPortUrl);
			return actorSystem;
		}
		catch (Throwable t) {
			if (t instanceof org.jboss.netty.channel.ChannelException) {
				Throwable cause = t.getCause();
				if (cause != null && t.getCause() instanceof java.net.BindException) {
					throw new IOException("Unable to create ActorSystem at address " + hostPortUrl +
							" : " + cause.getMessage(), t);
				}
			}
			throw new Exception("Could not create actor system", t);
		}
	}

	/**
	 * Starts the web frontend.
	 * @param config The Flink config.
	 * @param actorSystem The ActorSystem to start the web frontend in.
	 * @param logger Logger for log output
	 * @return WebMonitor instance.
	 * @throws Exception
	 */
	public static WebMonitor startWebMonitorIfConfigured(
				Configuration config,
				ActorSystem actorSystem,
				ActorRef jobManager,
				Logger logger) throws Exception {


		// this ensures correct values are present in the web frontend
		final Address address = AkkaUtils.getAddress(actorSystem);
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, address.host().get());
		config.setString(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, address.port().get().toString());

		if (config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0) >= 0) {
			logger.info("Starting JobManager Web Frontend");

			LeaderRetrievalService leaderRetrievalService = 
				LeaderRetrievalUtils.createLeaderRetrievalService(config, jobManager);

			// start the web frontend. we need to load this dynamically
			// because it is not in the same project/dependencies
			WebMonitor monitor = WebMonitorUtils.startWebRuntimeMonitor(
				config, leaderRetrievalService, actorSystem);

			// start the web monitor
			if (monitor != null) {
				String jobManagerAkkaURL = AkkaUtils.getAkkaURL(actorSystem, jobManager);
				monitor.start(jobManagerAkkaURL);
			}
			return monitor;
		}
		else {
			return null;
		}
	}

	/**
	 * Generate a task manager configuration.
	 * @param baseConfig Config to start from.
	 * @param jobManagerHostname Job manager host name.
	 * @param jobManagerPort Port of the job manager.
	 * @param numSlots Number of slots to configure.
	 * @param registrationTimeout Timeout for registration
	 * @return TaskManager configuration
	 */
	public static Configuration generateTaskManagerConfiguration(
				Configuration baseConfig,
				String jobManagerHostname,
				int jobManagerPort,
				int numSlots,
				FiniteDuration registrationTimeout) {

		Configuration cfg = baseConfig.clone();

		cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerHostname);
		cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort);
		cfg.setString(ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION, registrationTimeout.toString());
		if (numSlots != -1){
			cfg.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots);
		}

		return cfg; 
	}

	/**
	 * Writes a Flink YAML config file from a Flink Configuration object.
	 * @param cfg The Flink config
	 * @param file The File to write to
	 * @throws IOException
	 */
	public static void writeConfiguration(Configuration cfg, File file) throws IOException {
		try (FileWriter fwrt = new FileWriter(file);
			PrintWriter out = new PrintWriter(fwrt))
		{
			for (String key : cfg.keySet()) {
				String value = cfg.getString(key, null);
				out.print(key);
				out.print(": ");
				out.println(value);
			}
		}
	}

	/**
	* Sets the value of a new config key to the value of a deprecated config key.
	* @param config Config to write
	* @param deprecated The old config key
	* @param designated The new config key
	*/
	public static void substituteDeprecatedConfigKey(Configuration config, String deprecated, String designated) {
		// set the designated key only if it is not set already
		if (!config.containsKey(designated)) {
			final String valueForDeprecated = config.getString(deprecated, null);
			if (valueForDeprecated != null) {
				config.setString(designated, valueForDeprecated);
			}
		}
	}

	/**
	* Sets the value of of a new config key to the value of a deprecated config key. Taking into
	* account the changed prefix.
	* @param config Config to write
	* @param deprecatedPrefix Old prefix of key
	* @param designatedPrefix New prefix of key
	*/
	public static void substituteDeprecatedConfigPrefix(
			Configuration config,
			String deprecatedPrefix,
			String designatedPrefix) {

		// set the designated key only if it is not set already
		final int prefixLen = deprecatedPrefix.length();

		Configuration replacement = new Configuration();

		for (String key : config.keySet()) {
			if (key.startsWith(deprecatedPrefix)) {
				String newKey = designatedPrefix + key.substring(prefixLen);
				if (!config.containsKey(newKey)) {
					replacement.setString(newKey, config.getString(key, null));
				}
			}
		}

		config.addAll(replacement);
	}

	/**
	 * Generates the shell command to start a task manager.
	 * @param flinkConfig The Flink configuration.
	 * @param tmParams Paramaters for the task manager.
	 * @param configDirectory The configuration directory for the flink-conf.yaml
	 * @param logDirectory The log directory.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @param mainClass The main class to start with.
	 * @return A String containing the task manager startup command.
	 */
	public static String getTaskManagerShellCommand(
			Configuration flinkConfig,
			ContaineredTaskManagerParameters tmParams,
			String configDirectory,
			String logDirectory,
			boolean hasLogback,
			boolean hasLog4j,
			Class<?> mainClass) {

		StringBuilder tmCommand = new StringBuilder("$JAVA_HOME/bin/java");
		tmCommand.append(" -Xms").append(tmParams.taskManagerHeapSizeMB()).append("m");
		tmCommand.append(" -Xmx").append(tmParams.taskManagerHeapSizeMB()).append("m");
		tmCommand.append(" -XX:MaxDirectMemorySize=").append(tmParams.taskManagerDirectMemoryLimitMB()).append("m");

		String  javaOpts = flinkConfig.getString(ConfigConstants.FLINK_JVM_OPTIONS, "");
		tmCommand.append(' ').append(javaOpts);

		if (hasLogback || hasLog4j) {
			tmCommand.append(" -Dlog.file=").append(logDirectory).append("/taskmanager.log");
			if (hasLogback) {
				tmCommand.append(" -Dlogback.configurationFile=file:")
						.append(configDirectory).append("/logback.xml");
			}
			if (hasLog4j) {
				tmCommand.append(" -Dlog4j.configuration=file:")
						.append(configDirectory).append("/log4j.properties");
			}
		}

		tmCommand.append(' ').append(mainClass.getName());
		tmCommand.append(" --configDir ").append(configDirectory);
		tmCommand.append(" 1> ").append(logDirectory).append("/taskmanager.out");
		tmCommand.append(" 2> ").append(logDirectory).append("/taskmanager.err");

		return tmCommand.toString();
	}


	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation */
	private BootstrapTools() {}
}
