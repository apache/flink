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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Tools for starting JobManager and TaskManager processes, including the
 * Actor Systems used to run the JobManager and TaskManager actors.
 */
public class BootstrapTools {
	private static final Logger LOG = LoggerFactory.getLogger(BootstrapTools.class);

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

		String hostPortUrl = listeningAddress + ':' + listeningPort;
		logger.info("Trying to start actor system at {}", hostPortUrl);

		try {
			Config akkaConfig = AkkaUtils.getAkkaConfig(
				configuration,
				new scala.Some<>(new scala.Tuple2<String, Object>(listeningAddress, listeningPort))
			);

			logger.debug("Using akka configuration\n {}", akkaConfig);

			ActorSystem actorSystem = AkkaUtils.createActorSystem(akkaConfig);

			logger.info("Actor system started at {}", AkkaUtils.getAddress(actorSystem));
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
	 *
	 * @param config The Flink config.
	 * @param highAvailabilityServices Service factory for high availability services
	 * @param actorSystem The ActorSystem to start the web frontend in.
	 * @param logger Logger for log output
	 * @return WebMonitor instance.
	 * @throws Exception
	 */
	public static WebMonitor startWebMonitorIfConfigured(
			Configuration config,
			HighAvailabilityServices highAvailabilityServices,
			ActorSystem actorSystem,
			ActorRef jobManager,
			Logger logger) throws Exception {


		// this ensures correct values are present in the web frontend
		final Address address = AkkaUtils.getAddress(actorSystem);
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, address.host().get());
		config.setString(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, address.port().get().toString());

		if (config.getInteger(JobManagerOptions.WEB_PORT.key(), 0) >= 0) {
			logger.info("Starting JobManager Web Frontend");

			// start the web frontend. we need to load this dynamically
			// because it is not in the same project/dependencies
			WebMonitor monitor = WebMonitorUtils.startWebRuntimeMonitor(
				config,
				highAvailabilityServices,
				actorSystem);

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

	private static final String DYNAMIC_PROPERTIES_OPT = "D";

	/**
	 * Get an instance of the dynamic properties option.
	 *
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.network.memory.min=536346624</tt>
     */
	public static Option newDynamicPropertiesOption() {
		return new Option(DYNAMIC_PROPERTIES_OPT, true, "Dynamic properties");
	}

	/**
	 * Parse the dynamic properties (passed on the command line).
	 */
	public static Configuration parseDynamicProperties(CommandLine cmd) {
		final Configuration config = new Configuration();

		String[] values = cmd.getOptionValues(DYNAMIC_PROPERTIES_OPT);
		if(values != null) {
			for(String value : values) {
				String[] pair = value.split("=", 2);
				if(pair.length == 1) {
					config.setString(pair[0], Boolean.TRUE.toString());
				}
				else if(pair.length == 2) {
					config.setString(pair[0], pair[1]);
				}
			}
		}

		return config;
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
			boolean hasKrb5,
			Class<?> mainClass) {

		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");

		ArrayList<String> params = new ArrayList<>();
		params.add(String.format("-Xms%dm", tmParams.taskManagerHeapSizeMB()));
		params.add(String.format("-Xmx%dm", tmParams.taskManagerHeapSizeMB()));

		if (tmParams.taskManagerDirectMemoryLimitMB() >= 0) {
			params.add(String.format("-XX:MaxDirectMemorySize=%dm",
				tmParams.taskManagerDirectMemoryLimitMB()));
		}

		startCommandValues.put("jvmmem", StringUtils.join(params, ' '));

		String javaOpts = flinkConfig.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (flinkConfig.getString(CoreOptions.FLINK_TM_JVM_OPTIONS).length() > 0) {
			javaOpts += " " + flinkConfig.getString(CoreOptions.FLINK_TM_JVM_OPTIONS);
		}
		//applicable only for YarnMiniCluster secure test run
		//krb5.conf file will be available as local resource in JM/TM container
		if(hasKrb5) {
			javaOpts += " -Djava.security.krb5.conf=krb5.conf";
		}
		startCommandValues.put("jvmopts", javaOpts);

		String logging = "";
		if (hasLogback || hasLog4j) {
			logging = "-Dlog.file=" + logDirectory + "/taskmanager.log";
			if (hasLogback) {
				logging +=
					" -Dlogback.configurationFile=file:" + configDirectory +
						"/logback.xml";
			}
			if (hasLog4j) {
				logging += " -Dlog4j.configuration=file:" + configDirectory +
					"/log4j.properties";
			}
		}

		startCommandValues.put("logging", logging);
		startCommandValues.put("class", mainClass.getName());
		startCommandValues.put("redirects",
			"1> " + logDirectory + "/taskmanager.out " +
			"2> " + logDirectory + "/taskmanager.err");
		startCommandValues.put("args", "--configDir " + configDirectory);

		final String commandTemplate = flinkConfig
			.getString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
				ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE);
		String startCommand = getStartCommand(commandTemplate, startCommandValues);
		LOG.debug("TaskManager start command: " + startCommand);

		return startCommand;
	}


	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation */
	private BootstrapTools() {}

	/**
	 * Replaces placeholders in the template start command with values from
	 * <tt>startCommandValues</tt>.
	 *
	 * <p>If the default template {@link ConfigConstants#DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE}
	 * is used, the following keys must be present in the map or the resulting
	 * command will still contain placeholders:
	 * <ul>
	 * <li><tt>java</tt> = path to the Java executable</li>
	 * <li><tt>jvmmem</tt> = JVM memory limits and tweaks</li>
	 * <li><tt>jvmopts</tt> = misc options for the Java VM</li>
	 * <li><tt>logging</tt> = logging-related configuration settings</li>
	 * <li><tt>class</tt> = main class to execute</li>
	 * <li><tt>args</tt> = arguments for the main class</li>
	 * <li><tt>redirects</tt> = output redirects</li>
	 * </ul>
	 * </p>
	 *
	 * @param template
	 * 		a template start command with placeholders
	 * @param startCommandValues
	 * 		a replacement map <tt>placeholder -&gt; value</tt>
	 *
	 * @return the start command with placeholders filled in
	 */
	public static String getStartCommand(String template,
		Map<String, String> startCommandValues) {
		for (Map.Entry<String, String> variable : startCommandValues
			.entrySet()) {
			template = template
				.replace("%" + variable.getKey() + "%", variable.getValue());
		}
		return template;
	}
}
