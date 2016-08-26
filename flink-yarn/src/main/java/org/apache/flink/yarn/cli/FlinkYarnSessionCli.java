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
package org.apache.flink.yarn.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.HashMap;

import static org.apache.flink.client.cli.CliFrontendParser.ADDRESS_OPTION;
import static org.apache.flink.configuration.ConfigConstants.HA_ZOOKEEPER_NAMESPACE_KEY;

/**
 * Class handling the command line interface to the YARN session.
 */
public class FlinkYarnSessionCli implements CustomCommandLine<YarnClusterClient> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnSessionCli.class);

	//------------------------------------ Constants   -------------------------

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

	private static final int CLIENT_POLLING_INTERVALL = 3;

	/** The id for the CommandLine interface */
	private static final String ID = "yarn-cluster";

	// YARN-session related constants
	private static final String YARN_APP_INI = "yarn-app.ini";
	static final String YARN_APPLICATION_ID_KEY = "applicationID";
	private static final String YARN_PROPERTIES_PARALLELISM = "parallelism";
	private static final String YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING = "dynamicPropertiesString";
	private static final String YARN_PROPERTIES_SECURE_COOKIE = "secureCookie";
	private static final String YARN_LATEST_ENTRY_SECTION_NAME = "latest";

	private static final String YARN_DYNAMIC_PROPERTIES_SEPARATOR = "@@"; // this has to be a regex for String.split()

	//------------------------------------ Command Line argument options -------------------------
	// the prefix transformation is used by the CliFrontend static constructor.
	private final Option QUERY;
	// --- or ---
	private final Option APPLICATION_ID;
	// --- or ---
	private final Option QUEUE;
	private final Option SHIP_PATH;
	private final Option FLINK_JAR;
	private final Option JM_MEMORY;
	private final Option TM_MEMORY;
	private final Option CONTAINER;
	private final Option SLOTS;
	private final Option DETACHED;
	private final Option ZOOKEEPER_NAMESPACE;
	@Deprecated
	private final Option STREAMING;
	private final Option NAME;

	private final Options ALL_OPTIONS;

	private final Option SECURE_COOKIE_OPTION;

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 *  -D fs.overwrite-files=true  -D taskmanager.network.numberOfBuffers=16368
	 */
	private final Option DYNAMIC_PROPERTIES;

	private final boolean acceptInteractiveInput;

	//------------------------------------ Internal fields -------------------------
	private YarnClusterClient yarnCluster;
	private boolean detachedMode = false;

	public FlinkYarnSessionCli(String shortPrefix, String longPrefix) {
		this(shortPrefix, longPrefix, true);
	}

	public FlinkYarnSessionCli(String shortPrefix, String longPrefix, boolean acceptInteractiveInput) {
		this.acceptInteractiveInput = acceptInteractiveInput;

		QUERY = new Option(shortPrefix + "q", longPrefix + "query", false, "Display available YARN resources (memory, cores)");
		APPLICATION_ID = new Option(shortPrefix + "id", longPrefix + "applicationId", true, "Attach to running YARN session");
		QUEUE = new Option(shortPrefix + "qu", longPrefix + "queue", true, "Specify YARN queue.");
		SHIP_PATH = new Option(shortPrefix + "t", longPrefix + "ship", true, "Ship files in the specified directory (t for transfer)");
		FLINK_JAR = new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
		JM_MEMORY = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container [in MB]");
		TM_MEMORY = new Option(shortPrefix + "tm", longPrefix + "taskManagerMemory", true, "Memory per TaskManager Container [in MB]");
		CONTAINER = new Option(shortPrefix + "n", longPrefix + "container", true, "Number of YARN container to allocate (=Number of Task Managers)");
		SLOTS = new Option(shortPrefix + "s", longPrefix + "slots", true, "Number of slots per TaskManager");
		DYNAMIC_PROPERTIES = new Option(shortPrefix + "D", true, "Dynamic properties");
		DETACHED = new Option(shortPrefix + "d", longPrefix + "detached", false, "Start detached");
		STREAMING = new Option(shortPrefix + "st", longPrefix + "streaming", false, "Start Flink in streaming mode");
		NAME = new Option(shortPrefix + "nm", longPrefix + "name", true, "Set a custom name for the application on YARN");
		ZOOKEEPER_NAMESPACE = new Option(shortPrefix + "z", longPrefix + "zookeeperNamespace", true, "Namespace to create the Zookeeper sub-paths for high availability mode");
		SECURE_COOKIE_OPTION = new Option("k", "cookie", true,"String to authorize Akka-based RPC communication");

		ALL_OPTIONS = new Options();
		ALL_OPTIONS.addOption(FLINK_JAR);
		ALL_OPTIONS.addOption(JM_MEMORY);
		ALL_OPTIONS.addOption(TM_MEMORY);
		ALL_OPTIONS.addOption(CONTAINER);
		ALL_OPTIONS.addOption(QUEUE);
		ALL_OPTIONS.addOption(QUERY);
		ALL_OPTIONS.addOption(SHIP_PATH);
		ALL_OPTIONS.addOption(SLOTS);
		ALL_OPTIONS.addOption(DYNAMIC_PROPERTIES);
		ALL_OPTIONS.addOption(DETACHED);
		ALL_OPTIONS.addOption(STREAMING);
		ALL_OPTIONS.addOption(NAME);
		ALL_OPTIONS.addOption(APPLICATION_ID);
		ALL_OPTIONS.addOption(ZOOKEEPER_NAMESPACE);
		ALL_OPTIONS.addOption(SECURE_COOKIE_OPTION);
	}


	/**
	 * Tries to load a Flink Yarn properties file and returns the Yarn application id if successful
	 * @param cmdLine The command-line parameters
	 * @param flinkConfiguration The flink configuration
	 * @return Yarn application id or null if none could be retrieved
	 */
	private String loadYarnPropertiesFile(CommandLine cmdLine, Configuration flinkConfiguration) {

		String jobManagerOption = cmdLine.getOptionValue(ADDRESS_OPTION.getOpt(), null);
		if (jobManagerOption != null) {
			// don't resume from properties file if a JobManager has been specified
			return null;
		}

		for (Option option : cmdLine.getOptions()) {
			if (ALL_OPTIONS.hasOption(option.getOpt())) {
				if (!option.getOpt().equals(DETACHED.getOpt())) {
					// don't resume from properties file if yarn options have been specified
					return null;
				}
			}
		}

		YarnAppState appState = retrieveMostRecentYarnApp();
		if (appState == null) { return null; }

		String applicationID = appState.getApplicationId();
		try {
			// try converting id to ApplicationId
			ConverterUtils.toApplicationId(applicationID);
		}
		catch (Exception e) {
			throw new RuntimeException("YARN properties contains an invalid entry for " +
					"application id: " + applicationID, e);
		}
		logAndSysout("Using Yarn application id from YARN properties " + applicationID);

		// configure the default parallelism from YARN
		String propParallelism = appState.getParallelism();
		if (propParallelism != null) { // maybe the property is not set
			try {
				int parallelism = Integer.parseInt(propParallelism);
				flinkConfiguration.setInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, parallelism);
				logAndSysout("YARN properties set default parallelism to " + parallelism);
			}
			catch (NumberFormatException e) {
				throw new RuntimeException("Error while parsing the YARN properties: " +
						"Property " + YARN_PROPERTIES_PARALLELISM + " is not an integer.");
			}
		}

		// handle the YARN client's dynamic properties
		String dynamicPropertiesEncoded = appState.getDynamicProperties();
		Map<String, String> dynamicProperties = getDynamicProperties(dynamicPropertiesEncoded);
		for (Map.Entry<String, String> dynamicProperty : dynamicProperties.entrySet()) {
			flinkConfiguration.setString(dynamicProperty.getKey(), dynamicProperty.getValue());
		}

		return applicationID;
	}

	public AbstractYarnClusterDescriptor createDescriptor(String defaultApplicationName, CommandLine cmd) {

		AbstractYarnClusterDescriptor yarnClusterDescriptor = getClusterDescriptor();

		if (!cmd.hasOption(CONTAINER.getOpt())) { // number of containers is required option!
			LOG.error("Missing required argument {}", CONTAINER.getOpt());
			printUsage();
			throw new IllegalArgumentException("Missing required argument " + CONTAINER.getOpt());
		}
		yarnClusterDescriptor.setTaskManagerCount(Integer.valueOf(cmd.getOptionValue(CONTAINER.getOpt())));

		// Jar Path
		Path localJarPath;
		if (cmd.hasOption(FLINK_JAR.getOpt())) {
			String userPath = cmd.getOptionValue(FLINK_JAR.getOpt());
			if (!userPath.startsWith("file://")) {
				userPath = "file://" + userPath;
			}
			localJarPath = new Path(userPath);
		} else {
			LOG.info("No path for the flink jar passed. Using the location of "
				+ yarnClusterDescriptor.getClass() + " to locate the jar");
			String encodedJarPath =
				yarnClusterDescriptor.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
			try {
				// we have to decode the url encoded parts of the path
				String decodedPath = URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
				localJarPath = new Path(new File(decodedPath).toURI());
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Couldn't decode the encoded Flink dist jar path: " + encodedJarPath +
					" Please supply a path manually via the -" + FLINK_JAR.getOpt() + " option.");
			}
		}

		yarnClusterDescriptor.setLocalJarPath(localJarPath);

		List<File> shipFiles = new ArrayList<>();
		// path to directory to ship
		if (cmd.hasOption(SHIP_PATH.getOpt())) {
			String shipPath = cmd.getOptionValue(SHIP_PATH.getOpt());
			File shipDir = new File(shipPath);
			if (shipDir.isDirectory()) {
				shipFiles.add(shipDir);
			} else {
				LOG.warn("Ship directory is not a directory. Ignoring it.");
			}
		}

		yarnClusterDescriptor.addShipFiles(shipFiles);

		// queue
		if (cmd.hasOption(QUEUE.getOpt())) {
			yarnClusterDescriptor.setQueue(cmd.getOptionValue(QUEUE.getOpt()));
		}

		// JobManager Memory
		if (cmd.hasOption(JM_MEMORY.getOpt())) {
			int jmMemory = Integer.valueOf(cmd.getOptionValue(JM_MEMORY.getOpt()));
			yarnClusterDescriptor.setJobManagerMemory(jmMemory);
		}

		// Task Managers memory
		if (cmd.hasOption(TM_MEMORY.getOpt())) {
			int tmMemory = Integer.valueOf(cmd.getOptionValue(TM_MEMORY.getOpt()));
			yarnClusterDescriptor.setTaskManagerMemory(tmMemory);
		}

		if (cmd.hasOption(SLOTS.getOpt())) {
			int slots = Integer.valueOf(cmd.getOptionValue(SLOTS.getOpt()));
			yarnClusterDescriptor.setTaskManagerSlots(slots);
		}

		String[] dynamicProperties = null;
		if (cmd.hasOption(DYNAMIC_PROPERTIES.getOpt())) {
			dynamicProperties = cmd.getOptionValues(DYNAMIC_PROPERTIES.getOpt());
		}
		String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);

		yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);

		if (cmd.hasOption(DETACHED.getOpt()) || cmd.hasOption(CliFrontendParser.DETACHED_OPTION.getOpt())) {
			this.detachedMode = true;
			yarnClusterDescriptor.setDetachedMode(true);
		}

		if(cmd.hasOption(NAME.getOpt())) {
			yarnClusterDescriptor.setName(cmd.getOptionValue(NAME.getOpt()));
		} else {
			// set the default application name, if none is specified
			if(defaultApplicationName != null) {
				yarnClusterDescriptor.setName(defaultApplicationName);
			}
		}

		if (cmd.hasOption(ZOOKEEPER_NAMESPACE.getOpt())) {
			String zookeeperNamespace = cmd.getOptionValue(ZOOKEEPER_NAMESPACE.getOpt());
			yarnClusterDescriptor.setZookeeperNamespace(zookeeperNamespace);
		}

		// ----- Convenience -----

		// the number of slots available from YARN:
		int yarnTmSlots = yarnClusterDescriptor.getTaskManagerSlots();
		if (yarnTmSlots == -1) {
			yarnTmSlots = 1;
			yarnClusterDescriptor.setTaskManagerSlots(yarnTmSlots);
		}

		int maxSlots = yarnTmSlots * yarnClusterDescriptor.getTaskManagerCount();
		int userParallelism = Integer.valueOf(cmd.getOptionValue(CliFrontendParser.PARALLELISM_OPTION.getOpt(), "-1"));
		if (userParallelism != -1) {
			int slotsPerTM = (int) Math.ceil((double) userParallelism / yarnClusterDescriptor.getTaskManagerCount());
			String message = "The YARN cluster has " + maxSlots + " slots available, " +
				"but the user requested a parallelism of " + userParallelism + " on YARN. " +
				"Each of the " + yarnClusterDescriptor.getTaskManagerCount() + " TaskManagers " +
				"will get "+slotsPerTM+" slots.";
			logAndSysout(message);
			yarnClusterDescriptor.setTaskManagerSlots(slotsPerTM);
		}

		return yarnClusterDescriptor;
	}

	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("   Required");
		Options req = new Options();
		req.addOption(CONTAINER);
		formatter.printHelp(" ", req);

		formatter.setSyntaxPrefix("   Optional");
		Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);
		formatter.printHelp(" ", options);
	}

	public static void runInteractiveCli(YarnClusterClient yarnCluster, boolean readConsoleInput) {
		final String HELP = "Available commands:\n" +
				"help - show these commands\n" +
				"stop - stop the YARN session";
		int numTaskmanagers = 0;
		String applicationId = yarnCluster.getApplicationId().toString();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			label:
			while (true) {
				// ------------------ check if there are updates by the cluster -----------

				GetClusterStatusResponse status = yarnCluster.getClusterStatus();
				LOG.debug("Received status message: {}", status);

				if (status != null && numTaskmanagers != status.numRegisteredTaskManagers()) {
					System.err.println("Number of connected TaskManagers changed to " +
							status.numRegisteredTaskManagers() + ". " +
						"Slots available: " + status.totalNumberOfSlots());
					numTaskmanagers = status.numRegisteredTaskManagers();
				}

				List<String> messages = yarnCluster.getNewMessages();
				if (messages != null && messages.size() > 0) {
					System.err.println("New messages from the YARN cluster: ");
					for (String msg : messages) {
						System.err.println(msg);
					}
				}

				if (yarnCluster.getApplicationStatus() != ApplicationStatus.SUCCEEDED) {
					System.err.println("The YARN cluster has failed");
					yarnCluster.shutdown();
				}

				// wait until CLIENT_POLLING_INTERVAL is over or the user entered something.
				long startTime = System.currentTimeMillis();
				while ((System.currentTimeMillis() - startTime) < CLIENT_POLLING_INTERVALL * 1000
						&& (!readConsoleInput || !in.ready()))
				{
					Thread.sleep(200);
				}
				//------------- handle interactive command by user. ----------------------

				if (readConsoleInput && in.ready()) {
					String command = in.readLine();
					switch (command) {
						case "quit":
						case "stop":
							yarnCluster.shutdownCluster();
							break label;
						case "help":
							System.err.println(HELP);
							break;
						default:
							System.err.println("Unknown command '" + command + "'. Showing help: \n" + HELP);
							break;
					}
				}

				if (yarnCluster.hasBeenShutdown()) {
					LOG.info("Stopping interactive command line interface, YARN cluster has been stopped.");
					break;
				}
			}
		} catch(Exception e) {
			LOG.warn("Exception while running the interactive command line interface", e);
		}
	}

	public static void main(final String[] args) throws Exception {
		final FlinkYarnSessionCli cli = new FlinkYarnSessionCli("", ""); // no prefix for the YARN session
		Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration();
		SecurityUtils.install(new SecurityUtils.SecurityConfiguration(flinkConfiguration));
		int retCode = SecurityUtils.getInstalledContext().runSecured(new Callable<Integer>() {
			@Override
			public Integer call() {
				return cli.run(args);
			}
		});
		System.exit(retCode);
	}

	@Override
	public boolean isActive(CommandLine commandLine, Configuration configuration) {
		String jobManagerOption = commandLine.getOptionValue(ADDRESS_OPTION.getOpt(), null);
		boolean yarnJobManager = ID.equals(jobManagerOption);
		boolean yarnAppId = commandLine.hasOption(APPLICATION_ID.getOpt());
		return yarnJobManager || yarnAppId || loadYarnPropertiesFile(commandLine, configuration) != null;
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		for (Object option : ALL_OPTIONS.getOptions()) {
			baseOptions.addOption((Option) option);
		}
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(APPLICATION_ID);
	}

	@Override
	public YarnClusterClient retrieveCluster(
			CommandLine cmdLine,
			Configuration config) throws UnsupportedOperationException {

		// get secure cookie if passed as argument
		String secureCookieArg = cmdLine.hasOption(SECURE_COOKIE_OPTION.getOpt()) ?
				cmdLine.getOptionValue(SECURE_COOKIE_OPTION.getOpt()) : null;

		// first check for an application id, then try to load from yarn properties
		String applicationID = cmdLine.hasOption(APPLICATION_ID.getOpt()) ?
				cmdLine.getOptionValue(APPLICATION_ID.getOpt())
				: loadYarnPropertiesFile(cmdLine, config);

		if(null != applicationID) {
			String zkNamespace = cmdLine.hasOption(ZOOKEEPER_NAMESPACE.getOpt()) ?
					cmdLine.getOptionValue(ZOOKEEPER_NAMESPACE.getOpt())
					: config.getString(HighAvailabilityOptions.HA_CLUSTER_ID, applicationID);
			config.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);

			// Use cookie from CLI if provided, instead look for configuration setting and finally
			// try to retrieve from the persisted file
			boolean securityEnabled = SecurityUtils.isSecurityEnabled(config);
			LOG.debug("Security Enabled ? {}", securityEnabled);
			if(securityEnabled ) {
				if(secureCookieArg != null) {
					LOG.debug("Security is enabled and secure cookie is provided as argument");
					config.setString(ConfigConstants.SECURITY_COOKIE, secureCookieArg);
				} else {
					String secureCookie = config.getString(ConfigConstants.SECURITY_COOKIE, null);
					if(secureCookie == null) {
						LOG.debug("Security is enabled but cookie not provided. retrieving cookie from properties file");
						secureCookie = getAppSecureCookie(applicationID);
						config.setString(ConfigConstants.SECURITY_COOKIE, secureCookie);
					}
				}
			}

			AbstractYarnClusterDescriptor yarnDescriptor = getClusterDescriptor();
			yarnDescriptor.setFlinkConfiguration(config);
			return yarnDescriptor.retrieve(applicationID);
		} else {
			throw new UnsupportedOperationException("Could not resume a Yarn cluster.");
		}
	}

	@Override
	public YarnClusterClient createCluster(
			String applicationName,
			CommandLine cmdLine,
			Configuration config,
			List<URL> userJarFiles) {
		Preconditions.checkNotNull(userJarFiles, "User jar files should not be null.");

		// get secure cookie if passed as argument
		String secureCookieArg = cmdLine.hasOption(SECURE_COOKIE_OPTION.getOpt()) ?
				cmdLine.getOptionValue(SECURE_COOKIE_OPTION.getOpt()) : null;

		boolean securityEnabled = SecurityUtils.isSecurityEnabled(config);
		LOG.debug("Security Enabled ? {}", securityEnabled);

		//override cookie configuration if supplied through CLI
		if(securityEnabled && secureCookieArg != null) {
			LOG.debug("Secure cookie is provided as CLI argument and will be used");
			config.setBoolean(ConfigConstants.SECURITY_ENABLED, true);
			config.setString(ConfigConstants.SECURITY_COOKIE, secureCookieArg);
		}

		AbstractYarnClusterDescriptor yarnClusterDescriptor = createDescriptor(applicationName, cmdLine);
		yarnClusterDescriptor.setFlinkConfiguration(config);
		yarnClusterDescriptor.setProvidedUserJarFiles(userJarFiles);

		try {
			YarnClusterClient yarnCluster = yarnClusterDescriptor.deploy();
			persistYarnApplicationState(yarnClusterDescriptor, yarnCluster);
			return yarnCluster;
		} catch (Exception e) {
			throw new RuntimeException("Error deploying the YARN cluster", e);
		}

	}

	public int run(String[] args) {
		//
		//	Command Line Options
		//
		Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
		} catch(Exception e) {
			System.out.println(e.getMessage());
			printUsage();
			return 1;
		}

		// get secure cookie if passed as argument
		String secureCookieArg = cmd.hasOption(SECURE_COOKIE_OPTION.getOpt()) ?
				cmd.getOptionValue(SECURE_COOKIE_OPTION.getOpt()) : null;

		// Query cluster for metrics
		if (cmd.hasOption(QUERY.getOpt())) {
			AbstractYarnClusterDescriptor yarnDescriptor = getClusterDescriptor();
			String description;
			try {
				description = yarnDescriptor.getClusterDescription();
			} catch (Exception e) {
				System.err.println("Error while querying the YARN cluster for available resources: "+e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}
			System.out.println(description);
			return 0;
		} else if (cmd.hasOption(APPLICATION_ID.getOpt())) {
			AbstractYarnClusterDescriptor yarnDescriptor = getClusterDescriptor();

			//configure ZK namespace depending on the value passed
			String zkNamespace = cmd.hasOption(ZOOKEEPER_NAMESPACE.getOpt()) ?
									cmd.getOptionValue(ZOOKEEPER_NAMESPACE.getOpt())
									:yarnDescriptor.getFlinkConfiguration()
									.getString(HA_ZOOKEEPER_NAMESPACE_KEY, cmd.getOptionValue(APPLICATION_ID.getOpt()));
			LOG.info("Going to use the ZK namespace: {}", zkNamespace);
			yarnDescriptor.getFlinkConfiguration().setString(HA_ZOOKEEPER_NAMESPACE_KEY, zkNamespace);

			boolean securityEnabled = SecurityUtils.isSecurityEnabled(yarnDescriptor.getFlinkConfiguration());
			LOG.debug("Security Enabled ? {}", securityEnabled);

			//override cookie configuration if supplied through CLI
			if(securityEnabled && secureCookieArg != null) {
				LOG.debug("Secure cookie is provided as CLI argument and will be used");
				yarnDescriptor.getFlinkConfiguration().setBoolean(ConfigConstants.SECURITY_ENABLED, true);
				yarnDescriptor.getFlinkConfiguration().setString(ConfigConstants.SECURITY_COOKIE, secureCookieArg);
			}

			try {
				yarnCluster = yarnDescriptor.retrieve(cmd.getOptionValue(APPLICATION_ID.getOpt()));
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve existing Yarn application", e);
			}

			if (detachedMode) {
				LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
					"Flink on YARN, use the following command or a YARN web interface to stop it:\n" +
					"yarn application -kill " + APPLICATION_ID.getOpt());
				yarnCluster.disconnect();
			} else {
				runInteractiveCli(yarnCluster, true);
			}
		} else {

			AbstractYarnClusterDescriptor yarnDescriptor;
			try {
				yarnDescriptor = createDescriptor(null, cmd);
			} catch (Exception e) {
				System.err.println("Error while starting the YARN Client: " + e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}

			boolean securityEnabled = SecurityUtils.isSecurityEnabled(yarnDescriptor.getFlinkConfiguration());
			LOG.debug("Security Enabled ? {}", securityEnabled);

			//override cookie configuration if supplied through CLI
			if(securityEnabled && secureCookieArg != null) {
				LOG.debug("Secure cookie is provided as CLI argument and will be used");
				yarnDescriptor.getFlinkConfiguration().setBoolean(ConfigConstants.SECURITY_ENABLED, true);
				yarnDescriptor.getFlinkConfiguration().setString(ConfigConstants.SECURITY_COOKIE, secureCookieArg);
			}

			try {
				yarnCluster = yarnDescriptor.deploy();
			} catch (Exception e) {
				System.err.println("Error while deploying YARN cluster: "+e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}
			//------------------ ClusterClient deployed, handle connection details
			String jobManagerAddress =
				yarnCluster.getJobManagerAddress().getAddress().getHostName() +
					":" + yarnCluster.getJobManagerAddress().getPort();

			System.out.println("Flink JobManager is now running on " + jobManagerAddress);
			System.out.println("JobManager Web Interface: " + yarnCluster.getWebInterfaceURL());

			persistYarnApplicationState(yarnDescriptor, yarnCluster);

			//------------------ ClusterClient running, let user control it ------------

			if (detachedMode) {
				// print info and quit:
				LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
						"Flink on YARN, use the following command or a YARN web interface to stop it:\n" +
						"yarn application -kill " + yarnCluster.getApplicationId() + System.lineSeparator() +
						"Please also note that the temporary files of the YARN session in {} will not be removed.",
						yarnDescriptor.getSessionFilesDir());
				yarnCluster.waitForClusterToBeReady();
				yarnCluster.disconnect();
			} else {
				runInteractiveCli(yarnCluster, acceptInteractiveInput);
			}
		}
		return 0;
	}

	/**
	 * Utility method for tests.
	 */
	public void stop() {
		if (yarnCluster != null) {
			LOG.info("Command line interface is shutting down the yarnCluster");
			yarnCluster.shutdown();
		}
	}

	private void logAndSysout(String message) {
		LOG.info(message);
		System.out.println(message);
	}

	public static Map<String, String> getDynamicProperties(String dynamicPropertiesEncoded) {
		if (dynamicPropertiesEncoded != null && dynamicPropertiesEncoded.length() > 0) {
			Map<String, String> properties = new HashMap<>();

			String[] propertyLines = dynamicPropertiesEncoded.split(YARN_DYNAMIC_PROPERTIES_SEPARATOR);
			for (String propLine : propertyLines) {
				if (propLine == null) {
					continue;
				}

				String[] kv = propLine.split("=");
				if (kv.length >= 2 && kv[0] != null && kv[1] != null && kv[0].length() > 0) {
					properties.put(kv[0], kv[1]);
				}
			}
			return properties;
		}
		else {
			return Collections.emptyMap();
		}
	}

	public static File getYarnPropertiesLocation() {
		String path = System.getProperty("user.home") + File.separator + YARN_APP_INI;
		File stateFile;
		try {
			stateFile = new File(path);
			if(!stateFile.exists()) {
				stateFile.createNewFile();
			}
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		return stateFile;
	}

	public static void persistAppState(YarnAppState appState) {

		final String appId = appState.getApplicationId();
		final String parallelism = appState.getParallelism();
		final String dynaProps = appState.getDynamicProperties();
		final String cookie = appState.getCookie();

		if(appId == null) {
			throw new RuntimeException("Missing application ID from Yarn application state");
		}

		String path = getYarnPropertiesLocation().getAbsolutePath();

		LOG.debug("Going to persist Yarn application state: {} in {}", appState,path);

		try {
			HierarchicalINIConfiguration config = new HierarchicalINIConfiguration(path);

			SubnodeConfiguration subNode = config.getSection(appId);
			if(!subNode.isEmpty()) {
				throw new RuntimeException("Application with ID " + appId + "already exists");
			}

			subNode.addProperty(YARN_PROPERTIES_PARALLELISM, parallelism);
			subNode.addProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING, dynaProps);
			subNode.addProperty(YARN_PROPERTIES_SECURE_COOKIE, cookie);

			//update latest entry section with the most recent APP Id
			config.clearTree(YARN_LATEST_ENTRY_SECTION_NAME);
			SubnodeConfiguration activeAppSection = config.getSection(YARN_LATEST_ENTRY_SECTION_NAME);
			activeAppSection.addProperty(YARN_APPLICATION_ID_KEY, appId);

			config.save();
			LOG.debug("Persisted Yarn App state: {}", appState);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static YarnAppState retrieveMostRecentYarnApp() {
		String path = getYarnPropertiesLocation().getAbsolutePath();
		LOG.debug("Going to fetch app state from {}", path);
		try {
			HierarchicalINIConfiguration config = new HierarchicalINIConfiguration(path);
			SubnodeConfiguration subNode = config.getSection(YARN_LATEST_ENTRY_SECTION_NAME);
			String appId = subNode.getString(YARN_APPLICATION_ID_KEY, null);
			if(null != appId) {
				subNode = config.getSection(appId);
				YarnAppState.YarnAppStateBuilder builder = YarnAppState.builder();
				builder.appId(appId);
				builder.parallelism(subNode.getString(YARN_PROPERTIES_PARALLELISM, null));
				builder.dynamicProps(subNode.getString(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING, null));
				builder.cookie(subNode.getString(YARN_PROPERTIES_SECURE_COOKIE, null));
				YarnAppState yarnAppState = builder.build();
				LOG.debug("Retrieved Yarn App state: {} from: {} for the App ID: {}", yarnAppState, path, appId);
				return yarnAppState;
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	public static String getAppSecureCookie(String appId) {
		if(appId == null) {
			throw new RuntimeException("Application ID is required to retrieve Yarn App cookie Info");
		}
		String cookieFromFile;
		String path = getYarnPropertiesLocation().getAbsolutePath();
		LOG.debug("Going to fetch cookie for the appID: {} from {}", appId, path);

		try {
			HierarchicalINIConfiguration config = new HierarchicalINIConfiguration(path);
			SubnodeConfiguration subNode = config.getSection(appId);
			if (!subNode.containsKey(YARN_PROPERTIES_SECURE_COOKIE)) {
				String errorMessage = "Could  not find the app ID section in "+ path + " for the appID: "+ appId;
				throw new RuntimeException(errorMessage);
			}
			cookieFromFile = subNode.getString(YARN_PROPERTIES_SECURE_COOKIE, "");
			if(cookieFromFile.length() == 0) {
				String errorMessage = "Could  not find cookie in "+ path + " for the appID: "+ appId;
				throw new RuntimeException(errorMessage);
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}

		LOG.debug("Found cookie for the appID: {}", appId);
		return cookieFromFile;
	}

	public static void removeAppState(String appId) {
		if(appId == null) { return; }
		String path = getYarnPropertiesLocation().getAbsolutePath();
		LOG.debug("Going to remove the reference for the appId: {} from {}", appId, path);
		try {
			HierarchicalINIConfiguration config = new HierarchicalINIConfiguration(path);
			config.clearTree(appId);

			//clear app ID entry from most recent deployment section if the app id is part of it
			SubnodeConfiguration activeAppSection = config.getSection(YARN_LATEST_ENTRY_SECTION_NAME);
			String applicationId = activeAppSection.getString(YARN_APPLICATION_ID_KEY);
			LOG.debug("Retrieved application ID: {} from the active list", applicationId);
			if(applicationId == null || applicationId.equals(appId)) {
				config.clearTree(YARN_LATEST_ENTRY_SECTION_NAME);
			}

			config.save();
			LOG.debug("Removed the reference for the appId: {} from {}", appId, path);
		} catch(Exception e) {
			LOG.warn("Exception occurred while fetching cookie for app id: {}", appId, e);
		}
	}

	protected AbstractYarnClusterDescriptor getClusterDescriptor() {
		return new YarnClusterDescriptor();
	}

	private void persistYarnApplicationState(AbstractYarnClusterDescriptor yarnDescriptor, YarnClusterClient yarnCluster) {

		YarnAppState.YarnAppStateBuilder yarnAppStateBuilder = YarnAppState.builder();

		yarnAppStateBuilder.appId(yarnCluster.getApplicationId().toString());

		if (yarnDescriptor.getTaskManagerSlots() != -1) {
			String parallelism =
					Integer.toString(yarnDescriptor.getTaskManagerSlots() * yarnDescriptor.getTaskManagerCount());
			yarnAppStateBuilder.parallelism(parallelism);
		}

		if (yarnDescriptor.getDynamicPropertiesEncoded() != null) {
			yarnAppStateBuilder.dynamicProps(yarnDescriptor.getDynamicPropertiesEncoded());
		}

		String secureCookie = yarnDescriptor.getFlinkConfiguration()
				.getString(ConfigConstants.SECURITY_COOKIE, null);
		yarnAppStateBuilder.cookie(secureCookie);

		persistAppState(yarnAppStateBuilder.build());

	}

	public static class YarnAppState {

		private String applicationId;
		private String parallelism;
		private String dynamicProperties;
		private String cookie;

		public String getApplicationId() {
			return applicationId;
		}

		public String getParallelism() {
			return parallelism;
		}

		public String getCookie() {
			return cookie;
		}

		public String getDynamicProperties() {
			return dynamicProperties;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Application ID: ").append(applicationId).append(" ");
			builder.append("Parallelism: ").append(parallelism).append(" ");
			builder.append("Dynamic Properties: ").append(dynamicProperties).append(" ");
			builder.append("Cookie: ").append(cookie != null?"***":cookie).append(" ");
			return builder.toString();
		}

		private YarnAppState(){}

		public static YarnAppStateBuilder builder() {
			return new YarnAppStateBuilder();
		}

		public static class YarnAppStateBuilder {

			private String applicationId;
			private String parallelism;
			private String cookie;
			private String dynamicProperties;

			public YarnAppStateBuilder appId(String appId) {
				this.applicationId = appId;
				return this;
			}

			public YarnAppStateBuilder parallelism(String parallelism) {
				this.parallelism = parallelism;
				return this;
			}

			public YarnAppStateBuilder dynamicProps(String dynamicProps) {
				this.dynamicProperties = dynamicProps;
				return this;
			}

			public YarnAppStateBuilder cookie(String cookie) {
				this.cookie = cookie;
				return this;
			}

			public YarnAppState build() {
				YarnAppState appState = new YarnAppState();
				appState.applicationId = this.applicationId;
				appState.parallelism = this.parallelism;
				appState.dynamicProperties = this.dynamicProperties;
				appState.cookie = this.cookie;
				return appState;
			}
		}

	}
}
