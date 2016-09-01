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
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
	private static final String YARN_PROPERTIES_FILE = ".yarn-properties-";
	static final String YARN_APPLICATION_ID_KEY = "applicationID";
	private static final String YARN_PROPERTIES_PARALLELISM = "parallelism";
	private static final String YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING = "dynamicPropertiesString";

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

		// load the YARN properties
		File propertiesFile = getYarnPropertiesLocation(flinkConfiguration);
		if (!propertiesFile.exists()) {
			return null;
		}

		logAndSysout("Found YARN properties file " + propertiesFile.getAbsolutePath());

		Properties yarnProperties = new Properties();
		try {
			try (InputStream is = new FileInputStream(propertiesFile)) {
				yarnProperties.load(is);
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Cannot read the YARN properties file", e);
		}

		// get the Yarn application id from the properties file
		String applicationID = yarnProperties.getProperty(YARN_APPLICATION_ID_KEY);
		if (applicationID == null) {
			throw new IllegalConfigurationException("Yarn properties file found but doesn't contain a " +
				"Yarn applicaiton id. Please delete the file at " + propertiesFile.getAbsolutePath());
		}

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
		String propParallelism = yarnProperties.getProperty(YARN_PROPERTIES_PARALLELISM);
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
		String dynamicPropertiesEncoded = yarnProperties.getProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING);
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

	private static void writeYarnProperties(Properties properties, File propertiesFile) {
		try (final OutputStream out = new FileOutputStream(propertiesFile)) {
			properties.store(out, "Generated YARN properties file");
		} catch (IOException e) {
			throw new RuntimeException("Error writing the properties file", e);
		}
		propertiesFile.setReadable(true, false); // readable for all.
	}

	public static void runInteractiveCli(YarnClusterClient yarnCluster, boolean readConsoleInput) {
		final String HELP = "Available commands:\n" +
				"help - show these commands\n" +
				"stop - stop the YARN session";
		int numTaskmanagers = 0;
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

	public static void main(String[] args) {
		FlinkYarnSessionCli cli = new FlinkYarnSessionCli("", ""); // no prefix for the YARN session
		System.exit(cli.run(args));
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

		// first check for an application id, then try to load from yarn properties
		String applicationID = cmdLine.hasOption(APPLICATION_ID.getOpt()) ?
				cmdLine.getOptionValue(APPLICATION_ID.getOpt())
				: loadYarnPropertiesFile(cmdLine, config);

		if(null != applicationID) {
			String zkNamespace = cmdLine.hasOption(ZOOKEEPER_NAMESPACE.getOpt()) ?
					cmdLine.getOptionValue(ZOOKEEPER_NAMESPACE.getOpt())
					: config.getString(HA_ZOOKEEPER_NAMESPACE_KEY, applicationID);
			config.setString(HA_ZOOKEEPER_NAMESPACE_KEY, zkNamespace);

			AbstractYarnClusterDescriptor yarnDescriptor = getClusterDescriptor();
			yarnDescriptor.setFlinkConfiguration(config);
			return yarnDescriptor.retrieve(applicationID);
		} else {
			throw new UnsupportedOperationException("Could not resume a Yarn cluster.");
		}
	}

	@Override
	public YarnClusterClient createCluster(String applicationName, CommandLine cmdLine, Configuration config) {

		AbstractYarnClusterDescriptor yarnClusterDescriptor = createDescriptor(applicationName, cmdLine);
		yarnClusterDescriptor.setFlinkConfiguration(config);

		try {
			return yarnClusterDescriptor.deploy();
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

			try {
				yarnCluster = yarnDescriptor.deploy();
			} catch (Exception e) {
				System.err.println("Error while deploying YARN cluster: "+e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}
			//------------------ ClusterClient deployed, handle connection details
			String jobManagerAddress =
				yarnCluster.getJobManagerAddress().getAddress().getHostAddress() +
					":" + yarnCluster.getJobManagerAddress().getPort();

			System.out.println("Flink JobManager is now running on " + jobManagerAddress);
			System.out.println("JobManager Web Interface: " + yarnCluster.getWebInterfaceURL());

			// file that we write into the conf/ dir containing the jobManager address and the dop.
			File yarnPropertiesFile = getYarnPropertiesLocation(yarnCluster.getFlinkConfiguration());

			Properties yarnProps = new Properties();
			yarnProps.setProperty(YARN_APPLICATION_ID_KEY, yarnCluster.getApplicationId().toString());
			if (yarnDescriptor.getTaskManagerSlots() != -1) {
				String parallelism =
						Integer.toString(yarnDescriptor.getTaskManagerSlots() * yarnDescriptor.getTaskManagerCount());
				yarnProps.setProperty(YARN_PROPERTIES_PARALLELISM, parallelism);
			}
			// add dynamic properties
			if (yarnDescriptor.getDynamicPropertiesEncoded() != null) {
				yarnProps.setProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING,
						yarnDescriptor.getDynamicPropertiesEncoded());
			}
			writeYarnProperties(yarnProps, yarnPropertiesFile);

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

	public static File getYarnPropertiesLocation(Configuration conf) {
		String defaultPropertiesFileLocation = System.getProperty("java.io.tmpdir");
		String currentUser = System.getProperty("user.name");
		String propertiesFileLocation =
			conf.getString(ConfigConstants.YARN_PROPERTIES_FILE_LOCATION, defaultPropertiesFileLocation);

		return new File(propertiesFileLocation, YARN_PROPERTIES_FILE + currentUser);
	}

	protected AbstractYarnClusterDescriptor getClusterDescriptor() {
		return new YarnClusterDescriptor();
	}
}
