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

import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptorV2;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
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
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.apache.flink.client.cli.CliFrontendParser.ADDRESS_OPTION;
import static org.apache.flink.configuration.HighAvailabilityOptions.HA_CLUSTER_ID;

/**
 * Class handling the command line interface to the YARN session.
 */
public class FlinkYarnSessionCli implements CustomCommandLine<YarnClusterClient> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnSessionCli.class);

	//------------------------------------ Constants   -------------------------

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

	private static final int CLIENT_POLLING_INTERVALL = 3;

	/** The id for the CommandLine interface. */
	private static final String ID = "yarn-cluster";

	// YARN-session related constants
	private static final String YARN_PROPERTIES_FILE = ".yarn-properties-";
	static final String YARN_APPLICATION_ID_KEY = "applicationID";
	private static final String YARN_PROPERTIES_PARALLELISM = "parallelism";
	private static final String YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING = "dynamicPropertiesString";

	private static final String YARN_DYNAMIC_PROPERTIES_SEPARATOR = "@@"; // this has to be a regex for String.split()

	//------------------------------------ Command Line argument options -------------------------
	// the prefix transformation is used by the CliFrontend static constructor.
	private final Option query;
	// --- or ---
	private final Option applicationId;
	// --- or ---
	private final Option queue;
	private final Option shipPath;
	private final Option flinkJar;
	private final Option jmMemory;
	private final Option tmMemory;
	private final Option container;
	private final Option slots;
	private final Option detached;
	private final Option zookeeperNamespace;
	private final Option flip6;

	/**
	 * @deprecated Streaming mode has been deprecated without replacement. Set the
	 * {@link TaskManagerOptions#MANAGED_MEMORY_PRE_ALLOCATE} configuration
	 * key to true to get the previous batch mode behaviour.
	 */
	@Deprecated
	private final Option streaming;
	private final Option name;

	private final Options allOptions;

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.network.memory.min=536346624</tt>.
	 */
	private final Option dynamicproperties;

	private final boolean acceptInteractiveInput;

	//------------------------------------ Internal fields -------------------------
	private YarnClusterClient yarnCluster;
	private boolean detachedMode = false;

	public FlinkYarnSessionCli(String shortPrefix, String longPrefix) {
		this(shortPrefix, longPrefix, true);
	}

	public FlinkYarnSessionCli(String shortPrefix, String longPrefix, boolean acceptInteractiveInput) {
		this.acceptInteractiveInput = acceptInteractiveInput;

		query = new Option(shortPrefix + "q", longPrefix + "query", false, "Display available YARN resources (memory, cores)");
		applicationId = new Option(shortPrefix + "id", longPrefix + "applicationId", true, "Attach to running YARN session");
		queue = new Option(shortPrefix + "qu", longPrefix + "queue", true, "Specify YARN queue.");
		shipPath = new Option(shortPrefix + "t", longPrefix + "ship", true, "Ship files in the specified directory (t for transfer)");
		flinkJar = new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
		jmMemory = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container [in MB]");
		tmMemory = new Option(shortPrefix + "tm", longPrefix + "taskManagerMemory", true, "Memory per TaskManager Container [in MB]");
		container = new Option(shortPrefix + "n", longPrefix + "container", true, "Number of YARN container to allocate (=Number of Task Managers)");
		slots = new Option(shortPrefix + "s", longPrefix + "slots", true, "Number of slots per TaskManager");
		dynamicproperties = new Option(shortPrefix + "D", true, "Dynamic properties");
		detached = new Option(shortPrefix + "d", longPrefix + "detached", false, "Start detached");
		streaming = new Option(shortPrefix + "st", longPrefix + "streaming", false, "Start Flink in streaming mode");
		name = new Option(shortPrefix + "nm", longPrefix + "name", true, "Set a custom name for the application on YARN");
		zookeeperNamespace = new Option(shortPrefix + "z", longPrefix + "zookeeperNamespace", true, "Namespace to create the Zookeeper sub-paths for high availability mode");
		flip6 = new Option(shortPrefix + "f6", longPrefix + "flip6", false, "Specify this option to start a Flip-6 Yarn session cluster.");

		allOptions = new Options();
		allOptions.addOption(flinkJar);
		allOptions.addOption(jmMemory);
		allOptions.addOption(tmMemory);
		allOptions.addOption(container);
		allOptions.addOption(queue);
		allOptions.addOption(query);
		allOptions.addOption(shipPath);
		allOptions.addOption(slots);
		allOptions.addOption(dynamicproperties);
		allOptions.addOption(detached);
		allOptions.addOption(streaming);
		allOptions.addOption(name);
		allOptions.addOption(applicationId);
		allOptions.addOption(zookeeperNamespace);
		allOptions.addOption(flip6);
	}

	/**
	 * Tries to load a Flink Yarn properties file and returns the Yarn application id if successful.
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
			if (allOptions.hasOption(option.getOpt())) {
				if (!option.getOpt().equals(detached.getOpt())) {
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
				"Yarn application id. Please delete the file at " + propertiesFile.getAbsolutePath());
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

	public AbstractYarnClusterDescriptor createDescriptor(
		Configuration configuration,
		String configurationDirectory,
		String defaultApplicationName,
		CommandLine cmd) {

		AbstractYarnClusterDescriptor yarnClusterDescriptor = getClusterDescriptor(
			configuration,
			configurationDirectory,
			cmd.hasOption(flip6.getOpt()));

		// Jar Path
		Path localJarPath;
		if (cmd.hasOption(flinkJar.getOpt())) {
			String userPath = cmd.getOptionValue(flinkJar.getOpt());
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
					" Please supply a path manually via the -" + flinkJar.getOpt() + " option.");
			}
		}

		yarnClusterDescriptor.setLocalJarPath(localJarPath);

		List<File> shipFiles = new ArrayList<>();
		// path to directory to ship
		if (cmd.hasOption(shipPath.getOpt())) {
			String shipPath = cmd.getOptionValue(this.shipPath.getOpt());
			File shipDir = new File(shipPath);
			if (shipDir.isDirectory()) {
				shipFiles.add(shipDir);
			} else {
				LOG.warn("Ship directory is not a directory. Ignoring it.");
			}
		}

		yarnClusterDescriptor.addShipFiles(shipFiles);

		// queue
		if (cmd.hasOption(queue.getOpt())) {
			yarnClusterDescriptor.setQueue(cmd.getOptionValue(queue.getOpt()));
		}

		String[] dynamicProperties = null;
		if (cmd.hasOption(dynamicproperties.getOpt())) {
			dynamicProperties = cmd.getOptionValues(dynamicproperties.getOpt());
		}
		String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);

		yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);

		if (cmd.hasOption(detached.getOpt()) || cmd.hasOption(CliFrontendParser.DETACHED_OPTION.getOpt())) {
			this.detachedMode = true;
			yarnClusterDescriptor.setDetachedMode(true);
		}

		if (cmd.hasOption(name.getOpt())) {
			yarnClusterDescriptor.setName(cmd.getOptionValue(name.getOpt()));
		} else {
			// set the default application name, if none is specified
			if (defaultApplicationName != null) {
				yarnClusterDescriptor.setName(defaultApplicationName);
			}
		}

		if (cmd.hasOption(zookeeperNamespace.getOpt())) {
			String zookeeperNamespace = cmd.getOptionValue(this.zookeeperNamespace.getOpt());
			yarnClusterDescriptor.setZookeeperNamespace(zookeeperNamespace);
		}

		return yarnClusterDescriptor;
	}

	public ClusterSpecification createClusterSpecification(Configuration configuration, CommandLine cmd) {
		if (!cmd.hasOption(container.getOpt())) { // number of containers is required option!
			LOG.error("Missing required argument {}", container.getOpt());
			printUsage();
			throw new IllegalArgumentException("Missing required argument " + container.getOpt());
		}

		int numberTaskManagers = Integer.valueOf(cmd.getOptionValue(container.getOpt()));

		// JobManager Memory
		final int jobManagerMemoryMB;
		if (cmd.hasOption(jmMemory.getOpt())) {
			jobManagerMemoryMB = Integer.valueOf(cmd.getOptionValue(this.jmMemory.getOpt()));
		} else {
			jobManagerMemoryMB = configuration.getInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY);
		}

		// Task Managers memory
		final int taskManagerMemoryMB;
		if (cmd.hasOption(tmMemory.getOpt())) {
			taskManagerMemoryMB = Integer.valueOf(cmd.getOptionValue(this.tmMemory.getOpt()));
		} else {
			taskManagerMemoryMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);
		}

		int slotsPerTaskManager;
		if (cmd.hasOption(slots.getOpt())) {
			slotsPerTaskManager = Integer.valueOf(cmd.getOptionValue(this.slots.getOpt()));
		} else {
			slotsPerTaskManager = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
		}

		// convenience
		int userParallelism = Integer.valueOf(cmd.getOptionValue(CliFrontendParser.PARALLELISM_OPTION.getOpt(), "-1"));
		int maxSlots = slotsPerTaskManager * numberTaskManagers;
		if (userParallelism != -1) {
			int slotsPerTM = (int) Math.ceil((double) userParallelism / numberTaskManagers);
			String message = "The YARN cluster has " + maxSlots + " slots available, " +
				"but the user requested a parallelism of " + userParallelism + " on YARN. " +
				"Each of the " + numberTaskManagers + " TaskManagers " +
				"will get " + slotsPerTM + " slots.";
			logAndSysout(message);
			slotsPerTaskManager = slotsPerTM;
		}

		return new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(jobManagerMemoryMB)
			.setTaskManagerMemoryMB(taskManagerMemoryMB)
			.setNumberTaskManagers(numberTaskManagers)
			.setSlotsPerTaskManager(slotsPerTaskManager)
			.createClusterSpecification();
	}

	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("   Required");
		Options req = new Options();
		req.addOption(container);
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
		final String help = "Available commands:\n" +
				"help - show these commands\n" +
				"stop - stop the YARN session";
		int numTaskmanagers = 0;
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			label:
			while (true) {
				// ------------------ check if there are updates by the cluster -----------

				try {
					GetClusterStatusResponse status = yarnCluster.getClusterStatus();
					LOG.debug("Received status message: {}", status);

					if (status != null && numTaskmanagers != status.numRegisteredTaskManagers()) {
						System.err.println("Number of connected TaskManagers changed to " +
							status.numRegisteredTaskManagers() + ". " +
							"Slots available: " + status.totalNumberOfSlots());
						numTaskmanagers = status.numRegisteredTaskManagers();
					}
				} catch (Exception e) {
					LOG.warn("Could not retrieve the current cluster status. Skipping current retrieval attempt ...", e);
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
						&& (!readConsoleInput || !in.ready())) {
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
							System.err.println(help);
							break;
						default:
							System.err.println("Unknown command '" + command + "'. Showing help: \n" + help);
							break;
					}
				}

				if (yarnCluster.hasBeenShutdown()) {
					LOG.info("Stopping interactive command line interface, YARN cluster has been stopped.");
					break;
				}
			}
		} catch (Exception e) {
			LOG.warn("Exception while running the interactive command line interface", e);
		}
	}

	public static void main(final String[] args) throws Exception {
		final FlinkYarnSessionCli cli = new FlinkYarnSessionCli("", ""); // no prefix for the YARN session

		final String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();

		final Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration();
		SecurityUtils.install(new SecurityConfiguration(flinkConfiguration));
		int retCode = SecurityUtils.getInstalledContext().runSecured(new Callable<Integer>() {
			@Override
			public Integer call() {
				return cli.run(args, flinkConfiguration, configurationDirectory);
			}
		});
		System.exit(retCode);
	}

	@Override
	public boolean isActive(CommandLine commandLine, Configuration configuration) {
		String jobManagerOption = commandLine.getOptionValue(ADDRESS_OPTION.getOpt(), null);
		boolean yarnJobManager = ID.equals(jobManagerOption);
		boolean yarnAppId = commandLine.hasOption(applicationId.getOpt());
		return yarnJobManager || yarnAppId || loadYarnPropertiesFile(commandLine, configuration) != null;
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		for (Object option : allOptions.getOptions()) {
			baseOptions.addOption((Option) option);
		}
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(applicationId);
	}

	@Override
	public YarnClusterClient retrieveCluster(
			CommandLine cmdLine,
			Configuration config,
			String configurationDirectory) throws UnsupportedOperationException {

		// first check for an application id, then try to load from yarn properties
		String applicationID = cmdLine.hasOption(applicationId.getOpt()) ?
				cmdLine.getOptionValue(applicationId.getOpt())
				: loadYarnPropertiesFile(cmdLine, config);

		if (null != applicationID) {
			String zkNamespace = cmdLine.hasOption(zookeeperNamespace.getOpt()) ?
					cmdLine.getOptionValue(zookeeperNamespace.getOpt())
					: config.getString(HighAvailabilityOptions.HA_CLUSTER_ID, applicationID);
			config.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);

			AbstractYarnClusterDescriptor yarnDescriptor = getClusterDescriptor(
				config,
				configurationDirectory,
				cmdLine.hasOption(flip6.getOpt()));
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
			String configurationDirectory,
			List<URL> userJarFiles) {
		Preconditions.checkNotNull(userJarFiles, "User jar files should not be null.");

		AbstractYarnClusterDescriptor yarnClusterDescriptor = createDescriptor(
			config,
			configurationDirectory,
			applicationName,
			cmdLine);

		final ClusterSpecification clusterSpecification = createClusterSpecification(config, cmdLine);

		yarnClusterDescriptor.setProvidedUserJarFiles(userJarFiles);

		try {
			return yarnClusterDescriptor.deploySessionCluster(clusterSpecification);
		} catch (Exception e) {
			throw new RuntimeException("Error deploying the YARN cluster", e);
		}

	}

	public int run(
			String[] args,
			Configuration configuration,
			String configurationDirectory) {
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
		} catch (Exception e) {
			System.out.println(e.getMessage());
			printUsage();
			return 1;
		}

		// Query cluster for metrics
		if (cmd.hasOption(query.getOpt())) {
			AbstractYarnClusterDescriptor yarnDescriptor = getClusterDescriptor(
				configuration,
				configurationDirectory,
				cmd.hasOption(flip6.getOpt()));
			String description;
			try {
				description = yarnDescriptor.getClusterDescription();
			} catch (Exception e) {
				System.err.println("Error while querying the YARN cluster for available resources: " + e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}
			System.out.println(description);
			return 0;
		} else if (cmd.hasOption(applicationId.getOpt())) {

			AbstractYarnClusterDescriptor yarnDescriptor = getClusterDescriptor(
				configuration,
				configurationDirectory,
				cmd.hasOption(flip6.getOpt()));

			//configure ZK namespace depending on the value passed
			String zkNamespace = cmd.hasOption(zookeeperNamespace.getOpt()) ?
									cmd.getOptionValue(zookeeperNamespace.getOpt())
									: yarnDescriptor.getFlinkConfiguration()
									.getString(HA_CLUSTER_ID, cmd.getOptionValue(applicationId.getOpt()));
			LOG.info("Going to use the ZK namespace: {}", zkNamespace);
			yarnDescriptor.getFlinkConfiguration().setString(HA_CLUSTER_ID, zkNamespace);

			try {
				yarnCluster = yarnDescriptor.retrieve(cmd.getOptionValue(applicationId.getOpt()));
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve existing Yarn application", e);
			}

			if (detachedMode) {
				LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
					"Flink on YARN, use the following command or a YARN web interface to stop it:\n" +
					"yarn application -kill " + applicationId.getOpt());
				yarnCluster.disconnect();
			} else {
				runInteractiveCli(yarnCluster, true);
			}
		} else {

			AbstractYarnClusterDescriptor yarnDescriptor;
			try {
				yarnDescriptor = createDescriptor(configuration, configurationDirectory, null, cmd);
			} catch (Exception e) {
				System.err.println("Error while starting the YARN Client: " + e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}

			final ClusterSpecification clusterSpecification = createClusterSpecification(yarnDescriptor.getFlinkConfiguration(), cmd);

			try {
				yarnCluster = yarnDescriptor.deploySessionCluster(clusterSpecification);
			} catch (Exception e) {
				System.err.println("Error while deploying YARN cluster: " + e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}
			//------------------ ClusterClient deployed, handle connection details
			String jobManagerAddress =
				yarnCluster.getJobManagerAddress().getAddress().getHostName() +
					":" + yarnCluster.getJobManagerAddress().getPort();

			System.out.println("Flink JobManager is now running on " + jobManagerAddress);
			System.out.println("JobManager Web Interface: " + yarnCluster.getWebInterfaceURL());

			// file that we write into the conf/ dir containing the jobManager address and the dop.
			File yarnPropertiesFile = getYarnPropertiesLocation(yarnCluster.getFlinkConfiguration());

			Properties yarnProps = new Properties();
			yarnProps.setProperty(YARN_APPLICATION_ID_KEY, yarnCluster.getApplicationId().toString());
			if (clusterSpecification.getSlotsPerTaskManager() != -1) {
				String parallelism =
						Integer.toString(clusterSpecification.getSlotsPerTaskManager() * clusterSpecification.getNumberTaskManagers());
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
						"yarn application -kill " + yarnCluster.getApplicationId());
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

			try {
				yarnCluster.shutdown();
			} catch (Throwable t) {
				LOG.warn("Could not properly shutdown the yarn cluster.", t);
			}
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

				int firstEquals = propLine.indexOf("=");

				if (firstEquals >= 0) {
					String key = propLine.substring(0, firstEquals).trim();
					String value = propLine.substring(firstEquals + 1, propLine.length()).trim();

					if (!key.isEmpty()) {
						properties.put(key, value);
					}
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
			conf.getString(YarnConfigOptions.PROPERTIES_FILE_LOCATION, defaultPropertiesFileLocation);

		return new File(propertiesFileLocation, YARN_PROPERTIES_FILE + currentUser);
	}

	protected AbstractYarnClusterDescriptor getClusterDescriptor(Configuration configuration, String configurationDirectory, boolean flip6) {
		if (flip6) {
			return new YarnClusterDescriptorV2(configuration, configurationDirectory);
		} else {
			return new YarnClusterDescriptor(configuration, configurationDirectory);
		}
	}
}
