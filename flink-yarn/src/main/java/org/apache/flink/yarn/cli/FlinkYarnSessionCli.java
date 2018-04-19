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

import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.LegacyYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.YARN_DETACHED_OPTION;
import static org.apache.flink.configuration.HighAvailabilityOptions.HA_CLUSTER_ID;

/**
 * Class handling the command line interface to the YARN session.
 */
public class FlinkYarnSessionCli extends AbstractCustomCommandLine<ApplicationId> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnSessionCli.class);

	//------------------------------------ Constants   -------------------------

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

	private static final long CLIENT_POLLING_INTERVAL_MS = 3000L;

	/** The id for the CommandLine interface. */
	private static final String ID = "yarn-cluster";

	// YARN-session related constants
	private static final String YARN_PROPERTIES_FILE = ".yarn-properties-";
	private static final String YARN_APPLICATION_ID_KEY = "applicationID";
	private static final String YARN_PROPERTIES_PARALLELISM = "parallelism";
	private static final String YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING = "dynamicPropertiesString";

	private static final String YARN_DYNAMIC_PROPERTIES_SEPARATOR = "@@"; // this has to be a regex for String.split()

	private static final String YARN_SESSION_HELP = "Available commands:\n" +
		"help - show these commands\n" +
		"stop - stop the YARN session";

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
	private final Option zookeeperNamespace;
	private final Option help;

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

	private final String configurationDirectory;

	private final Properties yarnPropertiesFile;

	private final ApplicationId yarnApplicationIdFromYarnProperties;

	private final String yarnPropertiesFileLocation;

	private final boolean isNewMode;

	private final YarnConfiguration yarnConfiguration;

	public FlinkYarnSessionCli(
			Configuration configuration,
			String configurationDirectory,
			String shortPrefix,
			String longPrefix) throws FlinkException {
		this(configuration, configurationDirectory, shortPrefix, longPrefix, true);
	}

	public FlinkYarnSessionCli(
			Configuration configuration,
			String configurationDirectory,
			String shortPrefix,
			String longPrefix,
			boolean acceptInteractiveInput) throws FlinkException {
		super(configuration);
		this.configurationDirectory = Preconditions.checkNotNull(configurationDirectory);
		this.acceptInteractiveInput = acceptInteractiveInput;

		this.isNewMode = configuration.getString(CoreOptions.MODE).equalsIgnoreCase(CoreOptions.NEW_MODE);

		// Create the command line options

		query = new Option(shortPrefix + "q", longPrefix + "query", false, "Display available YARN resources (memory, cores)");
		applicationId = new Option(shortPrefix + "id", longPrefix + "applicationId", true, "Attach to running YARN session");
		queue = new Option(shortPrefix + "qu", longPrefix + "queue", true, "Specify YARN queue.");
		shipPath = new Option(shortPrefix + "t", longPrefix + "ship", true, "Ship files in the specified directory (t for transfer)");
		flinkJar = new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
		jmMemory = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container [in MB]");
		tmMemory = new Option(shortPrefix + "tm", longPrefix + "taskManagerMemory", true, "Memory per TaskManager Container [in MB]");
		container = new Option(shortPrefix + "n", longPrefix + "container", true, "Number of YARN container to allocate (=Number of Task Managers)");
		slots = new Option(shortPrefix + "s", longPrefix + "slots", true, "Number of slots per TaskManager");
		dynamicproperties = Option.builder(shortPrefix + "D")
			.argName("property=value")
			.numberOfArgs(2)
			.valueSeparator()
			.desc("use value for given property")
			.build();
		streaming = new Option(shortPrefix + "st", longPrefix + "streaming", false, "Start Flink in streaming mode");
		name = new Option(shortPrefix + "nm", longPrefix + "name", true, "Set a custom name for the application on YARN");
		zookeeperNamespace = new Option(shortPrefix + "z", longPrefix + "zookeeperNamespace", true, "Namespace to create the Zookeeper sub-paths for high availability mode");
		help = new Option(shortPrefix + "h", longPrefix + "help", false, "Help for the Yarn session CLI.");

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
		allOptions.addOption(DETACHED_OPTION);
		allOptions.addOption(YARN_DETACHED_OPTION);
		allOptions.addOption(streaming);
		allOptions.addOption(name);
		allOptions.addOption(applicationId);
		allOptions.addOption(zookeeperNamespace);
		allOptions.addOption(help);

		// try loading a potential yarn properties file
		this.yarnPropertiesFileLocation = configuration.getString(YarnConfigOptions.PROPERTIES_FILE_LOCATION);
		final File yarnPropertiesLocation = getYarnPropertiesLocation(yarnPropertiesFileLocation);

		yarnPropertiesFile = new Properties();

		if (yarnPropertiesLocation.exists()) {
			LOG.info("Found Yarn properties file under {}.", yarnPropertiesLocation.getAbsolutePath());

			try (InputStream is = new FileInputStream(yarnPropertiesLocation)) {
				yarnPropertiesFile.load(is);
			} catch (IOException ioe) {
				throw new FlinkException("Could not read the Yarn properties file " + yarnPropertiesLocation +
					". Please delete the file at " + yarnPropertiesLocation.getAbsolutePath() + '.', ioe);
			}

			final String yarnApplicationIdString = yarnPropertiesFile.getProperty(YARN_APPLICATION_ID_KEY);

			if (yarnApplicationIdString == null) {
				throw new FlinkException("Yarn properties file found but doesn't contain a " +
					"Yarn application id. Please delete the file at " + yarnPropertiesLocation.getAbsolutePath());
			}

			try {
				// try converting id to ApplicationId
				yarnApplicationIdFromYarnProperties = ConverterUtils.toApplicationId(yarnApplicationIdString);
			}
			catch (Exception e) {
				throw new FlinkException("YARN properties contains an invalid entry for " +
					"application id: " + yarnApplicationIdString + ". Please delete the file at " +
					yarnPropertiesLocation.getAbsolutePath(), e);
			}
		} else {
			yarnApplicationIdFromYarnProperties = null;
		}

		this.yarnConfiguration = new YarnConfiguration();
	}

	private AbstractYarnClusterDescriptor createDescriptor(
			Configuration configuration,
			YarnConfiguration yarnConfiguration,
			String configurationDirectory,
			CommandLine cmd) {

		AbstractYarnClusterDescriptor yarnClusterDescriptor = getClusterDescriptor(
			configuration,
			yarnConfiguration,
			configurationDirectory);

		// Jar Path
		final Path localJarPath;
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

			final String decodedPath;
			try {
				// we have to decode the url encoded parts of the path
				decodedPath = URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Couldn't decode the encoded Flink dist jar path: " + encodedJarPath +
					" Please supply a path manually via the -" + flinkJar.getOpt() + " option.");
			}

			// check whether it's actually a jar file --> when testing we execute this class without a flink-dist jar
			if (decodedPath.endsWith(".jar")) {
				localJarPath = new Path(new File(decodedPath).toURI());
			} else {
				localJarPath = null;
			}
		}

		if (localJarPath != null) {
			yarnClusterDescriptor.setLocalJarPath(localJarPath);
		}

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

		final Properties properties = cmd.getOptionProperties(dynamicproperties.getOpt());

		String[] dynamicProperties = properties.stringPropertyNames().stream()
			.flatMap(
				(String key) -> {
					final String value = properties.getProperty(key);

					if (value != null) {
						return Stream.of(key + dynamicproperties.getValueSeparator() + value);
					} else {
						return Stream.empty();
					}
				})
			.toArray(String[]::new);

		String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);

		yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);

		if (cmd.hasOption(YARN_DETACHED_OPTION.getOpt()) || cmd.hasOption(DETACHED_OPTION.getOpt())) {
			yarnClusterDescriptor.setDetachedMode(true);
		}

		if (cmd.hasOption(name.getOpt())) {
			yarnClusterDescriptor.setName(cmd.getOptionValue(name.getOpt()));
		}

		if (cmd.hasOption(zookeeperNamespace.getOpt())) {
			String zookeeperNamespaceValue = cmd.getOptionValue(this.zookeeperNamespace.getOpt());
			yarnClusterDescriptor.setZookeeperNamespace(zookeeperNamespaceValue);
		}

		return yarnClusterDescriptor;
	}

	private ClusterSpecification createClusterSpecification(Configuration configuration, CommandLine cmd) {
		if (!isNewMode && !cmd.hasOption(container.getOpt())) { // number of containers is required option!
			LOG.error("Missing required argument {}", container.getOpt());
			printUsage();
			throw new IllegalArgumentException("Missing required argument " + container.getOpt());
		}

		// TODO: The number of task manager should be deprecated soon
		final int numberTaskManagers;

		if (cmd.hasOption(container.getOpt())) {
			numberTaskManagers = Integer.valueOf(cmd.getOptionValue(container.getOpt()));
		} else {
			numberTaskManagers = 1;
		}

		// JobManager Memory
		final int jobManagerMemoryMB = configuration.getInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY);

		// Task Managers memory
		final int taskManagerMemoryMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);

		int slotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

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

	@Override
	public boolean isActive(CommandLine commandLine) {
		String jobManagerOption = commandLine.getOptionValue(addressOption.getOpt(), null);
		boolean yarnJobManager = ID.equals(jobManagerOption);
		boolean yarnAppId = commandLine.hasOption(applicationId.getOpt());
		return yarnJobManager || yarnAppId || (isYarnPropertiesFileMode(commandLine) && yarnApplicationIdFromYarnProperties != null);
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		super.addRunOptions(baseOptions);

		for (Object option : allOptions.getOptions()) {
			baseOptions.addOption((Option) option);
		}
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		super.addGeneralOptions(baseOptions);
		baseOptions.addOption(applicationId);
	}

	@Override
	public AbstractYarnClusterDescriptor createClusterDescriptor(CommandLine commandLine) throws FlinkException {
		final Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);

		return createDescriptor(
			effectiveConfiguration,
			yarnConfiguration,
			configurationDirectory,
			commandLine);
	}

	@Override
	@Nullable
	public ApplicationId getClusterId(CommandLine commandLine) {
		if (commandLine.hasOption(applicationId.getOpt())) {
			return ConverterUtils.toApplicationId(commandLine.getOptionValue(applicationId.getOpt()));
		} else if (isYarnPropertiesFileMode(commandLine)) {
			return yarnApplicationIdFromYarnProperties;
		} else {
			return null;
		}
	}

	@Override
	public ClusterSpecification getClusterSpecification(CommandLine commandLine) throws FlinkException {
		final Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);

		return createClusterSpecification(effectiveConfiguration, commandLine);
	}

	@Override
	protected Configuration applyCommandLineOptionsToConfiguration(CommandLine commandLine) throws FlinkException {
		// we ignore the addressOption because it can only contain "yarn-cluster"
		final Configuration effectiveConfiguration = new Configuration(configuration);

		if (commandLine.hasOption(zookeeperNamespaceOption.getOpt())) {
			String zkNamespace = commandLine.getOptionValue(zookeeperNamespaceOption.getOpt());
			effectiveConfiguration.setString(HA_CLUSTER_ID, zkNamespace);
		}

		final ApplicationId applicationId = getClusterId(commandLine);

		if (applicationId != null) {
			final String zooKeeperNamespace;
			if (commandLine.hasOption(zookeeperNamespace.getOpt())){
				zooKeeperNamespace = commandLine.getOptionValue(zookeeperNamespace.getOpt());
			} else {
				zooKeeperNamespace = effectiveConfiguration.getString(HA_CLUSTER_ID, applicationId.toString());
			}

			effectiveConfiguration.setString(HA_CLUSTER_ID, zooKeeperNamespace);
		}

		if (commandLine.hasOption(jmMemory.getOpt())) {
			effectiveConfiguration.setInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, Integer.parseInt(commandLine.getOptionValue(jmMemory.getOpt())));
		}

		if (commandLine.hasOption(tmMemory.getOpt())) {
			effectiveConfiguration.setInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, Integer.parseInt(commandLine.getOptionValue(tmMemory.getOpt())));
		}

		if (commandLine.hasOption(slots.getOpt())) {
			effectiveConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, Integer.parseInt(commandLine.getOptionValue(slots.getOpt())));
		}

		if (isYarnPropertiesFileMode(commandLine)) {
			return applyYarnProperties(effectiveConfiguration);
		} else {
			return effectiveConfiguration;
		}
	}

	private boolean isYarnPropertiesFileMode(CommandLine commandLine) {
		boolean canApplyYarnProperties = !commandLine.hasOption(addressOption.getOpt());

		for (Option option : commandLine.getOptions()) {
			if (allOptions.hasOption(option.getOpt())) {
				if (!isDetachedOption(option)) {
					// don't resume from properties file if yarn options have been specified
					canApplyYarnProperties = false;
					break;
				}
			}
		}

		return canApplyYarnProperties;
	}

	private boolean isDetachedOption(Option option) {
		return option.getOpt().equals(YARN_DETACHED_OPTION.getOpt()) || option.getOpt().equals(DETACHED_OPTION.getOpt());
	}

	private Configuration applyYarnProperties(Configuration configuration) throws FlinkException {
		final Configuration effectiveConfiguration = new Configuration(configuration);

		// configure the default parallelism from YARN
		String propParallelism = yarnPropertiesFile.getProperty(YARN_PROPERTIES_PARALLELISM);
		if (propParallelism != null) { // maybe the property is not set
			try {
				int parallelism = Integer.parseInt(propParallelism);
				effectiveConfiguration.setInteger(CoreOptions.DEFAULT_PARALLELISM, parallelism);

				logAndSysout("YARN properties set default parallelism to " + parallelism);
			}
			catch (NumberFormatException e) {
				throw new FlinkException("Error while parsing the YARN properties: " +
					"Property " + YARN_PROPERTIES_PARALLELISM + " is not an integer.", e);
			}
		}

		// handle the YARN client's dynamic properties
		String dynamicPropertiesEncoded = yarnPropertiesFile.getProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING);
		Map<String, String> dynamicProperties = getDynamicProperties(dynamicPropertiesEncoded);
		for (Map.Entry<String, String> dynamicProperty : dynamicProperties.entrySet()) {
			effectiveConfiguration.setString(dynamicProperty.getKey(), dynamicProperty.getValue());
		}

		return effectiveConfiguration;
	}

	public int run(String[] args) throws CliArgsException, FlinkException {
		//
		//	Command Line Options
		//
		final CommandLine cmd = parseCommandLineOptions(args, true);

		if (cmd.hasOption(help.getOpt())) {
			printUsage();
			return 0;
		}

		final AbstractYarnClusterDescriptor yarnClusterDescriptor = createClusterDescriptor(cmd);

		try {
			// Query cluster for metrics
			if (cmd.hasOption(query.getOpt())) {
				final String description = yarnClusterDescriptor.getClusterDescription();
				System.out.println(description);
				return 0;
			} else {
				final ClusterClient<ApplicationId> clusterClient;
				final ApplicationId yarnApplicationId;

				if (cmd.hasOption(applicationId.getOpt())) {
					yarnApplicationId = ConverterUtils.toApplicationId(cmd.getOptionValue(applicationId.getOpt()));

					clusterClient = yarnClusterDescriptor.retrieve(yarnApplicationId);
				} else {
					final ClusterSpecification clusterSpecification = getClusterSpecification(cmd);

					clusterClient = yarnClusterDescriptor.deploySessionCluster(clusterSpecification);

					//------------------ ClusterClient deployed, handle connection details
					yarnApplicationId = clusterClient.getClusterId();

					try {
						final LeaderConnectionInfo connectionInfo = clusterClient.getClusterConnectionInfo();

						System.out.println("Flink JobManager is now running on " + connectionInfo.getHostname() +
							':' + connectionInfo.getPort() + " with leader id " + connectionInfo.getLeaderSessionID() + '.');
						System.out.println("JobManager Web Interface: " + clusterClient.getWebInterfaceURL());

						writeYarnPropertiesFile(
							yarnApplicationId,
							clusterSpecification.getNumberTaskManagers() * clusterSpecification.getSlotsPerTaskManager(),
							yarnClusterDescriptor.getDynamicPropertiesEncoded());
					} catch (Exception e) {
						try {
							clusterClient.shutdown();
						} catch (Exception ex) {
							LOG.info("Could not properly shutdown cluster client.", ex);
						}

						try {
							yarnClusterDescriptor.killCluster(yarnApplicationId);
						} catch (FlinkException fe) {
							LOG.info("Could not properly terminate the Flink cluster.", fe);
						}

						throw new FlinkException("Could not write the Yarn connection information.", e);
					}
				}

				if (yarnClusterDescriptor.isDetachedMode()) {
					LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
						"Flink on YARN, use the following command or a YARN web interface to stop it:\n" +
						"yarn application -kill " + yarnApplicationId);
				} else {
					ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

					final YarnApplicationStatusMonitor yarnApplicationStatusMonitor = new YarnApplicationStatusMonitor(
						yarnClusterDescriptor.getYarnClient(),
						yarnApplicationId,
						new ScheduledExecutorServiceAdapter(scheduledExecutorService));

					try {
						runInteractiveCli(
							clusterClient,
							yarnApplicationStatusMonitor,
							acceptInteractiveInput);
					} finally {
						try {
							yarnApplicationStatusMonitor.close();
						} catch (Exception e) {
							LOG.info("Could not properly close the Yarn application status monitor.", e);
						}

						clusterClient.shutDownCluster();

						try {
							clusterClient.shutdown();
						} catch (Exception e) {
							LOG.info("Could not properly shutdown cluster client.", e);
						}

						// shut down the scheduled executor service
						ExecutorUtils.gracefulShutdown(
							1000L,
							TimeUnit.MILLISECONDS,
							scheduledExecutorService);

						deleteYarnPropertiesFile();

						ApplicationReport applicationReport;

						try {
							applicationReport = yarnClusterDescriptor
								.getYarnClient()
								.getApplicationReport(yarnApplicationId);
						} catch (YarnException | IOException e) {
							LOG.info("Could not log the final application report.", e);
							applicationReport = null;
						}

						if (applicationReport != null) {
							logFinalApplicationReport(applicationReport);
						}
					}
				}
			}
		} finally {
			try {
				yarnClusterDescriptor.close();
			} catch (Exception e) {
				LOG.info("Could not properly close the yarn cluster descriptor.", e);
			}
		}

		return 0;
	}

	private void logFinalApplicationReport(ApplicationReport appReport) {
		LOG.info("Application " + appReport.getApplicationId() + " finished with state " + appReport
			.getYarnApplicationState() + " and final state " + appReport
			.getFinalApplicationStatus() + " at " + appReport.getFinishTime());

		if (appReport.getYarnApplicationState() == YarnApplicationState.FAILED) {
			LOG.warn("Application failed. Diagnostics " + appReport.getDiagnostics());
			LOG.warn("If log aggregation is activated in the Hadoop cluster, we recommend to retrieve "
				+ "the full application log using this command:"
				+ System.lineSeparator()
				+ "\tyarn logs -applicationId " + appReport.getApplicationId()
				+ System.lineSeparator()
				+ "(It sometimes takes a few seconds until the logs are aggregated)");
		}
	}

	private void deleteYarnPropertiesFile() {
		// try to clean up the old yarn properties file
		try {
			File propertiesFile = getYarnPropertiesLocation(yarnPropertiesFileLocation);
			if (propertiesFile.isFile()) {
				if (propertiesFile.delete()) {
					LOG.info("Deleted Yarn properties file at {}", propertiesFile.getAbsoluteFile());
				} else {
					LOG.warn("Couldn't delete Yarn properties file at {}", propertiesFile.getAbsoluteFile());
				}
			}
		} catch (Exception e) {
			LOG.warn("Exception while deleting the JobManager address file", e);
		}
	}

	private void writeYarnPropertiesFile(
			ApplicationId yarnApplicationId,
			int parallelism,
			@Nullable String dynamicProperties) {
		// file that we write into the conf/ dir containing the jobManager address and the dop.
		final File yarnPropertiesFile = getYarnPropertiesLocation(yarnPropertiesFileLocation);

		Properties yarnProps = new Properties();
		yarnProps.setProperty(YARN_APPLICATION_ID_KEY, yarnApplicationId.toString());
		if (parallelism > 0) {
			yarnProps.setProperty(YARN_PROPERTIES_PARALLELISM, Integer.toString(parallelism));
		}

		// add dynamic properties
		if (dynamicProperties != null) {
			yarnProps.setProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING, dynamicProperties);
		}

		writeYarnProperties(yarnProps, yarnPropertiesFile);
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

	public static void main(final String[] args) {
		final String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();

		final Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration();

		int retCode;

		try {
			final FlinkYarnSessionCli cli = new FlinkYarnSessionCli(
				flinkConfiguration,
				configurationDirectory,
				"",
				""); // no prefix for the YARN session

			SecurityUtils.install(new SecurityConfiguration(flinkConfiguration));

			retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));
		} catch (CliArgsException e) {
			retCode = handleCliArgsException(e);
		} catch (Exception e) {
			retCode = handleError(e);
		}

		System.exit(retCode);
	}

	private static void runInteractiveCli(
			ClusterClient<?> clusterClient,
			YarnApplicationStatusMonitor yarnApplicationStatusMonitor,
			boolean readConsoleInput) {
		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			boolean continueRepl = true;
			int numTaskmanagers = 0;
			boolean isLastStatusUnknown = true;
			long unknownStatusSince = System.nanoTime();

			while (continueRepl) {

				final ApplicationStatus applicationStatus = yarnApplicationStatusMonitor.getApplicationStatusNow();

				switch (applicationStatus) {
					case FAILED:
					case CANCELED:
						System.err.println("The Flink Yarn cluster has failed.");
						continueRepl = false;
						break;
					case UNKNOWN:
						if (!isLastStatusUnknown) {
							unknownStatusSince = System.nanoTime();
							isLastStatusUnknown = true;
						}

						if ((System.nanoTime() - unknownStatusSince) > 5L * CLIENT_POLLING_INTERVAL_MS * 1_000_000L) {
							System.err.println("The Flink Yarn cluster is in an unknown state. Please check the Yarn cluster.");
							continueRepl = false;
						} else {
							continueRepl = repStep(in, readConsoleInput);
						}
						break;
					case SUCCEEDED:
						if (isLastStatusUnknown) {
							isLastStatusUnknown = false;
						}

						// ------------------ check if there are updates by the cluster -----------
						try {
							final GetClusterStatusResponse status = clusterClient.getClusterStatus();

							if (status != null && numTaskmanagers != status.numRegisteredTaskManagers()) {
								System.err.println("Number of connected TaskManagers changed to " +
									status.numRegisteredTaskManagers() + ". " +
									"Slots available: " + status.totalNumberOfSlots());
								numTaskmanagers = status.numRegisteredTaskManagers();
							}
						} catch (Exception e) {
							LOG.warn("Could not retrieve the current cluster status. Skipping current retrieval attempt ...", e);
						}

						printClusterMessages(clusterClient);

						continueRepl = repStep(in, readConsoleInput);
				}
			}
		} catch (Exception e) {
			LOG.warn("Exception while running the interactive command line interface.", e);
		}
	}

	private static void printClusterMessages(ClusterClient clusterClient) {
		final List<String> messages = clusterClient.getNewMessages();
		if (!messages.isEmpty()) {
			System.err.println("New messages from the YARN cluster: ");
			for (String msg : messages) {
				System.err.println(msg);
			}
		}
	}

	/**
	 * Read-Evaluate-Print step for the REPL.
	 *
	 * @param in to read from
	 * @param readConsoleInput true if console input has to be read
	 * @return true if the REPL shall be continued, otherwise false
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static boolean repStep(
			BufferedReader in,
			boolean readConsoleInput) throws IOException, InterruptedException {

		// wait until CLIENT_POLLING_INTERVAL is over or the user entered something.
		long startTime = System.currentTimeMillis();
		while ((System.currentTimeMillis() - startTime) < CLIENT_POLLING_INTERVAL_MS
			&& (!readConsoleInput || !in.ready())) {
			Thread.sleep(200L);
		}
		//------------- handle interactive command by user. ----------------------

		if (readConsoleInput && in.ready()) {
			String command = in.readLine();
			switch (command) {
				case "quit":
				case "stop":
					return false;

				case "help":
					System.err.println(YARN_SESSION_HELP);
					break;
				default:
					System.err.println("Unknown command '" + command + "'. Showing help:");
					System.err.println(YARN_SESSION_HELP);
					break;
			}
		}

		return true;
	}

	private static void writeYarnProperties(Properties properties, File propertiesFile) {
		try (final OutputStream out = new FileOutputStream(propertiesFile)) {
			properties.store(out, "Generated YARN properties file");
		} catch (IOException e) {
			throw new RuntimeException("Error writing the properties file", e);
		}
		propertiesFile.setReadable(true, false); // readable for all.
	}

	private static int handleCliArgsException(CliArgsException e) {
		LOG.error("Could not parse the command line arguments.", e);

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	private static int handleError(Exception e) {
		LOG.error("Error while running the Flink Yarn session.", e);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		e.printStackTrace();
		return 1;
	}

	public static File getYarnPropertiesLocation(@Nullable String yarnPropertiesFileLocation) {

		final String propertiesFileLocation;

		if (yarnPropertiesFileLocation != null) {
			propertiesFileLocation = yarnPropertiesFileLocation;
		} else {
			propertiesFileLocation = System.getProperty("java.io.tmpdir");
		}

		String currentUser = System.getProperty("user.name");

		return new File(propertiesFileLocation, YARN_PROPERTIES_FILE + currentUser);
	}

	private AbstractYarnClusterDescriptor getClusterDescriptor(
			Configuration configuration,
			YarnConfiguration yarnConfiguration,
			String configurationDirectory) {
		final YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(yarnConfiguration);
		yarnClient.start();

		if (isNewMode) {
			return new YarnClusterDescriptor(
				configuration,
				yarnConfiguration,
				configurationDirectory,
				yarnClient,
				false);
		} else {
			return new LegacyYarnClusterDescriptor(
				configuration,
				yarnConfiguration,
				configurationDirectory,
				yarnClient,
				false);
		}
	}
}
