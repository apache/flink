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
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;
import org.apache.flink.yarn.executors.YarnJobClusterExecutor;
import org.apache.flink.yarn.executors.YarnSessionClusterExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
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
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.YARN_DETACHED_OPTION;
import static org.apache.flink.configuration.HighAvailabilityOptions.HA_CLUSTER_ID;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class handling the command line interface to the YARN session.
 */
public class FlinkYarnSessionCli extends AbstractCustomCommandLine {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnSessionCli.class);

	//------------------------------------ Constants   -------------------------

	private static final long CLIENT_POLLING_INTERVAL_MS = 3000L;

	/** The id for the CommandLine interface. */
	private static final String ID = "yarn-cluster";

	// YARN-session related constants
	private static final String YARN_PROPERTIES_FILE = ".yarn-properties-";
	private static final String YARN_APPLICATION_ID_KEY = "applicationID";
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
	private final Option slots;
	private final Option zookeeperNamespace;
	private final Option nodeLabel;
	private final Option help;
	private final Option name;
	private final Option applicationType;

	private final Options allOptions;

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.memory.network.min=536346624</tt>.
	 */
	private final Option dynamicproperties;

	private final boolean acceptInteractiveInput;

	private final String configurationDirectory;

	private final Properties yarnPropertiesFile;

	private final ApplicationId yarnApplicationIdFromYarnProperties;

	private final String yarnPropertiesFileLocation;

	private final ClusterClientServiceLoader clusterClientServiceLoader;

	@Nullable
	private String dynamicPropertiesEncoded = null;

	public FlinkYarnSessionCli(
			Configuration configuration,
			String configurationDirectory,
			String shortPrefix,
			String longPrefix) throws FlinkException {
		this(configuration, new DefaultClusterClientServiceLoader(), configurationDirectory, shortPrefix, longPrefix, true);
	}

	public FlinkYarnSessionCli(
			Configuration configuration,
			String configurationDirectory,
			String shortPrefix,
			String longPrefix,
			boolean acceptInteractiveInput) throws FlinkException {
		this(configuration, new DefaultClusterClientServiceLoader(), configurationDirectory, shortPrefix, longPrefix, acceptInteractiveInput);
	}

	public FlinkYarnSessionCli(
			Configuration configuration,
			ClusterClientServiceLoader clusterClientServiceLoader,
			String configurationDirectory,
			String shortPrefix,
			String longPrefix,
			boolean acceptInteractiveInput) throws FlinkException {
		super(configuration);
		this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);
		this.configurationDirectory = checkNotNull(configurationDirectory);
		this.acceptInteractiveInput = acceptInteractiveInput;

		// Create the command line options

		query = new Option(shortPrefix + "q", longPrefix + "query", false, "Display available YARN resources (memory, cores)");
		applicationId = new Option(shortPrefix + "id", longPrefix + "applicationId", true, "Attach to running YARN session");
		queue = new Option(shortPrefix + "qu", longPrefix + "queue", true, "Specify YARN queue.");
		shipPath = new Option(shortPrefix + "t", longPrefix + "ship", true, "Ship files in the specified directory (t for transfer)");
		flinkJar = new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
		jmMemory = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container with optional unit (default: MB)");
		tmMemory = new Option(shortPrefix + "tm", longPrefix + "taskManagerMemory", true, "Memory per TaskManager Container with optional unit (default: MB)");
		slots = new Option(shortPrefix + "s", longPrefix + "slots", true, "Number of slots per TaskManager");
		dynamicproperties = Option.builder(shortPrefix + "D")
			.argName("property=value")
			.numberOfArgs(2)
			.valueSeparator()
			.desc("use value for given property")
			.build();
		name = new Option(shortPrefix + "nm", longPrefix + "name", true, "Set a custom name for the application on YARN");
		applicationType = new Option(shortPrefix + "at", longPrefix + "applicationType", true, "Set a custom application type for the application on YARN");
		zookeeperNamespace = new Option(shortPrefix + "z", longPrefix + "zookeeperNamespace", true, "Namespace to create the Zookeeper sub-paths for high availability mode");
		nodeLabel = new Option(shortPrefix + "nl", longPrefix + "nodeLabel", true, "Specify YARN node label for the YARN application");
		help = new Option(shortPrefix + "h", longPrefix + "help", false, "Help for the Yarn session CLI.");

		allOptions = new Options();
		allOptions.addOption(flinkJar);
		allOptions.addOption(jmMemory);
		allOptions.addOption(tmMemory);
		allOptions.addOption(queue);
		allOptions.addOption(query);
		allOptions.addOption(shipPath);
		allOptions.addOption(slots);
		allOptions.addOption(dynamicproperties);
		allOptions.addOption(DETACHED_OPTION);
		allOptions.addOption(YARN_DETACHED_OPTION);
		allOptions.addOption(name);
		allOptions.addOption(applicationId);
		allOptions.addOption(applicationType);
		allOptions.addOption(zookeeperNamespace);
		allOptions.addOption(nodeLabel);
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
				throw new FlinkException("YARN properties contain an invalid entry for " +
					"application id: " + yarnApplicationIdString + ". Please delete the file at " +
					yarnPropertiesLocation.getAbsolutePath(), e);
			}
		} else {
			yarnApplicationIdFromYarnProperties = null;
		}
	}

	private Path getLocalFlinkDistPathFromCmd(final CommandLine cmd) {
		final String flinkJarOptionName = flinkJar.getOpt();
		if (!cmd.hasOption(flinkJarOptionName)) {
			return null;
		}

		String userPath = cmd.getOptionValue(flinkJarOptionName);
		if (!userPath.startsWith("file://")) {
			userPath = "file://" + userPath;
		}
		return new Path(userPath);
	}

	private void encodeDirsToShipToCluster(final Configuration configuration, final CommandLine cmd) {
		checkNotNull(cmd);
		checkNotNull(configuration);

		if (cmd.hasOption(shipPath.getOpt())) {
			ConfigUtils.encodeArrayToConfig(
					configuration,
					YarnConfigOptions.SHIP_DIRECTORIES,
					cmd.getOptionValues(this.shipPath.getOpt()),
					(String path) -> {
						final File shipDir = new File(path);
						if (shipDir.isDirectory()) {
							return path;
						}
						LOG.warn("Ship directory {} is not a directory. Ignoring it.", shipDir.getAbsolutePath());
						return null;
					});
		}
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		final String jobManagerOption = commandLine.getOptionValue(addressOption.getOpt(), null);
		final boolean yarnJobManager = ID.equals(jobManagerOption);
		final boolean hasYarnAppId = commandLine.hasOption(applicationId.getOpt())
				|| configuration.getOptional(YarnConfigOptions.APPLICATION_ID).isPresent();
		final boolean hasYarnExecutor = YarnSessionClusterExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET))
				|| YarnJobClusterExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
		return hasYarnExecutor || yarnJobManager || hasYarnAppId || (isYarnPropertiesFileMode(commandLine) && yarnApplicationIdFromYarnProperties != null);
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
	public Configuration applyCommandLineOptionsToConfiguration(CommandLine commandLine) throws FlinkException {
		// we ignore the addressOption because it can only contain "yarn-cluster"
		final Configuration effectiveConfiguration = new Configuration(configuration);

		applyDescriptorOptionToConfig(commandLine, effectiveConfiguration);

		final ApplicationId applicationId = getApplicationId(commandLine);
		if (applicationId != null) {
			final String zooKeeperNamespace;
			if (commandLine.hasOption(zookeeperNamespace.getOpt())){
				zooKeeperNamespace = commandLine.getOptionValue(zookeeperNamespace.getOpt());
			} else {
				zooKeeperNamespace = effectiveConfiguration.getString(HA_CLUSTER_ID, applicationId.toString());
			}

			effectiveConfiguration.setString(HA_CLUSTER_ID, zooKeeperNamespace);
			effectiveConfiguration.setString(YarnConfigOptions.APPLICATION_ID, ConverterUtils.toString(applicationId));
			effectiveConfiguration.setString(DeploymentOptions.TARGET, YarnSessionClusterExecutor.NAME);
		} else {
			effectiveConfiguration.setString(DeploymentOptions.TARGET, YarnJobClusterExecutor.NAME);
		}

		if (commandLine.hasOption(jmMemory.getOpt())) {
			String jmMemoryVal = commandLine.getOptionValue(jmMemory.getOpt());
			if (!MemorySize.MemoryUnit.hasUnit(jmMemoryVal)) {
				jmMemoryVal += "m";
			}
			effectiveConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(jmMemoryVal));
		}

		if (commandLine.hasOption(tmMemory.getOpt())) {
			String tmMemoryVal = commandLine.getOptionValue(tmMemory.getOpt());
			if (!MemorySize.MemoryUnit.hasUnit(tmMemoryVal)) {
				tmMemoryVal += "m";
			}
			effectiveConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(tmMemoryVal));
		}

		if (commandLine.hasOption(slots.getOpt())) {
			effectiveConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, Integer.parseInt(commandLine.getOptionValue(slots.getOpt())));
		}

		dynamicPropertiesEncoded = encodeDynamicProperties(commandLine);
		if (!dynamicPropertiesEncoded.isEmpty()) {
			Map<String, String> dynProperties = getDynamicProperties(dynamicPropertiesEncoded);
			for (Map.Entry<String, String> dynProperty : dynProperties.entrySet()) {
				effectiveConfiguration.setString(dynProperty.getKey(), dynProperty.getValue());
			}
		}

		if (isYarnPropertiesFileMode(commandLine)) {
			return applyYarnProperties(effectiveConfiguration);
		} else {
			return effectiveConfiguration;
		}
	}

	private ApplicationId getApplicationId(CommandLine commandLine) {
		if (commandLine.hasOption(applicationId.getOpt())) {
			return ConverterUtils.toApplicationId(commandLine.getOptionValue(applicationId.getOpt()));
		} else if (configuration.getOptional(YarnConfigOptions.APPLICATION_ID).isPresent()) {
			return ConverterUtils.toApplicationId(configuration.get(YarnConfigOptions.APPLICATION_ID));
		} else if (isYarnPropertiesFileMode(commandLine)) {
			return yarnApplicationIdFromYarnProperties;
		}
		return null;
	}

	private void applyDescriptorOptionToConfig(final CommandLine commandLine, final Configuration configuration) {
		checkNotNull(commandLine);
		checkNotNull(configuration);

		final Path localJarPath = getLocalFlinkDistPathFromCmd(commandLine);
		if (localJarPath != null) {
			configuration.setString(YarnConfigOptions.FLINK_DIST_JAR, localJarPath.toString());
		}

		encodeDirsToShipToCluster(configuration, commandLine);

		if (commandLine.hasOption(queue.getOpt())) {
			final String queueName = commandLine.getOptionValue(queue.getOpt());
			configuration.setString(YarnConfigOptions.APPLICATION_QUEUE, queueName);
		}

		final boolean detached = commandLine.hasOption(YARN_DETACHED_OPTION.getOpt()) || commandLine.hasOption(DETACHED_OPTION.getOpt());
		configuration.setBoolean(DeploymentOptions.ATTACHED, !detached);

		if (commandLine.hasOption(name.getOpt())) {
			final String appName = commandLine.getOptionValue(name.getOpt());
			configuration.setString(YarnConfigOptions.APPLICATION_NAME, appName);
		}

		if (commandLine.hasOption(applicationType.getOpt())) {
			final String appType = commandLine.getOptionValue(applicationType.getOpt());
			configuration.setString(YarnConfigOptions.APPLICATION_TYPE, appType);
		}

		if (commandLine.hasOption(zookeeperNamespace.getOpt())) {
			String zookeeperNamespaceValue = commandLine.getOptionValue(zookeeperNamespace.getOpt());
			configuration.setString(HA_CLUSTER_ID, zookeeperNamespaceValue);
		} else if (commandLine.hasOption(zookeeperNamespaceOption.getOpt())) {
			String zookeeperNamespaceValue = commandLine.getOptionValue(zookeeperNamespaceOption.getOpt());
			configuration.setString(HA_CLUSTER_ID, zookeeperNamespaceValue);
		}

		if (commandLine.hasOption(nodeLabel.getOpt())) {
			final String nodeLabelValue = commandLine.getOptionValue(this.nodeLabel.getOpt());
			configuration.setString(YarnConfigOptions.NODE_LABEL, nodeLabelValue);
		}

		YarnLogConfigUtil.setLogConfigFileInConfig(configuration, configurationDirectory);
	}

	private boolean isYarnPropertiesFileMode(CommandLine commandLine) {
		boolean canApplyYarnProperties = !commandLine.hasOption(addressOption.getOpt());

		if (canApplyYarnProperties) {
			for (Option option : commandLine.getOptions()) {
				if (allOptions.hasOption(option.getOpt())) {
					if (!isDetachedOption(option)) {
						// don't resume from properties file if yarn options have been specified
						canApplyYarnProperties = false;
						break;
					}
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

		String applicationId = yarnPropertiesFile.getProperty(YARN_APPLICATION_ID_KEY);
		if (applicationId != null) {
			effectiveConfiguration.setString(YarnConfigOptions.APPLICATION_ID, applicationId);
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

		final Configuration configuration = applyCommandLineOptionsToConfiguration(cmd);
		final ClusterClientFactory<ApplicationId> yarnClusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(configuration);

		final YarnClusterDescriptor yarnClusterDescriptor = (YarnClusterDescriptor) yarnClusterClientFactory.createClusterDescriptor(configuration);

		try {
			// Query cluster for metrics
			if (cmd.hasOption(query.getOpt())) {
				final String description = yarnClusterDescriptor.getClusterDescription();
				System.out.println(description);
				return 0;
			} else {
				final ClusterClientProvider<ApplicationId> clusterClientProvider;
				final ApplicationId yarnApplicationId;

				if (cmd.hasOption(applicationId.getOpt())) {
					yarnApplicationId = ConverterUtils.toApplicationId(cmd.getOptionValue(applicationId.getOpt()));

					clusterClientProvider = yarnClusterDescriptor.retrieve(yarnApplicationId);
				} else {
					final ClusterSpecification clusterSpecification = yarnClusterClientFactory.getClusterSpecification(configuration);

					clusterClientProvider = yarnClusterDescriptor.deploySessionCluster(clusterSpecification);
					ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();

					//------------------ ClusterClient deployed, handle connection details
					yarnApplicationId = clusterClient.getClusterId();

					try {
						System.out.println("JobManager Web Interface: " + clusterClient.getWebInterfaceURL());

						writeYarnPropertiesFile(
							yarnApplicationId,
							dynamicPropertiesEncoded);
					} catch (Exception e) {
						try {
							clusterClient.close();
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

				if (!configuration.getBoolean(DeploymentOptions.ATTACHED)) {
					YarnClusterDescriptor.logDetachedClusterInformation(yarnApplicationId, LOG);
				} else {
					ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

					final YarnApplicationStatusMonitor yarnApplicationStatusMonitor = new YarnApplicationStatusMonitor(
						yarnClusterDescriptor.getYarnClient(),
						yarnApplicationId,
						new ScheduledExecutorServiceAdapter(scheduledExecutorService));
					Thread shutdownHook = ShutdownHookUtil.addShutdownHook(
						() -> shutdownCluster(
								clusterClientProvider.getClusterClient(),
								scheduledExecutorService,
								yarnApplicationStatusMonitor),
								getClass().getSimpleName(),
								LOG);
					try {
						runInteractiveCli(
							yarnApplicationStatusMonitor,
							acceptInteractiveInput);
					} finally {
						shutdownCluster(
								clusterClientProvider.getClusterClient(),
								scheduledExecutorService,
								yarnApplicationStatusMonitor);

						if (shutdownHook != null) {
							// we do not need the hook anymore as we have just tried to shutdown the cluster.
							ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
						}

						tryRetrieveAndLogApplicationReport(
							yarnClusterDescriptor.getYarnClient(),
							yarnApplicationId);
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

	private void shutdownCluster(
			ClusterClient clusterClient,
			ScheduledExecutorService scheduledExecutorService,
			YarnApplicationStatusMonitor yarnApplicationStatusMonitor) {
		try {
			yarnApplicationStatusMonitor.close();
		} catch (Exception e) {
			LOG.info("Could not properly close the Yarn application status monitor.", e);
		}

		clusterClient.shutDownCluster();

		try {
			clusterClient.close();
		} catch (Exception e) {
			LOG.info("Could not properly shutdown cluster client.", e);
		}

		// shut down the scheduled executor service
		ExecutorUtils.gracefulShutdown(
			1000L,
			TimeUnit.MILLISECONDS,
			scheduledExecutorService);

		deleteYarnPropertiesFile();
	}

	private void tryRetrieveAndLogApplicationReport(YarnClient yarnClient, ApplicationId yarnApplicationId) {
		ApplicationReport applicationReport;

		try {
			applicationReport = yarnClient.getApplicationReport(yarnApplicationId);
		} catch (YarnException | IOException e) {
			LOG.info("Could not log the final application report.", e);
			applicationReport = null;
		}

		if (applicationReport != null) {
			logApplicationReport(applicationReport);
		}
	}

	private void logApplicationReport(ApplicationReport appReport) {
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
			@Nullable String dynamicProperties) {
		// file that we write into the conf/ dir containing the jobManager address and the dop.
		final File yarnPropertiesFile = getYarnPropertiesLocation(yarnPropertiesFileLocation);

		Properties yarnProps = new Properties();
		yarnProps.setProperty(YARN_APPLICATION_ID_KEY, yarnApplicationId.toString());

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

	private String encodeDynamicProperties(final CommandLine cmd) {
		final Properties properties = cmd.getOptionProperties(dynamicproperties.getOpt());
		final String[] dynamicProperties = properties.stringPropertyNames().stream()
				.flatMap(
						(String key) -> {
							final String value = properties.getProperty(key);

							LOG.info("Dynamic Property set: {}={}", key, GlobalConfiguration.isSensitive(key) ? GlobalConfiguration.HIDDEN_CONTENT : value);

							if (value != null) {
								return Stream.of(key + dynamicproperties.getValueSeparator() + value);
							} else {
								return Stream.empty();
							}
						})
				.toArray(String[]::new);

		return StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);
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
			retCode = handleCliArgsException(e, LOG);
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			retCode = handleError(strippedThrowable, LOG);
		}

		System.exit(retCode);
	}

	private static void runInteractiveCli(
			YarnApplicationStatusMonitor yarnApplicationStatusMonitor,
			boolean readConsoleInput) {
		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			boolean continueRepl = true;
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

						continueRepl = repStep(in, readConsoleInput);
				}
			}
		} catch (Exception e) {
			LOG.warn("Exception while running the interactive command line interface.", e);
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
}
