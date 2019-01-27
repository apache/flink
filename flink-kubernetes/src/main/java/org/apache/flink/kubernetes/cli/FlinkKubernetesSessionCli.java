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

package org.apache.flink.kubernetes.cli;

import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.deploy.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.deploy.KubernetesClusterId;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.flink.client.cli.CliFrontendParser.ARGS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.kubernetes.configuration.Constants.SERVICE_NAME_SUFFIX;

/**
 * Class handling the command line interface to the Kubernetes session.
 */
public class FlinkKubernetesSessionCli extends AbstractCustomCommandLine<KubernetesClusterId> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKubernetesSessionCli.class);

	private static final long CLIENT_POLLING_INTERVAL_MS = 3000L;

	private static final String KUBERNETES_SESSION_HELP = "Available commands:\n" +
		"help - show these commands\n" +
		"stop - stop the kubernetes session";

	/** The id for the CommandLine interface. */
	private static final String ID = "kubernetes-cluster";

	//------------------------------------ Command Line argument options -------------------------
	private final Option serviceAddress;
	private final Option master;
	private final Option namespace;
	private final Option name;
	private final Option image;
	private final Option jmMemory;
	private final Option tmMemory;
	private final Option pods;
	private final Option slots;
	private final Option help;
	private final Options allOptions;

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.network.memory.min=536346624</tt>.
	 */
	private final Option dynamicproperties;

	private final String configurationDirectory;

	private int taskManagerContainerMemoryMB;

	public FlinkKubernetesSessionCli(
			Configuration configuration,
			String configurationDirectory,
			String shortPrefix,
			String longPrefix) {
		super(configuration);
		this.configurationDirectory = Preconditions.checkNotNull(configurationDirectory);

		// Create the command line options
		serviceAddress = new Option(shortPrefix + "sa", longPrefix + "serviceaddress", true,
			KubernetesConfigOptions.SERVICE_EXPOSED_ADDRESS.description());
		master = new Option(shortPrefix + "ms", longPrefix + "master", true, "Kubernetes cluster master url");
		namespace = new Option(shortPrefix + "ns", longPrefix + "namespace", true, "Specify kubernetes namespace.");
		name = new Option(shortPrefix + "nm", longPrefix + "name", true, "Set a custom name for the flink cluster on kubernetes");
		image = new Option(shortPrefix + "i", longPrefix + "image", true,
			KubernetesConfigOptions.CONTAINER_IMAGE.description());
		jmMemory = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container [in MB]");
		tmMemory = new Option(shortPrefix + "tm", longPrefix + "taskManagerMemory", true, "Memory per TaskManager Container [in MB]");
		pods = new Option(shortPrefix + "n", longPrefix + "pods", true, "Number of kubernetes pods to allocate (=Number of Task Managers)");
		slots = new Option(shortPrefix + "s", longPrefix + "slots", true, "Number of slots per TaskManager");
		dynamicproperties = Option.builder(shortPrefix + "D")
			.argName("property=value")
			.numberOfArgs(2)
			.valueSeparator()
			.desc("use value for given property")
			.build();
		help = new Option(shortPrefix + "h", longPrefix + "help", false, "Help for the kubernetes session CLI.");

		allOptions = new Options();
		allOptions.addOption(serviceAddress);
		allOptions.addOption(master);
		allOptions.addOption(namespace);
		allOptions.addOption(name);
		allOptions.addOption(image);
		allOptions.addOption(jmMemory);
		allOptions.addOption(tmMemory);
		allOptions.addOption(pods);
		allOptions.addOption(slots);
		allOptions.addOption(dynamicproperties);
		allOptions.addOption(DETACHED_OPTION);
		allOptions.addOption(help);
	}

	private ClusterSpecification createClusterSpecification(Configuration configuration, CommandLine cmd) {
		final int numberTaskManagers;

		if (cmd.hasOption(pods.getOpt())) {
			numberTaskManagers = Integer.valueOf(cmd.getOptionValue(pods.getOpt()));
		} else {
			numberTaskManagers = 1;
		}

		// JobManager Memory
		final int jobManagerMemoryMB = configuration.getInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY);

		// Task Managers memory
		final int taskManagerMemoryMB = taskManagerContainerMemoryMB;

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
		req.addOption(master);
		req.addOption(pods);
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
		return ID.equals(jobManagerOption);
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
	}

	@Override
	public KubernetesClusterDescriptor createClusterDescriptor(CommandLine commandLine) throws FlinkException {
		Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
		return new KubernetesClusterDescriptor(effectiveConfiguration, configurationDirectory);
	}

	@Override
	@Nullable
	public KubernetesClusterId getClusterId(CommandLine commandLine) {
		if (commandLine.getOptionValue(serviceAddress.getOpt()) != null) {
			return KubernetesClusterId.fromString(commandLine.getOptionValue(name.getOpt()));
		} else {
			return null;
		}
	}

	@Override
	public ClusterSpecification getClusterSpecification(CommandLine commandLine) {
		final Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
		return createClusterSpecification(effectiveConfiguration, commandLine);
	}

	@Override
	protected Configuration applyCommandLineOptionsToConfiguration(CommandLine commandLine) {
		final Configuration effectiveConfiguration = new Configuration(configuration);

		if (commandLine.hasOption(serviceAddress.getOpt())) {
			effectiveConfiguration.setString(KubernetesConfigOptions.SERVICE_EXPOSED_ADDRESS,
				commandLine.getOptionValue(serviceAddress.getOpt()));
		}

		if (commandLine.hasOption(master.getOpt())) {
			effectiveConfiguration.setString(KubernetesConfigOptions.MASTER_URL,
				commandLine.getOptionValue(master.getOpt()));
		}

		if (commandLine.hasOption(namespace.getOpt())) {
			effectiveConfiguration.setString(KubernetesConfigOptions.NAME_SPACE,
				commandLine.getOptionValue(namespace.getOpt()));
		}

		if (commandLine.hasOption(name.getOpt())) {
			effectiveConfiguration.setString(KubernetesConfigOptions.CLUSTER_ID,
				commandLine.getOptionValue(name.getOpt()));
		}

		if (commandLine.hasOption(image.getOpt())) {
			effectiveConfiguration.setString(KubernetesConfigOptions.CONTAINER_IMAGE,
				commandLine.getOptionValue(image.getOpt()));
		}

		if (commandLine.hasOption(jmMemory.getOpt())) {
			effectiveConfiguration.setInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY,
				Integer.parseInt(commandLine.getOptionValue(jmMemory.getOpt())));
		}

		if (commandLine.hasOption(tmMemory.getOpt())) {
			taskManagerContainerMemoryMB = Integer.parseInt(commandLine.getOptionValue(tmMemory.getOpt()));
		} else {
			taskManagerContainerMemoryMB = -1;
		}

		if (commandLine.hasOption(pods.getOpt())) {
			effectiveConfiguration.setInteger(KubernetesConfigOptions.TASK_MANAGER_COUNT,
				Integer.parseInt(commandLine.getOptionValue(pods.getOpt())));
		}

		if (commandLine.hasOption(slots.getOpt())) {
			effectiveConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS,
				Integer.parseInt(commandLine.getOptionValue(slots.getOpt())));
		}

		String[] progArgs = commandLine.getOptionValues(ARGS_OPTION.getOpt());
		if (progArgs == null && commandLine.getArgs().length > 0) {
			progArgs = Arrays.copyOfRange(commandLine.getArgs(), 1, commandLine.getArgs().length);
		}
		if (progArgs != null) {
			effectiveConfiguration.setString(KubernetesConfigOptions.USER_PROGRAM_ARGS,
				StringUtils.join(progArgs, " "));
		}

		if (commandLine.hasOption(CLASS_OPTION.getOpt())) {
			effectiveConfiguration.setString(KubernetesConfigOptions.USER_PROGRAM_ENTRYPOINT_CLASS,
				commandLine.getOptionValue(CLASS_OPTION.getOpt()));
		}

		// apply dynamic properties
		final Properties properties = commandLine.getOptionProperties(dynamicproperties.getOpt());
		properties.stringPropertyNames().forEach(
			key -> {
				final String value = properties.getProperty(key);
				if (value != null) {
					effectiveConfiguration.setString(key, value);
				}
			});
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

		final KubernetesClusterDescriptor kubernetesClusterDescriptor = createClusterDescriptor(cmd);

		try {
			final ClusterClient<KubernetesClusterId> clusterClient;
			final KubernetesClusterId clusterId;

			// retrieve an existing session cluster
			if (cmd.hasOption(name.getOpt()) && cmd.getOptionValue(serviceAddress.getOpt()) != null) {
				clusterId = KubernetesClusterId.fromString(cmd.getOptionValue(name.getOpt()));

				clusterClient = kubernetesClusterDescriptor.retrieve(clusterId);
			} else {
				// create a new one
				final ClusterSpecification clusterSpecification = getClusterSpecification(cmd);

				clusterClient = kubernetesClusterDescriptor.deploySessionCluster(clusterSpecification);

				//------------------ ClusterClient deployed, handle connection details
				clusterId = clusterClient.getClusterId();

				try {
					final LeaderConnectionInfo connectionInfo = clusterClient.getClusterConnectionInfo();

					System.out.println("Flink JobManager is now running on " + connectionInfo.getHostname() +
						':' + connectionInfo.getPort() + " with leader id " + connectionInfo.getLeaderSessionID() + '.');
					System.out.println("JobManager Web Interface: " + clusterClient.getWebInterfaceURL());
				} catch (Exception e) {
					try {
						clusterClient.shutdown();
					} catch (Exception ex) {
						LOG.info("Could not properly shutdown cluster client.", ex);
					}

					kubernetesClusterDescriptor.killCluster(clusterId);

					throw new FlinkException("Could not write the kubernetes connection information.", e);
				}
			}
			try {
				if (cmd.hasOption(DETACHED_OPTION.getOpt())) {
					LOG.info("The Flink kubernetes client has been started in detached mode. In order to stop " +
						"Flink on kubernetes, please attach to the session or use command to stop it : " +
						"kubectl delete service {}.", clusterId.toString() + SERVICE_NAME_SUFFIX);
				} else {
					boolean continueRepl = true;
					try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
						while (continueRepl) {
							continueRepl = repStep(in);
						}
					} catch (Exception e) {
						LOG.warn("Exception while running the interactive command line interface.", e);
					}
					clusterClient.shutDownCluster();
				}
				clusterClient.shutdown();
			} catch (Exception e) {
				LOG.info("Could not properly shutdown cluster client.", e);
			}
		} finally {
			try {
				kubernetesClusterDescriptor.close();
			} catch (Exception e) {
				LOG.info("Could not properly close the kubernetes cluster descriptor.", e);
			}
		}

		return 0;
	}

	public static void main(final String[] args) {
		final String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();

		final Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration();

		int retCode;

		try {
			final FlinkKubernetesSessionCli cli = new FlinkKubernetesSessionCli(
				flinkConfiguration,
				configurationDirectory,
				"",
				"");

			SecurityUtils.install(new SecurityConfiguration(flinkConfiguration));

			retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));
		} catch (CliArgsException e) {
			retCode = handleCliArgsException(e);
		} catch (Exception e) {
			retCode = handleError(e);
		}

		System.exit(retCode);
	}

	private static boolean repStep(BufferedReader in) throws IOException, InterruptedException {

		// wait until CLIENT_POLLING_INTERVAL is over or the user entered something.
		long startTime = System.currentTimeMillis();
		while ((System.currentTimeMillis() - startTime) < CLIENT_POLLING_INTERVAL_MS
			&& (!in.ready())) {
			Thread.sleep(200L);
		}
		//------------- handle interactive command by user. ----------------------

		if (in.ready()) {
			String command = in.readLine();
			switch (command) {
				case "quit":
				case "stop":
					return false;

				case "help":
					System.err.println(KUBERNETES_SESSION_HELP);
					break;
				default:
					System.err.println("Unknown command '" + command + "'. Showing help:");
					System.err.println(KUBERNETES_SESSION_HELP);
					break;
			}
		}

		return true;
	}

	private static int handleCliArgsException(CliArgsException e) {
		LOG.error("Could not parse the command line arguments.", e);

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	private static int handleError(Exception e) {
		LOG.error("Error while running the Flink kubernetes session.", e);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		e.printStackTrace();
		return 1;
	}
}
