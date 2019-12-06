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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.configuration.HighAvailabilityOptions.HA_CLUSTER_ID;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.CLUSTER_ID_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.HELP_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.IMAGE_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.JOB_CLASS_NAME_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.JOB_ID_OPTION;

/**
 * Kubernetes customized commandline.
 */
public class FlinkKubernetesCustomCli extends AbstractCustomCommandLine {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKubernetesCustomCli.class);

	private static final long CLIENT_POLLING_INTERVAL_MS = 3000L;

	private static final String KUBERNETES_CLUSTER_HELP = "Available commands:\n" +
		"help - show these commands\n" +
		"stop - stop the kubernetes cluster\n" +
		"quit - quit attach mode";

	private static final String ID = "kubernetes-cluster";

	// Options with short prefix(k) or long prefix(kubernetes)
	private final Option imageOption;
	private final Option clusterIdOption;
	private final Option jobManagerMemoryOption;
	private final Option taskManagerMemoryOption;
	private final Option taskManagerSlotsOption;
	private final Option dynamicPropertiesOption;
	private final Option helpOption;

	private final Option jobClassOption;
	private final Option jobIdOption;

	private final ClusterClientServiceLoader clusterClientServiceLoader;

	public FlinkKubernetesCustomCli(Configuration configuration, String shortPrefix, String longPrefix) {
		this(configuration, new DefaultClusterClientServiceLoader(), shortPrefix, longPrefix);
	}

	public FlinkKubernetesCustomCli(
			Configuration configuration,
			ClusterClientServiceLoader clusterClientServiceLoader,
			String shortPrefix,
			String longPrefix) {
		super(configuration);

		this.clusterClientServiceLoader = clusterClientServiceLoader;

		this.imageOption = KubernetesCliOptions.getOptionWithPrefix(IMAGE_OPTION, shortPrefix, longPrefix);
		this.clusterIdOption = KubernetesCliOptions.getOptionWithPrefix(CLUSTER_ID_OPTION, shortPrefix, longPrefix);
		this.dynamicPropertiesOption = KubernetesCliOptions.getOptionWithPrefix(
			DYNAMIC_PROPERTY_OPTION, shortPrefix, longPrefix);

		this.jobManagerMemoryOption = KubernetesCliOptions.getOptionWithPrefix(
			KubernetesCliOptions.JOB_MANAGER_MEMORY_OPTION, shortPrefix, longPrefix);
		this.taskManagerMemoryOption = KubernetesCliOptions.getOptionWithPrefix(
			KubernetesCliOptions.TASK_MANAGER_MEMORY_OPTION, shortPrefix, longPrefix);
		this.taskManagerSlotsOption = KubernetesCliOptions.getOptionWithPrefix(
			KubernetesCliOptions.TASK_MANAGER_SLOTS_OPTION, shortPrefix, longPrefix);

		this.jobClassOption = KubernetesCliOptions.getOptionWithPrefix(JOB_CLASS_NAME_OPTION, shortPrefix, longPrefix);
		this.jobIdOption = KubernetesCliOptions.getOptionWithPrefix(JOB_ID_OPTION, shortPrefix, longPrefix);

		this.helpOption = KubernetesCliOptions.getOptionWithPrefix(HELP_OPTION, shortPrefix, longPrefix);
	}

	/**
	 * active if commandline contains option -m kubernetes-cluster or -kid.
	 */
	@Override
	public boolean isActive(CommandLine commandLine) {
		final String jobManagerOption = commandLine.getOptionValue(addressOption.getOpt(), null);
		final boolean k8sJobManager = ID.equals(jobManagerOption);
		final boolean k8sClusterId = commandLine.hasOption(clusterIdOption.getOpt());
		return k8sJobManager || k8sClusterId;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		baseOptions.addOption(DETACHED_OPTION)
			.addOption(imageOption)
			.addOption(clusterIdOption)
			.addOption(jobManagerMemoryOption)
			.addOption(taskManagerMemoryOption)
			.addOption(taskManagerSlotsOption)
			.addOption(dynamicPropertiesOption)
			.addOption(helpOption);
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(clusterIdOption);
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public Configuration applyCommandLineOptionsToConfiguration(CommandLine commandLine) {
		final Configuration effectiveConfiguration = new Configuration(configuration);

		effectiveConfiguration.setString(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);

		if (commandLine.hasOption(clusterIdOption.getOpt())) {
			final String clusterId = commandLine.getOptionValue(clusterIdOption.getOpt());
			effectiveConfiguration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
			effectiveConfiguration.setString(HA_CLUSTER_ID, clusterId);
		}

		if (commandLine.hasOption(imageOption.getOpt())) {
			effectiveConfiguration.setString(KubernetesConfigOptions.CONTAINER_IMAGE,
				commandLine.getOptionValue(imageOption.getOpt()));
		}

		if (commandLine.hasOption(jobManagerMemoryOption.getOpt())) {
			String jmMemoryVal = commandLine.getOptionValue(jobManagerMemoryOption.getOpt());
			if (!MemorySize.MemoryUnit.hasUnit(jmMemoryVal)) {
				jmMemoryVal += "m";
			}
			effectiveConfiguration.setString(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, jmMemoryVal);
		}

		if (commandLine.hasOption(taskManagerMemoryOption.getOpt())) {
			String tmMemoryVal = commandLine.getOptionValue(taskManagerMemoryOption.getOpt());
			if (!MemorySize.MemoryUnit.hasUnit(tmMemoryVal)) {
				tmMemoryVal += "m";
			}
			effectiveConfiguration.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY, tmMemoryVal);
		}

		if (commandLine.hasOption(taskManagerSlotsOption.getOpt())) {
			effectiveConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS,
				Integer.parseInt(commandLine.getOptionValue(taskManagerSlotsOption.getOpt())));
		}

		final Properties dynamicProperties = commandLine.getOptionProperties(dynamicPropertiesOption.getOpt());
		for (String key : dynamicProperties.stringPropertyNames()) {
			effectiveConfiguration.setString(key, dynamicProperties.getProperty(key));
		}

		final StringBuilder entryPointClassArgs = new StringBuilder();
		if (commandLine.hasOption(jobClassOption.getOpt())) {
			entryPointClassArgs.append(" --")
				.append(jobClassOption.getLongOpt())
				.append(" ")
				.append(commandLine.getOptionValue(jobClassOption.getOpt()));
		}

		if (commandLine.hasOption(jobIdOption.getOpt())) {
			entryPointClassArgs.append(" --")
				.append(jobIdOption.getLongOpt())
				.append(" ")
				.append(commandLine.getOptionValue(jobIdOption.getOpt()));
		}
		if (!entryPointClassArgs.toString().isEmpty()) {
			effectiveConfiguration.setString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS_ARGS,
				entryPointClassArgs.toString());
		}

		return effectiveConfiguration;
	}

	private int run(String[] args) throws CliArgsException, FlinkException {
		final CommandLine cmd = parseCommandLineOptions(args, true);

		if (cmd.hasOption(HELP_OPTION.getOpt())) {
			printUsage();
			return 0;
		}

		final Configuration configuration = applyCommandLineOptionsToConfiguration(cmd);
		final ClusterClientFactory<String> kubernetesClusterClientFactory =
			clusterClientServiceLoader.getClusterClientFactory(configuration);

		final ClusterDescriptor<String> kubernetesClusterDescriptor =
			kubernetesClusterClientFactory.createClusterDescriptor(configuration);

		try {
			final ClusterClient<String> clusterClient;
			String clusterId = kubernetesClusterClientFactory.getClusterId(configuration);
			final boolean detached = cmd.hasOption(DETACHED_OPTION.getOpt());
			final FlinkKubeClient kubeClient = KubeClientFactory.fromConfiguration(configuration);

			// Retrieve or create a session cluster.
			if (clusterId != null && kubeClient.getInternalService(clusterId) != null) {
				clusterClient = kubernetesClusterDescriptor.retrieve(clusterId).getClusterClient();
			} else {
				clusterClient = kubernetesClusterDescriptor
						.deploySessionCluster(
								kubernetesClusterClientFactory.getClusterSpecification(configuration))
						.getClusterClient();
				clusterId = clusterClient.getClusterId();
			}

			try {
				if (!detached) {
					Tuple2<Boolean, Boolean> continueRepl = new Tuple2<>(true, false);
					try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
						while (continueRepl.f0) {
							continueRepl = repStep(in);
						}
					} catch (Exception e) {
						LOG.warn("Exception while running the interactive command line interface.", e);
					}
					if (continueRepl.f1) {
						kubernetesClusterDescriptor.killCluster(clusterId);
					}
				}
				clusterClient.close();
				kubeClient.close();
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

	/**
	 * Check whether need to continue or kill the cluster.
	 * @param in input buffer reader
	 * @return f0, whether need to continue read from input. f1, whether need to kill the cluster.
	 */
	private Tuple2<Boolean, Boolean> repStep(BufferedReader in) throws IOException, InterruptedException {
		final long startTime = System.currentTimeMillis();
		while ((System.currentTimeMillis() - startTime) < CLIENT_POLLING_INTERVAL_MS
			&& (!in.ready())) {
			Thread.sleep(200L);
		}
		//------------- handle interactive command by user. ----------------------

		if (in.ready()) {
			final String command = in.readLine();
			switch (command) {
				case "quit":
					return new Tuple2<>(false, false);
				case "stop":
					return new Tuple2<>(false, true);

				case "help":
					System.err.println(KUBERNETES_CLUSTER_HELP);
					break;
				default:
					System.err.println("Unknown command '" + command + "'. Showing help:");
					System.err.println(KUBERNETES_CLUSTER_HELP);
					break;
			}
		}

		return new Tuple2<>(true, false);
	}

	public static void main(String[] args) {
		final Configuration configuration = GlobalConfiguration.loadConfiguration();

		int retCode;

		try {
			final FlinkKubernetesCustomCli cli = new FlinkKubernetesCustomCli(configuration, "", "");
			retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));
		} catch (CliArgsException e) {
			retCode = handleCliArgsException(e, LOG);
		} catch (Exception e) {
			retCode = handleError(e, LOG);
		}

		System.exit(retCode);
	}
}
