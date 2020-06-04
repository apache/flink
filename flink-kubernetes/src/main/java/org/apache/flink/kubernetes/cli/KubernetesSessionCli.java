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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.ExecutorCLI;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Kubernetes customized commandline.
 */
@Internal
public class KubernetesSessionCli {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesSessionCli.class);

	private static final long CLIENT_POLLING_INTERVAL_MS = 3000L;

	private static final String KUBERNETES_CLUSTER_HELP = "Available commands:\n" +
		"help - show these commands\n" +
		"stop - stop the kubernetes cluster\n" +
		"quit - quit attach mode";

	private final Configuration baseConfiguration;

	private final ExecutorCLI cli;
	private final ClusterClientServiceLoader clusterClientServiceLoader;

	public KubernetesSessionCli(Configuration configuration) {
		this(configuration, new DefaultClusterClientServiceLoader());
	}

	public KubernetesSessionCli(Configuration configuration, ClusterClientServiceLoader clusterClientServiceLoader) {
		this.baseConfiguration = new UnmodifiableConfiguration(checkNotNull(configuration));
		this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);
		this.cli = new ExecutorCLI(baseConfiguration);
	}

	public Configuration getEffectiveConfiguration(String[] args) throws CliArgsException {
		final CommandLine commandLine = cli.parseCommandLineOptions(args, true);
		final Configuration effectiveConfiguration = cli.applyCommandLineOptionsToConfiguration(commandLine);
		effectiveConfiguration.set(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);
		return effectiveConfiguration;
	}

	private int run(String[] args) throws FlinkException, CliArgsException {
		final Configuration configuration = getEffectiveConfiguration(args);

		final ClusterClientFactory<String> kubernetesClusterClientFactory =
			clusterClientServiceLoader.getClusterClientFactory(configuration);

		final ClusterDescriptor<String> kubernetesClusterDescriptor =
			kubernetesClusterClientFactory.createClusterDescriptor(configuration);

		try {
			final ClusterClient<String> clusterClient;
			String clusterId = kubernetesClusterClientFactory.getClusterId(configuration);
			final boolean detached = !configuration.get(DeploymentOptions.ATTACHED);
			final FlinkKubeClient kubeClient = KubeClientFactory.fromConfiguration(configuration);

			// Retrieve or create a session cluster.
			if (clusterId != null && kubeClient.getRestService(clusterId).isPresent()) {
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
			final KubernetesSessionCli cli = new KubernetesSessionCli(configuration);
			retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));
		} catch (CliArgsException e) {
			retCode = AbstractCustomCommandLine.handleCliArgsException(e, LOG);
		} catch (Exception e) {
			retCode = AbstractCustomCommandLine.handleError(e, LOG);
		}

		System.exit(retCode);
	}
}
