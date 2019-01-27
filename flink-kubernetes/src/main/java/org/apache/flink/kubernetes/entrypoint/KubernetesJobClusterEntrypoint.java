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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.runtime.clusterframework.KubernetesResourceManager;
import org.apache.flink.kubernetes.utils.KubernetesClientFactory;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.StrictlyMatchingSlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.File;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.kubernetes.configuration.Constants.ENV_FLINK_CLASSPATH;
import static org.apache.flink.kubernetes.configuration.Constants.USER_JAR_NAME_IN_IMAGE;

/**
 * Entry point for Kubernetes job clusters.
 */
public class KubernetesJobClusterEntrypoint extends JobClusterEntrypoint {

	public KubernetesJobClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected SecurityContext installSecurityContext(Configuration configuration) throws Exception {
		return super.installSecurityContext(configuration);
	}

	@Override
	protected String getRPCPortRange(Configuration configuration) {
		return String.valueOf(configuration.getInteger(JobManagerOptions.PORT));
	}

	@Override
	protected ResourceManager<?> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl) throws Exception {
		final ResourceManagerConfiguration resourceManagerConfiguration = ResourceManagerConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServicesConfiguration resourceManagerRuntimeServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServices resourceManagerRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			resourceManagerRuntimeServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());
		SlotManager slotManager = new StrictlyMatchingSlotManager(
			rpcService.getScheduledExecutor(),
			resourceManagerRuntimeServicesConfiguration.getSlotManagerConfiguration().getTaskManagerRequestTimeout(),
			resourceManagerRuntimeServicesConfiguration.getSlotManagerConfiguration().getSlotRequestTimeout(),
			configuration.contains(ResourceManagerOptions.TASK_MANAGER_TIMEOUT) ?
				resourceManagerRuntimeServicesConfiguration.getSlotManagerConfiguration().getTaskManagerTimeout() :
				Time.seconds(AkkaUtils.INF_TIMEOUT().toSeconds()),
			resourceManagerRuntimeServicesConfiguration.getSlotManagerConfiguration().getTaskManagerCheckerInitialDelay());
		return new KubernetesResourceManager(
			rpcService,
			FlinkResourceManager.RESOURCE_MANAGER_NAME,
			resourceId,
			configuration,
			resourceManagerConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			resourceManagerRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler);
	}

	@Override
	protected JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException {
		// build job graph
		int parallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
		String flinkClassPath = System.getenv().get(ENV_FLINK_CLASSPATH);
		Preconditions.checkNotNull(flinkClassPath);
		File jobJarFile = null;
		for (String jar : flinkClassPath.split(File.pathSeparator)) {
			if (jar.split(File.separator)[jar.split(File.separator).length - 1].equals(USER_JAR_NAME_IN_IMAGE)) {
				jobJarFile = new File(jar);
			}
		}
		Preconditions.checkNotNull(jobJarFile);
		String programEntryPointClass = configuration.getString(KubernetesConfigOptions.USER_PROGRAM_ENTRYPOINT_CLASS);
		String programArgs = configuration.getString(KubernetesConfigOptions.USER_PROGRAM_ARGS);
		String[] programArgsArray = null;
		if (programArgs != null && !programArgs.isEmpty()) {
			programArgsArray = programArgs.split(" ");
		}

		final JobGraph jobGraph;
		try {
			final PackagedProgram packagedProgram = programEntryPointClass == null ?
				new PackagedProgram(jobJarFile, programArgsArray) :
				new PackagedProgram(jobJarFile, programEntryPointClass, programArgsArray);
			jobGraph = PackagedProgramUtils.createJobGraph(packagedProgram, configuration, parallelism);
			jobGraph.setAllowQueuedScheduling(true);
		} catch (final ProgramInvocationException e) {
			throw new FlinkException("Build job graph failed.", e);
		}
		return jobGraph;
	}

	@Override
	protected void registerShutdownActions(CompletableFuture<ApplicationStatus> terminationFuture) {
		Configuration configuration = GlobalConfiguration.loadConfiguration();
		if (configuration.getBoolean(KubernetesConfigOptions.DESTROY_PERJOB_CLUSTER_AFTER_JOB_FINISHED)) {
			destroyClusterOnKubernetes();
		}
	}

	private void destroyClusterOnKubernetes() {
		KubernetesClientFactory.destroyCluster(GlobalConfiguration.loadConfiguration());
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, KubernetesJobClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		Configuration configuration = GlobalConfiguration.loadConfiguration();

		KubernetesJobClusterEntrypoint kubernetesJobClusterEntrypoint =
			new KubernetesJobClusterEntrypoint(configuration);

		kubernetesJobClusterEntrypoint.startCluster();
	}
}
