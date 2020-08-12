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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.mesos.Utils;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.mesos.util.MesosResourceAllocation;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.util.Preconditions;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;

import scala.Option;

import static org.apache.flink.mesos.Utils.rangeValues;
import static org.apache.flink.mesos.Utils.variable;
import static org.apache.flink.mesos.configuration.MesosOptions.PORT_ASSIGNMENTS;

/**
 * Implements the launch of a Mesos worker.
 *
 * <p>Translates the abstract {@link ContainerSpecification} into a concrete
 * Mesos-specific {@link Protos.TaskInfo}.
 */
public class LaunchableMesosWorker implements LaunchableTask {

	protected static final Logger LOG = LoggerFactory.getLogger(LaunchableMesosWorker.class);

	/**
	 * The set of configuration keys to be dynamically configured with a port allocated from Mesos.
	 */
	static final Set<String> TM_PORT_KEYS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
		"taskmanager.rpc.port",
		"taskmanager.data.port")));

	private final MesosArtifactResolver resolver;
	private final ContainerSpecification containerSpec;
	private final MesosTaskManagerParameters params;
	private final Protos.TaskID taskID;
	private final Request taskRequest;
	private final MesosConfiguration mesosConfiguration;

	/**
	 * Construct a launchable Mesos worker.
	 * @param resolver The resolver for retrieving artifacts (e.g. jars, configuration)
	 * @param params the TM parameters such as memory, cpu to acquire.
	 * @param containerSpec an abstract container specification for launch time.
	 * @param taskID the taskID for this worker.
	 */
	LaunchableMesosWorker(
			MesosArtifactResolver resolver,
			MesosTaskManagerParameters params,
			ContainerSpecification containerSpec,
			Protos.TaskID taskID,
			MesosConfiguration mesosConfiguration) {
		this.resolver = Preconditions.checkNotNull(resolver);
		this.containerSpec = Preconditions.checkNotNull(containerSpec);
		this.params = Preconditions.checkNotNull(params);
		this.taskID = Preconditions.checkNotNull(taskID);
		this.mesosConfiguration = Preconditions.checkNotNull(mesosConfiguration);

		this.taskRequest = new Request();
	}

	public Protos.TaskID taskID() {
		return taskID;
	}

	@Override
	public TaskRequest taskRequest() {
		return taskRequest;
	}

	class Request implements TaskRequest {
		private final AtomicReference<TaskRequest.AssignedResources> assignedResources = new AtomicReference<>();

		@Override
		public String getId() {
			return taskID.getValue();
		}

		@Override
		public String taskGroupName() {
			return "";
		}

		@Override
		public double getCPUs() {
			return params.cpus();
		}

		double getGPUs() {
			return params.gpus();
		}

		@Override
		public double getMemory() {
			return params.containeredParameters().getTaskExecutorProcessSpec().getTotalProcessMemorySize().getMebiBytes();
		}

		@Override
		public double getNetworkMbps() {
			return params.network();
		}

		@Override
		public double getDisk() {
			return params.disk();
		}

		@Override
		public int getPorts() {
			return extractPortKeys(containerSpec.getFlinkConfiguration()).size();
		}

		@Override
		public Map<String, Double> getScalarRequests() {
			return Collections.singletonMap("gpus", (double) params.gpus());
		}

		@Override
		public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
			return Collections.emptyMap();
		}

		@Override
		public List<? extends ConstraintEvaluator> getHardConstraints() {
			return params.constraints();
		}

		@Override
		public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
			return null;
		}

		@Override
		public void setAssignedResources(AssignedResources assignedResources) {
			this.assignedResources.set(assignedResources);
		}

		@Override
		public AssignedResources getAssignedResources() {
			return assignedResources.get();
		}

		@Override
		public String toString() {
			return "Request{" +
				"cpus=" + getCPUs() +
				", memory=" + getMemory() +
				", gpus=" + getGPUs() +
				", disk=" + getDisk() +
				", network=" + getNetworkMbps() +
				"}";
		}
	}

	/**
	 * Construct the TaskInfo needed to launch the worker.
	 * @param slaveId the assigned slave.
	 * @param allocation the resource allocation (available resources).
	 * @return a fully-baked TaskInfo.
	 */
	@Override
	public Protos.TaskInfo launch(Protos.SlaveID slaveId, MesosResourceAllocation allocation) {

		ContaineredTaskManagerParameters tmParams = params.containeredParameters();

		final Configuration dynamicProperties = new Configuration();

		// incorporate the dynamic properties set by the template
		dynamicProperties.addAll(containerSpec.getFlinkConfiguration());

		// build a TaskInfo with assigned resources, environment variables, etc
		final Protos.TaskInfo.Builder taskInfo = Protos.TaskInfo.newBuilder()
			.setSlaveId(slaveId)
			.setTaskId(taskID)
			.setName(taskID.getValue());

		// take needed resources from the overall allocation, under the assumption of adequate resources
		Set<String> roles = mesosConfiguration.roles();
		taskInfo.addAllResources(allocation.takeScalar("cpus", taskRequest.getCPUs(), roles));
		taskInfo.addAllResources(allocation.takeScalar("gpus", taskRequest.getGPUs(), roles));
		taskInfo.addAllResources(allocation.takeScalar("mem", taskRequest.getMemory(), roles));

		if (taskRequest.getDisk() > 0.0) {
			taskInfo.addAllResources(allocation.takeScalar("disk", taskRequest.getDisk(), roles));
		}

		if (taskRequest.getNetworkMbps() > 0.0) {
			taskInfo.addAllResources(allocation.takeScalar("network", taskRequest.getNetworkMbps(), roles));
		}

		final Protos.CommandInfo.Builder cmd = taskInfo.getCommandBuilder();
		final Protos.Environment.Builder env = cmd.getEnvironmentBuilder();
		final StringBuilder jvmArgs = new StringBuilder();

		//configure task manager hostname property if hostname override property is supplied
		Option<String> taskManagerHostnameOption = params.getTaskManagerHostname();

		if (taskManagerHostnameOption.isDefined()) {
			// replace the TASK_ID pattern by the actual task id value of the Mesos task
			final String taskManagerHostname = MesosTaskManagerParameters.TASK_ID_PATTERN
				.matcher(taskManagerHostnameOption.get())
				.replaceAll(Matcher.quoteReplacement(taskID.getValue()));

			dynamicProperties.setString(TaskManagerOptions.HOST, taskManagerHostname);
		}

		// take needed ports for the TM
		Set<String> tmPortKeys = extractPortKeys(containerSpec.getFlinkConfiguration());
		List<Protos.Resource> portResources = allocation.takeRanges("ports", tmPortKeys.size(), roles);
		taskInfo.addAllResources(portResources);
		Iterator<String> portsToAssign = tmPortKeys.iterator();
		rangeValues(portResources).forEach(port -> dynamicProperties.setLong(portsToAssign.next(), port));
		if (portsToAssign.hasNext()) {
			throw new IllegalArgumentException("insufficient # of ports assigned");
		}

		// ship additional files
		for (ContainerSpecification.Artifact artifact : containerSpec.getArtifacts()) {
			cmd.addUris(Utils.uri(resolver, artifact));
		}

		// add user-specified URIs
		for (String uri : params.uris()) {
			cmd.addUris(CommandInfo.URI.newBuilder().setValue(uri));
		}

		// propagate environment variables
		for (Map.Entry<String, String> entry : params.containeredParameters().taskManagerEnv().entrySet()) {
			env.addVariables(variable(entry.getKey(), entry.getValue()));
		}
		for (Map.Entry<String, String> entry : containerSpec.getEnvironmentVariables().entrySet()) {
			env.addVariables(variable(entry.getKey(), entry.getValue()));
		}

		// set the ResourceID of TM to the Mesos task
		dynamicProperties.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, taskInfo.getTaskId().getValue());

		// finalize the memory parameters
		jvmArgs.append(" ").append(ProcessMemoryUtils.generateJvmParametersStr(tmParams.getTaskExecutorProcessSpec()));

		// pass dynamic system properties
		jvmArgs.append(' ').append(
			ContainerSpecification.formatSystemProperties(containerSpec.getSystemProperties()));

		// finalize JVM args
		env.addVariables(variable(MesosConfigKeys.ENV_JVM_ARGS, jvmArgs.toString()));

		// populate TASK_NAME and FRAMEWORK_NAME environment variables to the TM container
		env.addVariables(variable(MesosConfigKeys.ENV_TASK_NAME, taskInfo.getTaskId().getValue()));
		env.addVariables(variable(MesosConfigKeys.ENV_FRAMEWORK_NAME, mesosConfiguration.frameworkInfo().getName()));

		// build the launch command w/ dynamic application properties
		StringBuilder launchCommand = new StringBuilder();
		if (params.bootstrapCommand().isDefined()) {
			launchCommand.append(params.bootstrapCommand().get()).append(" && ");
		}
		launchCommand
			.append(params.command())
			.append(" ")
			.append(ContainerSpecification.formatSystemProperties(dynamicProperties))
			.append(" ")
			.append(TaskExecutorProcessUtils.generateDynamicConfigsStr(tmParams.getTaskExecutorProcessSpec()));
		cmd.setValue(launchCommand.toString());

		// build the container info
		Protos.ContainerInfo.Builder containerInfo = Protos.ContainerInfo.newBuilder();
		// in event that no docker image or mesos image name is specified, we must still
		// set type to MESOS
		containerInfo.setType(Protos.ContainerInfo.Type.MESOS);
		switch (params.containerType()) {
			case MESOS:
				if (params.containerImageName().isDefined()) {
					containerInfo
						.setMesos(Protos.ContainerInfo.MesosInfo.newBuilder()
							.setImage(Protos.Image.newBuilder()
								.setType(Protos.Image.Type.DOCKER)
								.setDocker(Protos.Image.Docker.newBuilder()
									.setName(params.containerImageName().get()))));
				}
				break;

			case DOCKER:
				assert(params.containerImageName().isDefined());
				containerInfo
					.setType(Protos.ContainerInfo.Type.DOCKER)
					.setDocker(Protos.ContainerInfo.DockerInfo.newBuilder()
						.addAllParameters(params.dockerParameters())
						.setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST)
						.setImage(params.containerImageName().get())
						.setForcePullImage(params.dockerForcePullImage()));
				break;

			default:
				throw new IllegalStateException("unsupported container type");
		}

		// add any volumes to the containerInfo
		containerInfo.addAllVolumes(params.containerVolumes());
		taskInfo.setContainer(containerInfo);

		LOG.info("Starting TaskExecutor {} with command: {}", slaveId, taskInfo.getCommand().getValue());

		return taskInfo.build();
	}

	/**
	 * Get the port keys representing the TM's configured endpoints. This includes mandatory TM endpoints such as
	 * data and rpc as well as optionally configured endpoints for services such as prometheus reporter
	 *
	 * @param config to extract the port keys from
	 * @return A deterministically ordered Set of port keys to expose from the TM container
	 */
	static Set<String> extractPortKeys(Configuration config) {
		final LinkedHashSet<String> tmPortKeys = new LinkedHashSet<>(TM_PORT_KEYS);

		final String portKeys = config.getString(PORT_ASSIGNMENTS);

		if (portKeys != null) {
			Arrays.stream(portKeys.split(","))
				.map(String::trim)
				.peek(key -> LOG.debug("Adding port key {} to mesos request", key))
				.forEach(tmPortKeys::add);
		}

		return Collections.unmodifiableSet(tmPortKeys);
	}

	@Override
	public String toString() {
		return "LaunchableMesosWorker{" +
			"taskID=" + taskID +
			"taskRequest=" + taskRequest +
			'}';
	}

	/**
	 * Configures an artifact server to serve the artifacts associated with a container specification.
	 * @param server the server to configure.
	 * @param container the container with artifacts to serve.
	 * @throws IOException if the artifacts cannot be accessed.
	 */
	static void configureArtifactServer(MesosArtifactServer server, ContainerSpecification container) throws IOException {
		// serve the artifacts associated with the container environment
		for (ContainerSpecification.Artifact artifact : container.getArtifacts()) {
			server.addPath(artifact.source, artifact.dest);
		}
	}
}
