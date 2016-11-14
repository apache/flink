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

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.cli.FlinkMesosSessionCli;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.mesos.Protos;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.mesos.Utils.variable;
import static org.apache.flink.mesos.Utils.range;
import static org.apache.flink.mesos.Utils.ranges;
import static org.apache.flink.mesos.Utils.scalar;

/**
 * Specifies how to launch a Mesos worker.
 */
public class LaunchableMesosWorker implements LaunchableTask {

	/**
	 * The set of configuration keys to be dynamically configured with a port allocated from Mesos.
	 */
	private static String[] TM_PORT_KEYS = {
		"taskmanager.rpc.port",
		"taskmanager.data.port" };

	private final MesosTaskManagerParameters params;
	private final Protos.TaskInfo.Builder template;
	private final Protos.TaskID taskID;
	private final Request taskRequest;

	/**
	 * Construct a launchable Mesos worker.
	 * @param params the TM parameters such as memory, cpu to acquire.
	 * @param template a template for the TaskInfo to be constructed at launch time.
	 * @param taskID the taskID for this worker.
	 */
	public LaunchableMesosWorker(MesosTaskManagerParameters params, Protos.TaskInfo.Builder template, Protos.TaskID taskID) {
		this.params = params;
		this.template = template;
		this.taskID = taskID;
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

		@Override
		public double getMemory() {
			return params.containeredParameters().taskManagerTotalMemoryMB();
		}

		@Override
		public double getNetworkMbps() {
			return 0.0;
		}

		@Override
		public double getDisk() {
			return 0.0;
		}

		@Override
		public int getPorts() {
			return TM_PORT_KEYS.length;
		}

		@Override
		public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
			return Collections.emptyMap();
		}

		@Override
		public List<? extends ConstraintEvaluator> getHardConstraints() {
			return null;
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
				"memory=" + getMemory() +
				'}';
		}
	}

	/**
	 * Construct the TaskInfo needed to launch the worker.
	 * @param slaveId the assigned slave.
	 * @param assignment the assignment details.
	 * @return a fully-baked TaskInfo.
	 */
	@Override
	public Protos.TaskInfo launch(Protos.SlaveID slaveId, TaskAssignmentResult assignment) {

		final Configuration dynamicProperties = new Configuration();

		// specialize the TaskInfo template with assigned resources, environment variables, etc
		final Protos.TaskInfo.Builder taskInfo = template
			.clone()
			.setSlaveId(slaveId)
			.setTaskId(taskID)
			.setName(taskID.getValue())
			.addResources(scalar("cpus", assignment.getRequest().getCPUs()))
			.addResources(scalar("mem", assignment.getRequest().getMemory()));

		// use the assigned ports for the TM
		if (assignment.getAssignedPorts().size() < TM_PORT_KEYS.length) {
			throw new IllegalArgumentException("unsufficient # of ports assigned");
		}
		for (int i = 0; i < TM_PORT_KEYS.length; i++) {
			int port = assignment.getAssignedPorts().get(i);
			String key = TM_PORT_KEYS[i];
			taskInfo.addResources(ranges("ports", range(port, port)));
			dynamicProperties.setInteger(key, port);
		}

		// finalize environment variables
		final Protos.Environment.Builder environmentBuilder = taskInfo.getCommandBuilder().getEnvironmentBuilder();

		// propagate the Mesos task ID to the TM
		environmentBuilder
			.addVariables(variable(MesosConfigKeys.ENV_FLINK_CONTAINER_ID, taskInfo.getTaskId().getValue()));

		// propagate the dynamic configuration properties to the TM
		String dynamicPropertiesEncoded = FlinkMesosSessionCli.encodeDynamicProperties(dynamicProperties);
		environmentBuilder
			.addVariables(variable(MesosConfigKeys.ENV_DYNAMIC_PROPERTIES, dynamicPropertiesEncoded));

		return taskInfo.build();
	}

	@Override
	public String toString() {
		return "LaunchableMesosWorker{" +
			"taskID=" + taskID +
			"taskRequest=" + taskRequest +
			'}';
	}
}
