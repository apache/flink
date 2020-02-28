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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.mesos.configuration.MesosOptions;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.flink.mesos.util.MesosResourceAllocation;
import org.apache.flink.mesos.util.MesosUtils;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.util.TestLogger;

import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import scala.Option;

import static org.apache.flink.mesos.Utils.ports;
import static org.apache.flink.mesos.Utils.range;
import static org.apache.flink.mesos.configuration.MesosOptions.PORT_ASSIGNMENTS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test that mesos config are extracted correctly from the configuration.
 */
public class LaunchableMesosWorkerTest extends TestLogger {

	@Test
	public void canGetPortKeys() {
		// Setup
		Set<String> additionalPorts = new HashSet<>(Arrays.asList("someport.here", "anotherport"));

		Configuration config = new Configuration();
		config.setString(PORT_ASSIGNMENTS, String.join(",", additionalPorts));

		// Act
		Set<String> portKeys = LaunchableMesosWorker.extractPortKeys(config);

		// Assert
		Set<String> expectedPorts = new HashSet<>(LaunchableMesosWorker.TM_PORT_KEYS);
		expectedPorts.addAll(additionalPorts);
		assertThat(portKeys, is(equalTo(expectedPorts)));
	}

	@Test
	public void canGetNoPortKeys() {
		// Setup
		Configuration config = new Configuration();

		// Act
		Set<String> portKeys = LaunchableMesosWorker.extractPortKeys(config);

		// Assert
		assertThat(portKeys, is(equalTo(LaunchableMesosWorker.TM_PORT_KEYS)));
	}

	@Test
	public void launch_withNonDefaultConfiguration_forwardsConfigurationValues() {
		final Configuration configuration = new Configuration();
		configuration.setString(MesosOptions.MASTER_URL, "foobar");
		final MemorySize memorySize = new MemorySize(1337L);
		configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, memorySize);
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));

		final LaunchableTask launchableTask = new LaunchableMesosWorker(
			ignored -> Option.empty(),
			MesosTaskManagerParameters.create(configuration),
			ContainerSpecification.from(configuration),
			Protos.TaskID.newBuilder().setValue("test-task-id").build(),
			MesosUtils.createMesosSchedulerConfiguration(configuration, "localhost"));

		final Protos.TaskInfo taskInfo = launchableTask.launch(
			Protos.SlaveID.newBuilder().setValue("test-slave-id").build(),
			new MesosResourceAllocation(Collections.singleton(ports(range(1000, 2000)))));

		assertThat(
			taskInfo.getCommand().getValue(),
			containsString(ContainerSpecification.createDynamicProperty(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), memorySize.toString())));
	}

}
