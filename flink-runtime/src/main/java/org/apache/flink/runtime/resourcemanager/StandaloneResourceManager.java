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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

/**
 * A standalone implementation of the resource manager. Used when the system is started in
 * standalone mode (via scripts), rather than via a resource framework like YARN or Mesos.
 */
public class StandaloneResourceManager extends ResourceManager<ResourceManagerGateway, TaskExecutorRegistration> {

	public StandaloneResourceManager(RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		SlotManager slotManager) {
		super(rpcService, highAvailabilityServices, slotManager);
	}

	@Override
	protected void initialize() throws Exception {
		// nothing to initialize
	}

	@Override
	protected void fatalError(final String message, final Throwable error) {
		log.error("FATAL ERROR IN RESOURCE MANAGER: " + message, error);
		// kill this process
		System.exit(EXIT_CODE_FATAL_ERROR);
	}

	@Override
	protected TaskExecutorRegistration workerStarted(ResourceID resourceID, TaskExecutorGateway taskExecutorGateway) {
		InstanceID instanceID = new InstanceID();
		TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(taskExecutorGateway, instanceID);
		return taskExecutorRegistration;
	}

	@Override
	protected void shutDownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {

	}
}
