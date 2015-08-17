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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.memory.MemoryManager;

/**
 * Encapsulation of TaskManager runtime information, like hostname, configuration, IO and memory
 * managers etc.
 *
 */
public class TaskManagerContext implements java.io.Serializable {

	private static final long serialVersionUID = 5598219619760274072L;
	
	/** host name of the interface that the TaskManager uses to communicate */
	private final String hostname;

	/** configuration that the TaskManager was started with */
	private final Configuration configuration;

	/** IO manager of the task manager */
	private final IOManager ioManager;

	/** Memory manager of the task manager */
	private final MemoryManager memoryManager;

	/** Network environment of the Task Manager */
	private final NetworkEnvironment networkEnvironment;

	/**
	 * Creates a runtime info.
	 * @param hostname The host name of the interface that the TaskManager uses to communicate.
	 * @param configuration The configuration that the TaskManager was started with.
	 * @param ioManager IO manager of the Task manager
	 * @param memoryManager Memory Manager of the Task Manager
	 * @param networkEnvironment Network Environment of the Task Manager
	 */
	public TaskManagerContext(String hostname, Configuration configuration, IOManager ioManager,
								MemoryManager memoryManager, NetworkEnvironment networkEnvironment) {
		this.hostname = hostname;
		this.configuration = configuration;
		this.ioManager = ioManager;
		this.memoryManager = memoryManager;
		this.networkEnvironment = networkEnvironment;
	}

	/**
	 * Gets host name of the interface that the TaskManager uses to communicate.
	 * @return The host name of the interface that the TaskManager uses to communicate.
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * Gets the configuration that the TaskManager was started with.
	 * @return The configuration that the TaskManager was started with.
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Gets the IO manager of the Task Manager
	 *
	 * @return The IO Manager of the task manager
	 */
	public IOManager getIoManager() {
		return ioManager;
	}

	/**
	 * Gets the Memory manager of the Task Manager
	 *
	 * @return The Memory Manager of the task manager
	 */
	public MemoryManager getMemoryManager() {
		return memoryManager;
	}

	/**
	 * Gets the network environment of the Task Manager
	 *
	 * @return The network environment of the task manager
	 */
	public NetworkEnvironment getNetworkEnvironment() {
		return networkEnvironment;
	}
}
