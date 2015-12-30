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

/**
 * Encapsulation of TaskManager runtime information, like hostname and configuration.
 */
public class TaskManagerRuntimeInfo implements java.io.Serializable {

	private static final long serialVersionUID = 5598219619760274072L;
	
	/** host name of the interface that the TaskManager uses to communicate */
	private final String hostname;

	/** configuration that the TaskManager was started with */
	private final Configuration configuration;

	/**
	 * Creates a runtime info.
	 * @param hostname The host name of the interface that the TaskManager uses to communicate.
	 * @param configuration The configuration that the TaskManager was started with.
	 */
	public TaskManagerRuntimeInfo(String hostname, Configuration configuration) {
		this.hostname = hostname;
		this.configuration = configuration;
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
}
