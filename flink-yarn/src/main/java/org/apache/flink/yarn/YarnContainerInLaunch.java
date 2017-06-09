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

package org.apache.flink.yarn;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;

import org.apache.hadoop.yarn.api.records.Container;

import static java.util.Objects.requireNonNull;

/**
 * This class describes a container in which a TaskManager is being launched (or
 * has been launched) but where the TaskManager has not properly registered, yet.
 */
public class YarnContainerInLaunch implements ResourceIDRetrievable {

	private final Container container;

	private final long timestamp;

	/** The resource id associated with this worker type. */
	private final ResourceID resourceID;

	public YarnContainerInLaunch(Container container) {
		this(container, System.currentTimeMillis());
	}

	public YarnContainerInLaunch(Container container, long timestamp) {
		this.container = requireNonNull(container);
		this.timestamp = timestamp;
		this.resourceID = YarnFlinkResourceManager.extractResourceID(container);
	}

	// ------------------------------------------------------------------------

	public Container container() {
		return container;
	}

	public long timestamp() {
		return timestamp;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "ContainerInLaunch @ " + timestamp + ": " + container;
	}

	@Override
	public ResourceID getResourceID() {
		return resourceID;
	}
}
