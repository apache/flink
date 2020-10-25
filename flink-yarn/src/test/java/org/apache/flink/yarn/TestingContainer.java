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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;

/**
 * A {@link Container} implementation for testing.
 */
class TestingContainer extends ContainerPBImpl {
	private final ContainerId containerId;
	private final NodeId nodeId;
	private final Resource resource;
	private final Priority priority;

	TestingContainer(
		final ContainerId containerId,
		final NodeId nodeId,
		final Resource resource,
		final Priority priority) {

		this.containerId = containerId;
		this.nodeId = nodeId;
		this.resource = resource;
		this.priority = priority;
	}

	@Override
	public ContainerId getId() {
		return containerId;
	}

	@Override
	public NodeId getNodeId() {
		return nodeId;
	}

	@Override
	public Resource getResource() {
		return resource;
	}

	@Override
	public Priority getPriority() {
		return priority;
	}
}
