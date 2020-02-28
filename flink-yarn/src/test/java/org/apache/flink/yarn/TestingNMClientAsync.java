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
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.util.function.BiConsumer;

/**
 * Testing implementation of {@link NMClientAsync} for tests.
 */
public class TestingNMClientAsync extends NMClientAsync {
	private BiConsumer<ContainerId, NodeId> getContainerStatusAsyncBiConsumer = (containerId, nodeId) -> {};
	private BiConsumer<Container, ContainerLaunchContext> startContainerAsyncBiConsumer = (container, containerLaunchContext) -> {};
	private BiConsumer<ContainerId, NodeId> stopContainerAsyncBiConsumer = (containerId, nodeId) -> {};

	TestingNMClientAsync(
			CallbackHandler callbackHandler) {
		super(callbackHandler);
	}

	@Override
	public void startContainerAsync(Container container, ContainerLaunchContext containerLaunchContext) {
		startContainerAsyncBiConsumer.accept(container, containerLaunchContext);
	}

	@Override
	public void stopContainerAsync(ContainerId containerId, NodeId nodeId) {
		stopContainerAsyncBiConsumer.accept(containerId, nodeId);
	}

	@Override
	public void getContainerStatusAsync(ContainerId containerId, NodeId nodeId) {
		getContainerStatusAsyncBiConsumer.accept(containerId, nodeId);
	}

	// This method override the NMClientAsync#increaseContainerResourceAsync when using Hadoop 2.8.0+
	public void increaseContainerResourceAsync(Container container) {}

	public void setStartContainerAsyncBiConsumer(BiConsumer<Container, ContainerLaunchContext> startContainerAsyncBiConsumer) {
		this.startContainerAsyncBiConsumer = startContainerAsyncBiConsumer;
	}

	public void setGetContainerStatusAsyncBiConsumer(BiConsumer<ContainerId, NodeId> getContainerStatusAsyncBiConsumer) {
		this.getContainerStatusAsyncBiConsumer = getContainerStatusAsyncBiConsumer;
	}

	public void setStopContainerAsyncBiConsumer(BiConsumer<ContainerId, NodeId> stopContainerAsyncBiConsumer) {
		this.stopContainerAsyncBiConsumer = stopContainerAsyncBiConsumer;
	}
}
