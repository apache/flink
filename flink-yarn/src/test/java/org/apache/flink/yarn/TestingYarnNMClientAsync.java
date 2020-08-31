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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;

/**
 * A Yarn {@link NMClientAsync} implementation for testing.
 */
class TestingYarnNMClientAsync extends NMClientAsyncImpl {

	private final TriConsumer<Container, ContainerLaunchContext, CallbackHandler> startContainerAsyncConsumer;
	private final TriConsumer<ContainerId, NodeId, CallbackHandler> stopContainerAsyncConsumer;

	private TestingYarnNMClientAsync(
		final CallbackHandler callbackHandler,
		TriConsumer<Container, ContainerLaunchContext, CallbackHandler> startContainerAsyncConsumer,
		TriConsumer<ContainerId, NodeId, CallbackHandler> stopContainerAsyncConsumer) {
		super(callbackHandler);
		this.startContainerAsyncConsumer = Preconditions.checkNotNull(startContainerAsyncConsumer);
		this.stopContainerAsyncConsumer = Preconditions.checkNotNull(stopContainerAsyncConsumer);
	}

	@Override
	public void startContainerAsync(Container container, ContainerLaunchContext containerLaunchContext) {
		this.startContainerAsyncConsumer.accept(container, containerLaunchContext, callbackHandler);
	}

	@Override
	public void stopContainerAsync(ContainerId containerId, NodeId nodeId) {
		this.stopContainerAsyncConsumer.accept(containerId, nodeId, callbackHandler);
	}

	static Builder builder() {
		return new Builder();
	}

	// ------------------------------------------------------------------------
	//  Override lifecycle methods to avoid actually starting the service
	// ------------------------------------------------------------------------

	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		// noop
	}

	@Override
	protected void serviceStart() throws Exception {
		// noop
	}

	@Override
	protected void serviceStop() throws Exception {
		// noop
	}

	/**
	 * Builder class for {@link TestingYarnAMRMClientAsync}.
	 */
	public static class Builder {
		private TriConsumer<Container, ContainerLaunchContext, CallbackHandler> startContainerAsyncConsumer = (ignored1, ignored2, ignored3) -> {};
		private TriConsumer<ContainerId, NodeId, CallbackHandler> stopContainerAsyncConsumer = (ignored1, ignored2, ignored3) -> {};

		private Builder() {}

		Builder setStartContainerAsyncConsumer(TriConsumer<Container, ContainerLaunchContext, CallbackHandler> startContainerAsyncConsumer) {
			this.startContainerAsyncConsumer = startContainerAsyncConsumer;
			return this;
		}

		Builder setStopContainerAsyncConsumer(TriConsumer<ContainerId, NodeId, CallbackHandler> stopContainerAsyncConsumer) {
			this.stopContainerAsyncConsumer = stopContainerAsyncConsumer;
			return this;
		}

		public TestingYarnNMClientAsync build(CallbackHandler callbackHandler) {
			return new TestingYarnNMClientAsync(
				callbackHandler,
				startContainerAsyncConsumer,
				stopContainerAsyncConsumer);
		}
	}
}
