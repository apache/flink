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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.util.function.TriFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A Yarn {@link AMRMClientAsync} implementation for testing.
 */
public class TestingYarnAMRMClientAsync extends AMRMClientAsyncImpl<AMRMClient.ContainerRequest> {

	private final Function<Tuple4<Priority, String, Resource, CallbackHandler>, List<? extends Collection<AMRMClient.ContainerRequest>>> getMatchingRequestsFunction;
	private final BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> addContainerRequestConsumer;
	private final BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> removeContainerRequestConsumer;
	private final BiConsumer<ContainerId, CallbackHandler> releaseAssignedContainerConsumer;
	private final Consumer<Integer> setHeartbeatIntervalConsumer;
	private final TriFunction<String, Integer, String, RegisterApplicationMasterResponse> registerApplicationMasterFunction;
	private final TriConsumer<FinalApplicationStatus, String, String> unregisterApplicationMasterConsumer;
	private final Runnable clientInitRunnable;
	private final Runnable clientStartRunnable;
	private final Runnable clientStopRunnable;

	private TestingYarnAMRMClientAsync(
		CallbackHandler callbackHandler,
		Function<Tuple4<Priority, String, Resource, CallbackHandler>, List<? extends Collection<AMRMClient.ContainerRequest>>> getMatchingRequestsFunction,
		BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> addContainerRequestConsumer,
		BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> removeContainerRequestConsumer,
		BiConsumer<ContainerId, CallbackHandler> releaseAssignedContainerConsumer,
		Consumer<Integer> setHeartbeatIntervalConsumer,
		TriFunction<String, Integer, String, RegisterApplicationMasterResponse> registerApplicationMasterFunction,
		TriConsumer<FinalApplicationStatus, String, String> unregisterApplicationMasterConsumer,
		Runnable clientInitRunnable,
		Runnable clientStartRunnable,
		Runnable clientStopRunnable) {
		super(0, callbackHandler);
		this.setHeartbeatIntervalConsumer = Preconditions.checkNotNull(setHeartbeatIntervalConsumer);
		this.addContainerRequestConsumer = Preconditions.checkNotNull(addContainerRequestConsumer);
		this.releaseAssignedContainerConsumer = Preconditions.checkNotNull(releaseAssignedContainerConsumer);
		this.removeContainerRequestConsumer = Preconditions.checkNotNull(removeContainerRequestConsumer);
		this.registerApplicationMasterFunction = Preconditions.checkNotNull(registerApplicationMasterFunction);
		this.getMatchingRequestsFunction = Preconditions.checkNotNull(getMatchingRequestsFunction);
		this.unregisterApplicationMasterConsumer = Preconditions.checkNotNull(unregisterApplicationMasterConsumer);
		this.clientInitRunnable = Preconditions.checkNotNull(clientInitRunnable);
		this.clientStartRunnable = Preconditions.checkNotNull(clientStartRunnable);
		this.clientStopRunnable = Preconditions.checkNotNull(clientStopRunnable);
	}

	@Override
	public List<? extends Collection<AMRMClient.ContainerRequest>> getMatchingRequests(Priority priority, String resourceName, Resource capability) {
		return getMatchingRequestsFunction.apply(Tuple4.of(priority, resourceName, capability, handler));
	}

	@Override
	public void addContainerRequest(AMRMClient.ContainerRequest req) {
		addContainerRequestConsumer.accept(req, handler);
	}

	@Override
	public void removeContainerRequest(AMRMClient.ContainerRequest req) {
		removeContainerRequestConsumer.accept(req, handler);
	}

	@Override
	public void releaseAssignedContainer(ContainerId containerId) {
		releaseAssignedContainerConsumer.accept(containerId, handler);
	}

	@Override
	public void setHeartbeatInterval(int interval) {
		setHeartbeatIntervalConsumer.accept(interval);
	}

	@Override
	public RegisterApplicationMasterResponse registerApplicationMaster(String appHostName, int appHostPort, String appTrackingUrl) {
		return registerApplicationMasterFunction.apply(appHostName, appHostPort, appTrackingUrl);
	}

	@Override
	public void unregisterApplicationMaster(FinalApplicationStatus appStatus, String appMessage, String appTrackingUrl) {
		unregisterApplicationMasterConsumer.accept(appStatus, appMessage, appTrackingUrl);
	}

	static Builder builder() {
		return new Builder();
	}

	// ------------------------------------------------------------------------
	//  Override lifecycle methods to avoid actually starting the service
	// ------------------------------------------------------------------------

	@Override
	public void init(Configuration conf) {
		clientInitRunnable.run();
	}

	@Override
	public void start() {
		clientStartRunnable.run();
	}

	@Override
	public void stop() {
		clientStopRunnable.run();
	}

	/**
	 * Builder class for {@link TestingYarnAMRMClientAsync}.
	 */
	public static class Builder {
		private Function<Tuple4<Priority, String, Resource, CallbackHandler>, List<? extends Collection<AMRMClient.ContainerRequest>>>
			getMatchingRequestsFunction = ignored -> Collections.emptyList();
		private BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> addContainerRequestConsumer = (ignored1, ignored2) -> {};
		private BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> removeContainerRequestConsumer = (ignored1, ignored2) -> {};
		private BiConsumer<ContainerId, CallbackHandler> releaseAssignedContainerConsumer = (ignored1, ignored2) -> {};
		private Consumer<Integer> setHeartbeatIntervalConsumer = (ignored) -> {};
		private TriFunction<String, Integer, String, RegisterApplicationMasterResponse> registerApplicationMasterFunction =
			(ignored1, ignored2, ignored3) -> new TestingRegisterApplicationMasterResponse(Collections::emptyList);
		private TriConsumer<FinalApplicationStatus, String, String> unregisterApplicationMasterConsumer = (ignored1, ignored2, ignored3) -> {};
		private Runnable clientInitRunnable = () -> {};
		private Runnable clientStartRunnable = () -> {};
		private Runnable clientStopRunnable = () -> {};

		private Builder() {}

		Builder setAddContainerRequestConsumer(BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> addContainerRequestConsumer) {
			this.addContainerRequestConsumer = addContainerRequestConsumer;
			return this;
		}

		Builder setGetMatchingRequestsFunction(Function<Tuple4<Priority, String, Resource, CallbackHandler>, List<? extends Collection<AMRMClient.ContainerRequest>>> getMatchingRequestsFunction) {
			this.getMatchingRequestsFunction = getMatchingRequestsFunction;
			return this;
		}

		Builder setRegisterApplicationMasterFunction(TriFunction<String, Integer, String, RegisterApplicationMasterResponse> registerApplicationMasterFunction) {
			this.registerApplicationMasterFunction = registerApplicationMasterFunction;
			return this;
		}

		Builder setReleaseAssignedContainerConsumer(BiConsumer<ContainerId, CallbackHandler> releaseAssignedContainerConsumer) {
			this.releaseAssignedContainerConsumer = releaseAssignedContainerConsumer;
			return this;
		}

		Builder setRemoveContainerRequestConsumer(BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> removeContainerRequestConsumer) {
			this.removeContainerRequestConsumer = removeContainerRequestConsumer;
			return this;
		}

		Builder setSetHeartbeatIntervalConsumer(Consumer<Integer> setHeartbeatIntervalConsumer) {
			this.setHeartbeatIntervalConsumer = setHeartbeatIntervalConsumer;
			return this;
		}

		Builder setUnregisterApplicationMasterConsumer(TriConsumer<FinalApplicationStatus, String, String> unregisterApplicationMasterConsumer) {
			this.unregisterApplicationMasterConsumer = unregisterApplicationMasterConsumer;
			return this;
		}

		Builder setClientInitRunnable(Runnable clientInitRunnable) {
			this.clientInitRunnable = clientInitRunnable;
			return this;
		}

		Builder setClientStartRunnable(Runnable clientStartRunnable) {
			this.clientStartRunnable = clientStartRunnable;
			return this;
		}

		Builder setClientStopRunnable(Runnable clientStopRunnable) {
			this.clientStopRunnable = clientStopRunnable;
			return this;
		}

		public TestingYarnAMRMClientAsync build(CallbackHandler callbackHandler) {
			return new TestingYarnAMRMClientAsync(
				callbackHandler,
				getMatchingRequestsFunction,
				addContainerRequestConsumer,
				removeContainerRequestConsumer,
				releaseAssignedContainerConsumer,
				setHeartbeatIntervalConsumer,
				registerApplicationMasterFunction,
				unregisterApplicationMasterConsumer,
				clientInitRunnable,
				clientStartRunnable,
				clientStopRunnable);
		}
	}
}
