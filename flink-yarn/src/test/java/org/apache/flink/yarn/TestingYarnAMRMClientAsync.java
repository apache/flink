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

	private volatile Function<Tuple4<Priority, String, Resource, CallbackHandler>, List<? extends Collection<AMRMClient.ContainerRequest>>>
		getMatchingRequestsFunction = ignored -> Collections.emptyList();
	private volatile BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> addContainerRequestConsumer = (ignored1, ignored2) -> {};
	private volatile BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> removeContainerRequestConsumer = (ignored1, ignored2) -> {};
	private volatile BiConsumer<ContainerId, CallbackHandler> releaseAssignedContainerConsumer = (ignored1, ignored2) -> {};
	private volatile Consumer<Integer> setHeartbeatIntervalConsumer = (ignored) -> {};
	private volatile TriFunction<String, Integer, String, RegisterApplicationMasterResponse> registerApplicationMasterFunction =
		(ignored1, ignored2, ignored3) -> RegisterApplicationMasterResponse.newInstance(
			Resource.newInstance(0, 0),
			Resource.newInstance(Integer.MAX_VALUE, Integer.MAX_VALUE),
			Collections.emptyMap(),
			null,
			Collections.emptyList(),
			null,
			Collections.emptyList());
	private volatile TriConsumer<FinalApplicationStatus, String, String> unregisterApplicationMasterConsumer = (ignored1, ignored2, ignored3) -> {};

	TestingYarnAMRMClientAsync(CallbackHandler callbackHandler) {
		super(0, callbackHandler);
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

	void setGetMatchingRequestsFunction(
		Function<Tuple4<Priority, String, Resource, CallbackHandler>, List<? extends Collection<AMRMClient.ContainerRequest>>>
			getMatchingRequestsFunction) {
		this.getMatchingRequestsFunction = Preconditions.checkNotNull(getMatchingRequestsFunction);
	}

	void setAddContainerRequestConsumer(
		BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> addContainerRequestConsumer) {
		this.addContainerRequestConsumer = Preconditions.checkNotNull(addContainerRequestConsumer);
	}

	void setRemoveContainerRequestConsumer(
		BiConsumer<AMRMClient.ContainerRequest, CallbackHandler> removeContainerRequestConsumer) {
		this.removeContainerRequestConsumer = Preconditions.checkNotNull(removeContainerRequestConsumer);
	}

	void setReleaseAssignedContainerConsumer(
		BiConsumer<ContainerId, CallbackHandler> releaseAssignedContainerConsumer) {
		this.releaseAssignedContainerConsumer = Preconditions.checkNotNull(releaseAssignedContainerConsumer);
	}

	void setSetHeartbeatIntervalConsumer(
		Consumer<Integer> setHeartbeatIntervalConsumer) {
		this.setHeartbeatIntervalConsumer = setHeartbeatIntervalConsumer;
	}

	void setRegisterApplicationMasterFunction(
		TriFunction<String, Integer, String, RegisterApplicationMasterResponse> registerApplicationMasterFunction) {
		this.registerApplicationMasterFunction = registerApplicationMasterFunction;
	}

	void setUnregisterApplicationMasterConsumer(
		TriConsumer<FinalApplicationStatus, String, String> unregisterApplicationMasterConsumer) {
		this.unregisterApplicationMasterConsumer = unregisterApplicationMasterConsumer;
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
}
