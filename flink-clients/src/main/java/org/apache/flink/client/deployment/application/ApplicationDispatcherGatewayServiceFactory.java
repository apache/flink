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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobGraphStore;
import org.apache.flink.runtime.dispatcher.runner.AbstractDispatcherLeaderProcess;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherGatewayService;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.runtime.dispatcher.runner.AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
 * DispatcherGatewayServiceFactory} used when executing a job in Application Mode, i.e. the user's main is executed on
 * the same machine as the {@link Dispatcher} and the lifecycle of the cluster is the same as the one of the application.
 *
 * <p>It instantiates a {@link org.apache.flink.runtime.dispatcher.runner.AbstractDispatcherLeaderProcess.DispatcherGatewayService
 * DispatcherGatewayService} with an {@link ApplicationDispatcherBootstrap} containing the user's program.
 */
@Internal
public class ApplicationDispatcherGatewayServiceFactory implements AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory {

	private final Configuration configuration;

	private final DispatcherFactory dispatcherFactory;

	private final PackagedProgram application;

	private final RpcService rpcService;

	private final PartialDispatcherServices partialDispatcherServices;

	public ApplicationDispatcherGatewayServiceFactory(
			Configuration configuration,
			DispatcherFactory dispatcherFactory,
			PackagedProgram application,
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices) {
		this.configuration = configuration;
		this.dispatcherFactory = dispatcherFactory;
		this.application = checkNotNull(application);
		this.rpcService = rpcService;
		this.partialDispatcherServices = partialDispatcherServices;
	}

	@Override
	public AbstractDispatcherLeaderProcess.DispatcherGatewayService create(
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			JobGraphWriter jobGraphWriter) {

		final List<JobID> recoveredJobIds = getRecoveredJobIds(recoveredJobs);

		final Dispatcher dispatcher;
		try {
			dispatcher = dispatcherFactory.createDispatcher(
					rpcService,
					fencingToken,
					recoveredJobs,
					(dispatcherGateway, scheduledExecutor, errorHandler) -> new ApplicationDispatcherBootstrap(
							application, recoveredJobIds, configuration, dispatcherGateway, scheduledExecutor, errorHandler),
					PartialDispatcherServicesWithJobGraphStore.from(partialDispatcherServices, jobGraphWriter));
		} catch (Exception e) {
			throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
		}

		dispatcher.start();

		return DefaultDispatcherGatewayService.from(dispatcher);
	}

	private List<JobID> getRecoveredJobIds(final Collection<JobGraph> recoveredJobs) {
		return recoveredJobs
				.stream()
				.map(JobGraph::getJobID)
				.collect(Collectors.toList());
	}
}
