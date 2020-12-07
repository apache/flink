/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.NoOpDispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobGraphStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;

/**
 * Factory for the {@link DefaultDispatcherGatewayService}.
 */
class DefaultDispatcherGatewayServiceFactory implements AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory {

	private final DispatcherFactory dispatcherFactory;

	private final RpcService rpcService;

	private final PartialDispatcherServices partialDispatcherServices;

	DefaultDispatcherGatewayServiceFactory(
			DispatcherFactory dispatcherFactory,
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices) {
		this.dispatcherFactory = dispatcherFactory;
		this.rpcService = rpcService;
		this.partialDispatcherServices = partialDispatcherServices;
	}

	@Override
	public AbstractDispatcherLeaderProcess.DispatcherGatewayService create(
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			JobGraphWriter jobGraphWriter) {

		final Dispatcher dispatcher;
		try {
			dispatcher = dispatcherFactory.createDispatcher(
				rpcService,
				fencingToken,
				recoveredJobs,
				(dispatcherGateway, scheduledExecutor, errorHandler) -> new NoOpDispatcherBootstrap(),
				PartialDispatcherServicesWithJobGraphStore.from(partialDispatcherServices, jobGraphWriter));
		} catch (Exception e) {
			throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
		}

		dispatcher.start();

		return DefaultDispatcherGatewayService.from(dispatcher);
	}
}
