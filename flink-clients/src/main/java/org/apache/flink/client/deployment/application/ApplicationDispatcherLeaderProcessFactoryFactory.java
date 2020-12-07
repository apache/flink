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
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.runner.DispatcherLeaderProcessFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherLeaderProcessFactoryFactory;
import org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcessFactory;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Factory for a {@link DispatcherLeaderProcessFactoryFactory} designed to be used when executing
 * an application in Application Mode.
 */
@Internal
public class ApplicationDispatcherLeaderProcessFactoryFactory implements DispatcherLeaderProcessFactoryFactory {

	private final Configuration configuration;

	private final DispatcherFactory dispatcherFactory;

	private final PackagedProgram program;

	private ApplicationDispatcherLeaderProcessFactoryFactory(
			final Configuration configuration,
			final DispatcherFactory dispatcherFactory,
			final PackagedProgram program) {
		this.configuration = checkNotNull(configuration);
		this.dispatcherFactory = checkNotNull(dispatcherFactory);
		this.program = checkNotNull(program);
	}

	@Override
	public DispatcherLeaderProcessFactory createFactory(
			JobGraphStoreFactory jobGraphStoreFactory,
			Executor ioExecutor,
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices,
			FatalErrorHandler fatalErrorHandler) {

		final ApplicationDispatcherGatewayServiceFactory dispatcherServiceFactory = new ApplicationDispatcherGatewayServiceFactory(
				configuration,
				dispatcherFactory,
				program,
				rpcService,
				partialDispatcherServices);

		return new SessionDispatcherLeaderProcessFactory(
				dispatcherServiceFactory,
				jobGraphStoreFactory,
				ioExecutor,
				fatalErrorHandler);
	}

	public static ApplicationDispatcherLeaderProcessFactoryFactory create(
			final Configuration configuration,
			final DispatcherFactory dispatcherFactory,
			final PackagedProgram program) {
		return new ApplicationDispatcherLeaderProcessFactoryFactory(configuration, dispatcherFactory, program);
	}
}
