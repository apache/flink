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

import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * {@link DispatcherRunnerFactory} implementation which creates {@link DispatcherRunnerImplNG}
 * instances.
 */
public class DispatcherRunnerImplNGFactory implements DispatcherRunnerFactory {
	private final DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory;

	private DispatcherRunnerImplNGFactory(DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory) {
		this.dispatcherLeaderProcessFactoryFactory = dispatcherLeaderProcessFactoryFactory;
	}

	@Override
	public DispatcherRunnerImplNG createDispatcherRunner(
			LeaderElectionService leaderElectionService,
			FatalErrorHandler fatalErrorHandler,
			JobGraphStoreFactory jobGraphStoreFactory,
			Executor ioExecutor,
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices) throws Exception {

		final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory = dispatcherLeaderProcessFactoryFactory.createFactory(
			jobGraphStoreFactory,
			ioExecutor,
			rpcService,
			partialDispatcherServices,
			fatalErrorHandler);

		return new DispatcherRunnerImplNG(
			leaderElectionService,
			fatalErrorHandler,
			dispatcherLeaderProcessFactory);
	}

	public static DispatcherRunnerImplNGFactory createSessionRunner(DispatcherFactory dispatcherFactory) {
		return new DispatcherRunnerImplNGFactory(
			SessionDispatcherLeaderProcessFactoryFactory.create(dispatcherFactory));
	}

	public static DispatcherRunnerImplNGFactory createJobRunner(JobGraphRetriever jobGraphRetriever) {
		return new DispatcherRunnerImplNGFactory(
			JobDispatcherLeaderProcessFactoryFactory.create(jobGraphRetriever));
	}
}
