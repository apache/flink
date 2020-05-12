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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Factory for the {@link SessionDispatcherLeaderProcess}.
 */
@Internal
public class SessionDispatcherLeaderProcessFactory implements DispatcherLeaderProcessFactory {

	private final AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;
	private final JobGraphStoreFactory jobGraphStoreFactory;
	private final Executor ioExecutor;
	private final FatalErrorHandler fatalErrorHandler;

	public SessionDispatcherLeaderProcessFactory(
			AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
			JobGraphStoreFactory jobGraphStoreFactory,
			Executor ioExecutor,
			FatalErrorHandler fatalErrorHandler) {
		this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
		this.jobGraphStoreFactory = jobGraphStoreFactory;
		this.ioExecutor = ioExecutor;
		this.fatalErrorHandler = fatalErrorHandler;
	}

	@Override
	public DispatcherLeaderProcess create(UUID leaderSessionID) {
		return SessionDispatcherLeaderProcess.create(
			leaderSessionID,
			dispatcherGatewayServiceFactory,
			jobGraphStoreFactory.create(),
			ioExecutor,
			fatalErrorHandler);
	}
}
