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

import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.ThrowingJobGraphWriter;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.Collections;
import java.util.UUID;

/**
 * {@link DispatcherLeaderProcess} implementation for the per-job mode.
 */
public class JobDispatcherLeaderProcess extends AbstractDispatcherLeaderProcess {

	private final DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;

	private final JobGraph jobGraph;

	JobDispatcherLeaderProcess(
			UUID leaderSessionId,
			DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
			JobGraph jobGraph,
			FatalErrorHandler fatalErrorHandler) {
		super(leaderSessionId, fatalErrorHandler);
		this.jobGraph = jobGraph;
		this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
	}

	@Override
	protected void onStart() {
		final DispatcherGatewayService dispatcherService = dispatcherGatewayServiceFactory.create(
			DispatcherId.fromUuid(getLeaderSessionId()),
			Collections.singleton(jobGraph),
			ThrowingJobGraphWriter.INSTANCE);

		completeDispatcherSetup(dispatcherService);
	}
}
