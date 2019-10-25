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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.concurrent.CompletableFuture;

class DispatcherServiceImpl implements DispatcherLeaderProcessImpl.DispatcherService {

	private final Dispatcher dispatcher;
	private final DispatcherGateway dispatcherGateway;

	private DispatcherServiceImpl(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		this.dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
	}

	@Override
	public DispatcherGateway getGateway() {
		return dispatcherGateway;
	}

	@Override
	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
		return dispatcher.onRemovedJobGraph(jobId);
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return dispatcher.closeAsync();
	}

	public static DispatcherServiceImpl from(Dispatcher dispatcher) {
		return new DispatcherServiceImpl(dispatcher);
	}
}
