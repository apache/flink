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
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * Factory which creates a {@link DispatcherRunnerImpl} which runs a {@link StandaloneDispatcher}.
 */
public class DispatcherRunnerFactoryImpl implements DispatcherRunnerFactory {

	private final DispatcherFactory dispatcherFactory;

	public DispatcherRunnerFactoryImpl(DispatcherFactory dispatcherFactory) {
		this.dispatcherFactory = dispatcherFactory;
	}

	@Override
	public DispatcherRunnerImpl createDispatcherRunner(
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices) throws Exception {
		return new DispatcherRunnerImpl(dispatcherFactory, rpcService, partialDispatcherServices);
	}
}
