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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.runtime.dispatcher.DefaultJobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nonnull;

/**
 * {@link DispatcherFactory} which creates a {@link StandaloneDispatcher} which has an
 * endpoint id with a random UUID suffix.
 */
public enum SessionDispatcherWithUUIDFactory implements DispatcherFactory {
	INSTANCE;

	@Override
	public StandaloneDispatcher createDispatcher(
			@Nonnull RpcService rpcService,
			@Nonnull PartialDispatcherServices partialDispatcherServices) throws Exception {
		// create the default dispatcher
		return new StandaloneDispatcher(
			rpcService,
			generateEndpointIdWithUUID(),
			DispatcherServices.from(partialDispatcherServices, DefaultJobManagerRunnerFactory.INSTANCE));
	}
}
