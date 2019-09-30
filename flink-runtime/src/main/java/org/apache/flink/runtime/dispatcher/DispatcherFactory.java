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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nonnull;

import java.util.UUID;

/**
 * {@link Dispatcher} factory interface.
 */
public interface DispatcherFactory<T extends Dispatcher> {

	/**
	 * Create a {@link Dispatcher} of the given type {@link T}.
	 */
	T createDispatcher(
		@Nonnull RpcService rpcService,
		@Nonnull PartialDispatcherServices partialDispatcherServices) throws Exception;

	default String generateEndpointIdWithUUID() {
		return getEndpointId() + UUID.randomUUID();
	}

	default String getEndpointId() {
		return Dispatcher.DISPATCHER_NAME;
	}
}
