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

package org.apache.flink.runtime.webmonitor.retriever;

import org.apache.flink.runtime.rpc.RpcGateway;

import java.util.concurrent.CompletableFuture;

/**
 * Testing {@link GatewayRetriever} implementation which can be
 * manually set.
 *
 * @param <T> type of the gateway
 */
public class TestingGatewayRetriever<T extends RpcGateway> implements GatewayRetriever<T> {
	private CompletableFuture<T> gatewayFuture;

	public TestingGatewayRetriever() {
		gatewayFuture = new CompletableFuture<>();
	}

	@Override
	public CompletableFuture<T> getFuture() {
		return null;
	}

	public void setGateway(T gateway) {
		gatewayFuture = CompletableFuture.completedFuture(gateway);
	}
}
