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

package org.apache.flink.runtime.rpc;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An RPC Service implementation for testing. This RPC service acts as a replacement for
 * the regular RPC service for cases where tests need to return prepared mock gateways instead of
 * proper RPC gateways.
 * 
 * <p>The TestingRpcService can be used for example in the following fashion,
 * using <i>Mockito</i> for mocks and verification:
 * 
 * <pre>{@code
 * TestingRpcService rpc = new TestingRpcService();
 *
 * ResourceManagerGateway testGateway = mock(ResourceManagerGateway.class);
 * rpc.registerGateway("myAddress", testGateway);
 * 
 * MyComponentToTest component = new MyComponentToTest();
 * component.triggerSomethingThatCallsTheGateway();
 * 
 * verify(testGateway, timeout(1000)).theTestMethod(any(UUID.class), anyString());
 * }</pre>
 */
public class TestingRpcService extends AkkaRpcService {

	private static final Function<RpcGateway, CompletableFuture<RpcGateway>> DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION = CompletableFuture::completedFuture;

	/** Map of pre-registered connections. */
	private final ConcurrentHashMap<String, RpcGateway> registeredConnections;

	private volatile Function<RpcGateway, CompletableFuture<RpcGateway>> rpcGatewayFutureFunction = DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION;

	/**
	 * Creates a new {@code TestingRpcService}.
	 */
	public TestingRpcService() {
		this(new Configuration());
	}

	/**
	 * Creates a new {@code TestingRpcService}, using the given configuration.
	 */
	public TestingRpcService(Configuration configuration) {
		super(AkkaUtils.createLocalActorSystem(configuration), Time.seconds(10));

		this.registeredConnections = new ConcurrentHashMap<>();
	}

	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<Void> stopService() {
		final CompletableFuture<Void> terminationFuture = super.stopService();

		terminationFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				registeredConnections.clear();
			});

		return terminationFuture;
	}

	// ------------------------------------------------------------------------
	// connections
	// ------------------------------------------------------------------------

	public void registerGateway(String address, RpcGateway gateway) {
		checkNotNull(address);
		checkNotNull(gateway);

		if (registeredConnections.putIfAbsent(address, gateway) != null) {
			throw new IllegalStateException("a gateway is already registered under " + address);
		}
	}

	@SuppressWarnings("unchecked")
	private <C extends RpcGateway> CompletableFuture<C> getRpcGatewayFuture(C gateway) {
		return (CompletableFuture<C>) rpcGatewayFutureFunction.apply(gateway);
	}

	@Override
	public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
		RpcGateway gateway = registeredConnections.get(address);

		if (gateway != null) {
			if (clazz.isAssignableFrom(gateway.getClass())) {
				@SuppressWarnings("unchecked")
				C typedGateway = (C) gateway;
				return getRpcGatewayFuture(typedGateway);
			} else {
				return FutureUtils.completedExceptionally(new Exception("Gateway registered under " + address + " is not of type " + clazz));
			}
		} else {
			return super.connect(address, clazz);
		}
	}

	@Override
	public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
			String address,
			F fencingToken,
			Class<C> clazz) {
		RpcGateway gateway = registeredConnections.get(address);

		if (gateway != null) {
			if (clazz.isAssignableFrom(gateway.getClass())) {
				@SuppressWarnings("unchecked")
				C typedGateway = (C) gateway;
				return getRpcGatewayFuture(typedGateway);
			} else {
				return FutureUtils.completedExceptionally(new Exception("Gateway registered under " + address + " is not of type " + clazz));
			}
		} else {
			return super.connect(address, fencingToken, clazz);
		}
	}

	public void clearGateways() {
		registeredConnections.clear();
	}

	public void resetRpcGatewayFutureFunction() {
		rpcGatewayFutureFunction = DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION;
	}

	public void setRpcGatewayFutureFunction(Function<RpcGateway, CompletableFuture<RpcGateway>> rpcGatewayFutureFunction) {
		this.rpcGatewayFutureFunction = rpcGatewayFutureFunction;
	}
}
