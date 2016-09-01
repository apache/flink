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

import akka.dispatch.Futures;
import akka.util.Timeout;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;

import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An RPC Service implementation for testing. This RPC service acts as a replacement for
 * teh regular RPC service for cases where tests need to return prepared mock gateways instead of
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

	/** Map of pre-registered connections */
	private final ConcurrentHashMap<String, RpcGateway> registeredConnections;

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
		super(AkkaUtils.createLocalActorSystem(configuration), new Timeout(new FiniteDuration(10, TimeUnit.SECONDS)));

		this.registeredConnections = new ConcurrentHashMap<>();
	}

	// ------------------------------------------------------------------------

	@Override
	public void stopService() {
		super.stopService();
		registeredConnections.clear();
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

	@Override
	public <C extends RpcGateway> Future<C> connect(String address, Class<C> clazz) {
		RpcGateway gateway = registeredConnections.get(address);

		if (gateway != null) {
			if (clazz.isAssignableFrom(gateway.getClass())) {
				@SuppressWarnings("unchecked")
				C typedGateway = (C) gateway;
				return Futures.successful(typedGateway);
			} else {
				return Futures.failed(
						new Exception("Gateway registered under " + address + " is not of type " + clazz));
			}
		} else {
			return Futures.failed(new Exception("No gateway registered under that name"));
		}
	}

	public void clearGateways() {
		registeredConnections.clear();
	}
}
