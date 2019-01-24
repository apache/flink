/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorSystem;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RPC sync invoke test.
 */
public class SyncCallsTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static final Time timeout = Time.seconds(10L);

	private static ActorSystem actorSystem1;
	private static ActorSystem actorSystem2;
	private static AkkaRpcService akkaRpcService1;
	private static AkkaRpcService akkaRpcService2;

	@BeforeClass
	public static void setup() {
		Configuration configuration = new Configuration();

		actorSystem1 = AkkaUtils.createDefaultActorSystem();
		actorSystem2 = AkkaUtils.createDefaultActorSystem();

		AkkaRpcServiceConfiguration akkaRpcServiceConfig = AkkaRpcServiceConfiguration.fromConfiguration(configuration);
		akkaRpcService1 = new AkkaRpcService(actorSystem1, akkaRpcServiceConfig);
		akkaRpcService2 = new AkkaRpcService(actorSystem2, akkaRpcServiceConfig);
	}

	@AfterClass
	public static void teardown() throws InterruptedException, ExecutionException, TimeoutException {
		final Collection<CompletableFuture<?>> terminationFutures = new ArrayList<>(4);

		terminationFutures.add(akkaRpcService1.stopService());
		terminationFutures.add(FutureUtils.toJava(actorSystem1.terminate()));
		terminationFutures.add(akkaRpcService2.stopService());
		terminationFutures.add(FutureUtils.toJava(actorSystem2.terminate()));

		FutureUtils
			.waitForAll(terminationFutures)
			.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	@Test
	public void testSimpleLocalSyncCall() throws Exception {
		RpcEndpoint rpcEndpoint = new DummyRpcEndpoint(akkaRpcService1);
		rpcEndpoint.start();

		try {
			DummyRpcGateway gateway = rpcEndpoint.getSelfGateway(DummyRpcGateway.class);

			int actualResult = gateway.foobar();

			assertEquals(1234, actualResult);
		} finally {
			rpcEndpoint.shutDown();
		}

	}

	@Test
	public void testSimpleRemoteSyncCall() throws Exception {
		RpcEndpoint rpcEndpoint = null;

		try {
			rpcEndpoint = new DummyRpcEndpoint(akkaRpcService1);
			rpcEndpoint.start();

			CompletableFuture<DummyRpcGateway> future = akkaRpcService2.connect(rpcEndpoint.getAddress(), DummyRpcGateway.class);
			DummyRpcGateway rpcGateway = future.get(10000, TimeUnit.SECONDS);

			int actualResult = rpcGateway.foobar();

			assertEquals(1234, actualResult);
		} finally {
			if (rpcEndpoint != null) {
				rpcEndpoint.shutDown();
			}
		}
	}

	@Test
	public void testSimpleRemoteSyncCallWithOversizedMsg() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(AkkaOptions.FRAMESIZE, "10 b");
		OversizedMsgRpcEndpoint rpcEndpoint = null;

		ActorSystem actorSystem1 = AkkaUtils.createDefaultActorSystem();
		ActorSystem actorSystem2 = AkkaUtils.createDefaultActorSystem();
		AkkaRpcServiceConfiguration akkaRpcServiceConfig = AkkaRpcServiceConfiguration.fromConfiguration(configuration);
		AkkaRpcService rpcService1 = new AkkaRpcService(actorSystem1, akkaRpcServiceConfig);;
		AkkaRpcService rpcService2 = new AkkaRpcService(actorSystem2, akkaRpcServiceConfig);;

		try {
			rpcEndpoint = new OversizedMsgRpcEndpoint(rpcService1, "hello world");

			rpcEndpoint.start();

			CompletableFuture<OversizedMsgRpcGateway> future = rpcService2.connect(
				rpcEndpoint.getAddress(), OversizedMsgRpcGateway.class);
			OversizedMsgRpcGateway rpcGateway = future.get(10000, TimeUnit.SECONDS);

			String result = rpcGateway.response();

			fail("Expected an AkkaRpcException.");
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof IOException);
		} finally {
			if (rpcEndpoint != null) {
				rpcEndpoint.shutDown();
			}

			final Collection<CompletableFuture<?>> terminationFutures = new ArrayList<>(4);
			terminationFutures.add(rpcService1.stopService());
			terminationFutures.add(FutureUtils.toJava(actorSystem1.terminate()));
			terminationFutures.add(rpcService2.stopService());
			terminationFutures.add(FutureUtils.toJava(actorSystem2.terminate()));

			FutureUtils
				.waitForAll(terminationFutures)
				.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * A dummy rpc gateway.
	 */
	public interface DummyRpcGateway extends RpcGateway {
		int foobar();
	}

	/**
	 * A dummy rpc endpoint.
	 */
	public static class DummyRpcEndpoint extends RpcEndpoint implements DummyRpcGateway {

		DummyRpcEndpoint(RpcService rpcService) {
			super(rpcService);
		}

		@Override
		public int foobar() {
			return 1234;
		}

		@Override
		public CompletableFuture<Void> postStop() {
			return CompletableFuture.completedFuture(null);
		}
	}

	/**
	 * Oversized message rpc gateway.
	 */
	private interface OversizedMsgRpcGateway extends RpcGateway {
		String response();
	}

	/**
	 * Oversized message rpc endpoint.
	 */
	private static class OversizedMsgRpcEndpoint extends RpcEndpoint implements OversizedMsgRpcGateway {

		private String txt;

		public OversizedMsgRpcEndpoint(RpcService rpcService, String txt) {
			super(rpcService);
			this.txt = txt;
		}

		@Override
		public CompletableFuture<Void> postStop() {
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public String response() {
			return this.txt;
		}
	}

}
