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

import akka.actor.ActorSystem;
import akka.util.Timeout;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import org.junit.AfterClass;
import org.junit.Test;

import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * This test validates that the RPC service gives a good message when it cannot
 * connect to an RpcEndpoint.
 */
public class RpcConnectionTest {

	@Test
	public void testConnectFailure() {
		ActorSystem actorSystem = null;
		RpcService rpcService = null;
		try {
			actorSystem = AkkaUtils.createActorSystem(
					new Configuration(), Option.apply(new Tuple2<String, Object>("localhost", 0)));

			// we start the RPC service with a very long timeout to ensure that the test
			// can only pass if the connection problem is not recognized merely via a timeout
			rpcService = new AkkaRpcService(actorSystem, new Timeout(10000000, TimeUnit.SECONDS));

			Future<TaskExecutorGateway> future = rpcService.connect("foo.bar.com.test.invalid", TaskExecutorGateway.class);

			Await.result(future, new FiniteDuration(10000000, TimeUnit.SECONDS));
			fail("should never complete normally");
		}
		catch (TimeoutException e) {
			fail("should not fail with a generic timeout exception");
		}
		catch (RpcConnectionException e) {
			// that is what we want
			assertTrue("wrong error message", e.getMessage().contains("foo.bar.com.test.invalid"));
		}
		catch (Throwable t) {
			fail("wrong exception: " + t);
		}
		finally {
			if (rpcService != null) {
				rpcService.stopService();
			}
			if (actorSystem != null) {
				actorSystem.shutdown();
			}
		}
	}
}
