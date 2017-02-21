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
package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorSystem;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;

public class AkkaRpcServiceTerminationTest extends TestLogger {
	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

	private static AkkaRpcService akkaRpcService =
		new AkkaRpcService(actorSystem, Time.milliseconds(10000));

	@AfterClass
	public static void shutdown() {
		akkaRpcService.stopService();
		actorSystem.shutdown();
	}

	/**
	 * Tests that we can wait for the termination of the rpc service
	 *
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test(timeout = 1000)
	public void testTerminationFuture() throws ExecutionException, InterruptedException {

		Future<Void> terminationFuture = akkaRpcService.getTerminationFuture();

		assertFalse(terminationFuture.isDone());

		FlinkFuture.supplyAsync(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				akkaRpcService.stopService();

				return null;
			}
		}, actorSystem.dispatcher());

		terminationFuture.get();
	}
}
