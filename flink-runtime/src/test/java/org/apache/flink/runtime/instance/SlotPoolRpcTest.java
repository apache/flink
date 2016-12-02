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

package org.apache.flink.runtime.instance;

import akka.actor.ActorSystem;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.util.clock.SystemClock;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.instance.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests for the SlotPool using a proper RPC setup.
 */
public class SlotPoolRpcTest {

	private static RpcService rpcService;

	// ------------------------------------------------------------------------
	//  setup
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setup() {
		ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());
		rpcService = new AkkaRpcService(actorSystem, Time.seconds(10));
	}

	@AfterClass
	public static  void shutdown() {
		rpcService.stopService();
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testSlotAllocationNoResourceManager() throws Exception {
		final JobID jid = new JobID();
		
		final SlotPool pool = new SlotPool(
				rpcService, jid,
				SystemClock.getInstance(),
				Time.days(1), Time.days(1),
				Time.milliseconds(100) // this is the timeout for the request tested here
		);
		pool.start(UUID.randomUUID());

		Future<SimpleSlot> future = pool.allocateSlot(mock(ScheduledUnit.class), DEFAULT_TESTING_PROFILE, null);

		try {
			future.get(4, TimeUnit.SECONDS);
			fail("We expected a ExecutionException.");
		}
		catch (ExecutionException e) {
			assertEquals(NoResourceAvailableException.class, e.getCause().getClass());
		}
		catch (TimeoutException e) {
			fail("future timed out rather than being failed");
		}
		catch (Exception e) {
			fail("wrong exception: " + e);
		}
	}
}
