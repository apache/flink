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

package org.apache.flink.runtime.rpc.heartbeat;

import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.util.Timeout;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * test for HeartbeatScheduler, including successful case, timeout case(retry on backoff timeout), failure case(retry)
 */
public class HeartbeatSchedulerTest extends TestLogger {
	private static final long INITIAL_INTERVAL = 200;
	private static final long INITIAL_TIMEOUT = 20;
	private static final long MAX_TIMEOUT = 150;
	private static final long DELAY_ON_ERROR = 100;
	private static final long MAX_ATTEMPT_TIME = 300;

	private RpcService rpcService;

	@Before
	public void setup() throws Exception {
		rpcService = new TestingRpcService();
	}

	@After
	public void teardown() throws Exception {
		rpcService.stopService();
	}


	/**
	 * test for receiving a regular heartbeat response in time, and checking the heartbeatResponse is properly delivered
	 * @throws Exception
	 */
	@Test
	public void testHeartbeatSuccessful() throws Exception {
		HeartbeatSender sender = mock(HeartbeatSender.class);
		UUID leaderSessionID = UUID.randomUUID();

		HeartbeatReceiverGateway targetGateway = mock(HeartbeatReceiverGateway.class);

		String response = "ok";
		when(targetGateway.triggerHeartbeat(any(UUID.class), any(FiniteDuration.class))).thenReturn(
			Futures.successful(response)
		);

		TestingHeartbeatScheduler heartbeatScheduler = new TestingHeartbeatScheduler(sender, rpcService, leaderSessionID,
			targetGateway, "taskExecutor-test-address", "testTargetGateway", log, INITIAL_INTERVAL, INITIAL_TIMEOUT, MAX_TIMEOUT, DELAY_ON_ERROR, MAX_ATTEMPT_TIME);
		heartbeatScheduler.start();

		// verify heartbeat successful and syncHeartbeatResponse is invoked for sync heartbeat response
		verify(sender, timeout(5000)).syncHeartbeatResponse(eq(response));
		// verify heartbeat trigger is still on
		Assert.assertFalse(heartbeatScheduler.isClosed());
		heartbeatScheduler.close();
	}

	/**
	 * test for fail upon first time, but successful after retry, and checking the heartbeatResponse is properly delivered
	 * @throws Exception
	 */
	@Test
	public void testHeartbeatRetrySuccessOnError() throws Exception {
		HeartbeatSender sender = mock(HeartbeatSender.class);
		UUID leaderSessionID = UUID.randomUUID();

		HeartbeatReceiverGateway targetGateway = mock(HeartbeatReceiverGateway.class);

		String response = "ok";
		// triggerHeartbeat fails upon first time, but success on the second
		when(targetGateway.triggerHeartbeat(any(UUID.class), any(FiniteDuration.class))).thenReturn(
			Futures.<String> failed(new Exception("error happened")), // first attempt fail
			Futures.successful(response) // second attempt success
		);

		TestingHeartbeatScheduler heartbeatScheduler = new TestingHeartbeatScheduler(sender, rpcService, leaderSessionID,
			targetGateway, "taskExecutor-test-address", "testTargetGateway", log, INITIAL_INTERVAL, INITIAL_TIMEOUT, MAX_TIMEOUT, DELAY_ON_ERROR, MAX_ATTEMPT_TIME);
		heartbeatScheduler.start();

		// verify heartbeat successful and syncHeartbeatResponse is invoked for sync heartbeat response
		verify(sender, timeout(5000)).syncHeartbeatResponse(eq(response));
		// verify heartbeat trigger is still on
		Assert.assertFalse(heartbeatScheduler.isClosed());
		heartbeatScheduler.close();
	}

	/**
	 * test loss of heartbeat and check mark the resource failed after retry max attempt times,
	 * @throws Exception
	 */
	@Test
	public void testLostHeartbeatOnError() throws Exception {
		HeartbeatSender sender = mock(HeartbeatSender.class);
		UUID leaderSessionID = UUID.randomUUID();

		HeartbeatReceiverGateway targetGateway = mock(HeartbeatReceiverGateway.class);

		// triggerHeartbeat always fail
		when(targetGateway.triggerHeartbeat(any(UUID.class), any(FiniteDuration.class))).thenReturn(
			Futures.<String> failed(new Exception("error happened")));

		TestingHeartbeatScheduler heartbeatScheduler = new TestingHeartbeatScheduler(sender, rpcService, leaderSessionID,
			targetGateway, "taskExecutor-test-address", "testTargetGateway", log, INITIAL_INTERVAL, INITIAL_TIMEOUT, MAX_TIMEOUT, DELAY_ON_ERROR, MAX_ATTEMPT_TIME);
		heartbeatScheduler.start();
		// verify lost heartbeat after max attempts and notifyLostHeartbeat is invoked
		verify(sender, timeout(5000)).notifyLostHeartbeat();
		// verify heartbeat scheduler will retry at most MAX_ATTEMPT_TIME time upon failure
		verify(targetGateway, atMost((int)Math.ceil(MAX_ATTEMPT_TIME/DELAY_ON_ERROR) + 1)).triggerHeartbeat(any(UUID.class), any(FiniteDuration.class));
		// verify the heartbeat scheduler is closed after lost heartbeat
		Assert.assertTrue(heartbeatScheduler.isClosed());
		heartbeatScheduler.close();
	}

	/**
	 * test backoff behavior upon timeout exception
	 * @throws Exception
	 */
	@Test
	public void testHeartbeatOnTimeoutException() throws Exception {
		HeartbeatSender sender = mock(HeartbeatSender.class);
		UUID leaderSessionID = UUID.randomUUID();

		HeartbeatReceiverGateway targetGateway = mock(HeartbeatReceiverGateway.class);

		String response = "ok";
		// triggerHeartbeat throws timeOutException upon first time, second time, third time, but success on the forth time
		when(targetGateway.triggerHeartbeat(any(UUID.class), any(FiniteDuration.class))).thenReturn(
			Futures.<String> failed(new TimeoutException("timeout exception happened")),
			Futures.<String> failed(new TimeoutException("timeout exception happened")),
			Futures.<String> failed(new TimeoutException("timeout exception happened")),
			Futures.successful(response) // last attempt success
		);

		TestingHeartbeatScheduler heartbeatScheduler = new TestingHeartbeatScheduler(sender, rpcService, leaderSessionID,
			targetGateway, "taskExecutor-test-address", "testTargetGateway", log, INITIAL_INTERVAL, INITIAL_TIMEOUT, MAX_TIMEOUT, DELAY_ON_ERROR, MAX_ATTEMPT_TIME);
		heartbeatScheduler.start();

		// verify heartbeat successful and syncHeartbeatResponse is invoked for report heartbeat response
		verify(sender, timeout(5000)).syncHeartbeatResponse(eq(response));

		// verify back-off behavior upon timeout exception
		FiniteDuration firstTimeout = new FiniteDuration(INITIAL_TIMEOUT, TimeUnit.MILLISECONDS);
		FiniteDuration secondTimeout = new FiniteDuration(INITIAL_TIMEOUT * 2, TimeUnit.MILLISECONDS);
		FiniteDuration thirdTimeout = new FiniteDuration(INITIAL_TIMEOUT * 4, TimeUnit.MILLISECONDS);
		// if back-off timeout exceeds maxTimeout, take  maxTimeout as next timeout value
		FiniteDuration forthTimeout = new FiniteDuration(MAX_TIMEOUT, TimeUnit.MILLISECONDS);
		verify(targetGateway, times(1)).triggerHeartbeat(any(UUID.class), eq(firstTimeout));
		verify(targetGateway, times(1)).triggerHeartbeat(any(UUID.class), eq(secondTimeout));
		verify(targetGateway, times(1)).triggerHeartbeat(any(UUID.class), eq(thirdTimeout));
		verify(targetGateway, times(1)).triggerHeartbeat(any(UUID.class), eq(forthTimeout));
		// verify heartbeat trigger is still on
		Assert.assertFalse(heartbeatScheduler.isClosed());
		heartbeatScheduler.close();
	}

	/**
	 * TestingHeartbeatScheduler class is responsible for testing
	 */
	private static class TestingHeartbeatScheduler extends HeartbeatScheduler<HeartbeatReceiverGateway, String> {
		private HeartbeatSender sender;

		public TestingHeartbeatScheduler(HeartbeatSender heartbeatSender, RpcService rpcService, UUID leaderID, HeartbeatReceiverGateway targetGateway,
			String targetAddress, String targetName, Logger log, long heartbeatInterval,
			long heartbeatTimeout, long maxHeartbeatTimeout, long delayOnError, long maxAttemptTime) {
			super(rpcService, leaderID, targetGateway , targetAddress,
				"TestingHeartbeatReceiver", log, heartbeatInterval, heartbeatTimeout, maxHeartbeatTimeout, delayOnError, maxAttemptTime);
			this.sender = heartbeatSender;
		}

		@Override
		protected Future<String> triggerHeartbeat(UUID leaderID, FiniteDuration timeout) {
			return targetGateway.triggerHeartbeat(leaderID, timeout);
		}

		@Override
		protected void reportHeartbeatPayload(String heartbeatResponsePayload) {
			this.sender.syncHeartbeatResponse(heartbeatResponsePayload);
		}

		@Override
		protected void lostHeartbeat() {
			this.sender.notifyLostHeartbeat();
		}
	}

	private static interface HeartbeatSender {
		void syncHeartbeatResponse(String payload);

		void notifyLostHeartbeat();
	}

	private static interface HeartbeatReceiverGateway extends RpcGateway {
		Future<String> triggerHeartbeat(UUID leaderId, @RpcTimeout FiniteDuration timeout);
	}

}
