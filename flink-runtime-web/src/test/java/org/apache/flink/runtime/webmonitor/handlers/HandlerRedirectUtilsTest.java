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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the HandlerRedirectUtils.
 */
public class HandlerRedirectUtilsTest extends TestLogger {

	private static final String localJobManagerAddress = "akka.tcp://flink@127.0.0.1:1234/user/foobar";
	private static final String remoteHostname = "127.0.0.2";
	private static final int webPort = 1235;
	private static final String remoteURL = remoteHostname + ':' + webPort;
	private static final String remotePath = "akka.tcp://flink@" + remoteURL + "/user/jobmanager";

	@Test
	public void testGetRedirectAddressWithLocalAkkaPath() throws Exception {
		JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		when(jobManagerGateway.getAddress()).thenReturn("akka://flink/user/foobar");

		String redirectingAddress = HandlerRedirectUtils.getRedirectAddress(
			localJobManagerAddress,
			jobManagerGateway,
			Time.seconds(3L));

		Assert.assertNull(redirectingAddress);
	}

	@Test
	public void testGetRedirectAddressWithRemoteAkkaPath() throws Exception {
		JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		when(jobManagerGateway.getAddress()).thenReturn(remotePath);
		when(jobManagerGateway.getHostname()).thenReturn(remoteHostname);
		when(jobManagerGateway.requestWebPort(any(Time.class))).thenReturn(CompletableFuture.completedFuture(webPort));

		String redirectingAddress = HandlerRedirectUtils.getRedirectAddress(
			localJobManagerAddress,
			jobManagerGateway,
			Time.seconds(3L));

		Assert.assertEquals(remoteURL, redirectingAddress);
	}
}
