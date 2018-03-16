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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaJobManagerGateway;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.util.HandlerRedirectUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the HandlerRedirectUtils.
 */
public class HandlerRedirectUtilsTest extends TestLogger {

	private static final String localRestAddress = "http://127.0.0.1:1234";
	private static final String remoteRestAddress = "http://127.0.0.2:1234";

	@Test
	public void testGetRedirectAddressWithLocalEqualsRemoteRESTAddress() throws Exception {
		JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		when(jobManagerGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(localRestAddress));

		CompletableFuture<Optional<String>> redirectingAddressFuture = HandlerRedirectUtils.getRedirectAddress(
			localRestAddress,
			jobManagerGateway,
			Time.seconds(3L));

		Assert.assertTrue(redirectingAddressFuture.isDone());
		// no redirection needed
		Assert.assertFalse(redirectingAddressFuture.get().isPresent());
	}

	@Test
	public void testGetRedirectAddressWithRemoteAkkaPath() throws Exception {
		JobManagerGateway jobManagerGateway = mock(AkkaJobManagerGateway.class);
		when(jobManagerGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(remoteRestAddress));

		CompletableFuture<Optional<String>> optRedirectingAddress = HandlerRedirectUtils.getRedirectAddress(
			localRestAddress,
			jobManagerGateway,
			Time.seconds(3L));

		Assert.assertTrue(optRedirectingAddress.isDone());

		Assert.assertEquals(remoteRestAddress, optRedirectingAddress.get().get());
	}
}
