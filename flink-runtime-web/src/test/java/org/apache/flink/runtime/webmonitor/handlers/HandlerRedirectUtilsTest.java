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

import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.DummyActorGateway;
import org.junit.Test;
import org.junit.Assert;
import scala.Tuple2;

public class HandlerRedirectUtilsTest {

	static final String localJobManagerAddress = "akka.tcp://flink@127.0.0.1:1234/user/foobar";
	static final String remoteURL = "127.0.0.2:1235";
	static final String remotePath = "akka.tcp://flink@" + remoteURL + "/user/jobmanager";

	@Test
	public void testGetRedirectAddressWithLocalAkkaPath() throws Exception {
		ActorGateway leaderGateway = new DummyActorGateway("akka://flink/user/foobar");

		Tuple2<ActorGateway, Integer> leader = new Tuple2<>(leaderGateway, 1235);

		String redirectingAddress =HandlerRedirectUtils.getRedirectAddress(localJobManagerAddress, leader);

		Assert.assertNull(redirectingAddress);
	}

	@Test
	public void testGetRedirectAddressWithRemoteAkkaPath() throws Exception {
		ActorGateway leaderGateway = new DummyActorGateway(remotePath);

		Tuple2<ActorGateway, Integer> leader = new Tuple2<>(leaderGateway, 1235);

		String redirectingAddress =HandlerRedirectUtils.getRedirectAddress(localJobManagerAddress, leader);

		Assert.assertEquals(remoteURL, redirectingAddress);
	}
}
