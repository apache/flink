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

package org.apache.flink.streaming.connectors.gcp.pubsub.emulator;

import org.apache.flink.util.TestLogger;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.spotify.docker.client.exceptions.DockerException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.streaming.connectors.gcp.pubsub.emulator.GCloudEmulatorManager.getDockerIpAddress;
import static org.apache.flink.streaming.connectors.gcp.pubsub.emulator.GCloudEmulatorManager.getDockerPubSubPort;

/**
 * The base class from which unit tests should inherit if they need to use the Google cloud emulators.
 */
public class GCloudUnitTestBase extends TestLogger implements Serializable {
	@BeforeClass
	public static void launchGCloudEmulator() throws Exception {
		// Separated out into separate class so the entire test class to be serializable
		GCloudEmulatorManager.launchDocker();
	}

	@AfterClass
	public static void terminateGCloudEmulator() throws DockerException, InterruptedException {
		channel.shutdownNow();
		channel.awaitTermination(1, TimeUnit.MINUTES);
		channel = null;
		GCloudEmulatorManager.terminateDocker();
	}

	// ====================================================================================
	// Pubsub helpers

	private static ManagedChannel channel = null;
	private static TransportChannelProvider channelProvider = null;

	public static PubsubHelper getPubsubHelper() {
		if (channel == null) {
			//noinspection deprecation
			channel = ManagedChannelBuilder
				.forTarget(getPubSubHostPort())
				.usePlaintext()
				.build();
			channelProvider = FixedTransportChannelProvider
				.create(GrpcTransportChannel.create(channel));
		}
		return new PubsubHelper(channelProvider);
	}

	public static String getPubSubHostPort() {
		return getDockerIpAddress() + ":" + getDockerPubSubPort();
	}

	@AfterClass
	public static void cleanupPubsubChannel() throws InterruptedException {
		if (channel != null) {
			channel.shutdownNow().awaitTermination(1, SECONDS);
			channel = null;
		}
	}
}
