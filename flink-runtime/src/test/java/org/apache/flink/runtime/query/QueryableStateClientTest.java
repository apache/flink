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

package org.apache.flink.runtime.query;

import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.netty.AtomicKvStateRequestStats;
import org.apache.flink.runtime.query.netty.KvStateClient;
import org.apache.flink.runtime.query.netty.KvStateServer;
import org.apache.flink.runtime.query.netty.UnknownKvStateID;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapValueState;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.MathUtils;
import org.junit.AfterClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.net.ConnectException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueryableStateClientTest {

	private static final ActorSystem testActorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

	private static final FiniteDuration timeout = new FiniteDuration(100, TimeUnit.SECONDS);

	@AfterClass
	public static void tearDown() throws Exception {
		if (testActorSystem != null) {
			testActorSystem.shutdown();
		}
	}

	/**
	 * All failures should lead to a retry with a forced location lookup.
	 *
	 * UnknownKvStateID, UnknownKvStateKeyGroupLocation, UnknownKvStateLocation,
	 * ConnectException are checked explicitly as these indicate out-of-sync
	 * KvStateLocation.
	 */
	@Test
	public void testForceLookupOnOutdatedLocation() throws Exception {
		KvStateLocationLookupService lookupService = mock(KvStateLocationLookupService.class);
		KvStateClient networkClient = mock(KvStateClient.class);

		QueryableStateClient client = new QueryableStateClient(
				lookupService,
				networkClient,
				testActorSystem.dispatcher());

		try {
			JobID jobId = new JobID();
			int numKeyGroups = 4;

			//
			// UnknownKvStateLocation
			//
			String query1 = "lucky";

			Future<KvStateLocation> unknownKvStateLocation = Futures.failed(
					new UnknownKvStateLocation(query1));

			when(lookupService.getKvStateLookupInfo(eq(jobId), eq(query1)))
					.thenReturn(unknownKvStateLocation);

			Future<byte[]> result = client.getKvState(
					jobId,
					query1,
					0,
					new byte[0]);

			try {
				Await.result(result, timeout);
				fail("Did not throw expected UnknownKvStateLocation exception");
			} catch (UnknownKvStateLocation ignored) {
				// Expected
			}

			verify(lookupService, times(2)).getKvStateLookupInfo(eq(jobId), eq(query1));

			//
			// UnknownKvStateKeyGroupLocation
			//
			String query2 = "unlucky";

			Future<KvStateLocation> unknownKeyGroupLocation = Futures.successful(
					new KvStateLocation(jobId, new JobVertexID(), numKeyGroups, query2));

			when(lookupService.getKvStateLookupInfo(eq(jobId), eq(query2)))
					.thenReturn(unknownKeyGroupLocation);

			result = client.getKvState(jobId, query2, 0, new byte[0]);

			try {
				Await.result(result, timeout);
				fail("Did not throw expected UnknownKvStateKeyGroupLocation exception");
			} catch (UnknownKvStateKeyGroupLocation ignored) {
				// Expected
			}

			verify(lookupService, times(2)).getKvStateLookupInfo(eq(jobId), eq(query2));

			//
			// UnknownKvStateID
			//
			String query3 = "water";
			KvStateID kvStateId = new KvStateID();
			Future<byte[]> unknownKvStateId = Futures.failed(new UnknownKvStateID(kvStateId));

			KvStateServerAddress serverAddress = new KvStateServerAddress(InetAddress.getLocalHost(), 12323);
			KvStateLocation location = new KvStateLocation(jobId, new JobVertexID(), numKeyGroups, query3);
			for (int i = 0; i < numKeyGroups; i++) {
				location.registerKvState(new KeyGroupRange(i, i), kvStateId, serverAddress);
			}

			when(lookupService.getKvStateLookupInfo(eq(jobId), eq(query3)))
					.thenReturn(Futures.successful(location));

			when(networkClient.getKvState(eq(serverAddress), eq(kvStateId), any(byte[].class)))
					.thenReturn(unknownKvStateId);

			result = client.getKvState(jobId, query3, 0, new byte[0]);

			try {
				Await.result(result, timeout);
				fail("Did not throw expected UnknownKvStateID exception");
			} catch (UnknownKvStateID ignored) {
				// Expected
			}

			verify(lookupService, times(2)).getKvStateLookupInfo(eq(jobId), eq(query3));

			//
			// ConnectException
			//
			String query4 = "space";
			Future<byte[]> connectException = Futures.failed(new ConnectException());
			kvStateId = new KvStateID();

			serverAddress = new KvStateServerAddress(InetAddress.getLocalHost(), 11123);
			location = new KvStateLocation(jobId, new JobVertexID(), numKeyGroups, query4);
			for (int i = 0; i < numKeyGroups; i++) {
				location.registerKvState(new KeyGroupRange(i, i), kvStateId, serverAddress);
			}

			when(lookupService.getKvStateLookupInfo(eq(jobId), eq(query4)))
					.thenReturn(Futures.successful(location));

			when(networkClient.getKvState(eq(serverAddress), eq(kvStateId), any(byte[].class)))
					.thenReturn(connectException);

			result = client.getKvState(jobId, query4, 0, new byte[0]);

			try {
				Await.result(result, timeout);
				fail("Did not throw expected ConnectException exception");
			} catch (ConnectException ignored) {
				// Expected
			}

			verify(lookupService, times(2)).getKvStateLookupInfo(eq(jobId), eq(query4));

			//
			// Other Exceptions don't lead to a retry no retry
			//
			String query5 = "universe";
			Future<KvStateLocation> exception = Futures.failed(new RuntimeException("Test exception"));
			when(lookupService.getKvStateLookupInfo(eq(jobId), eq(query5)))
					.thenReturn(exception);

			client.getKvState(jobId, query5, 0, new byte[0]);

			verify(lookupService, times(1)).getKvStateLookupInfo(eq(jobId), eq(query5));
		} finally {
			client.shutDown();
		}
	}

	/**
	 * Tests queries against multiple servers.
	 *
	 * <p>The servers are populated with different keys and the client queries
	 * all available keys from all servers.
	 */
	@Test
	public void testIntegrationWithKvStateServer() throws Exception {
		// Config
		int numServers = 2;
		int numKeys = 1024;
		int numKeyGroups = 1;

		JobID jobId = new JobID();
		JobVertexID jobVertexId = new JobVertexID();

		KvStateServer[] servers = new KvStateServer[numServers];
		AtomicKvStateRequestStats[] serverStats = new AtomicKvStateRequestStats[numServers];

		QueryableStateClient client = null;
		KvStateClient networkClient = null;
		AtomicKvStateRequestStats networkClientStats = new AtomicKvStateRequestStats();

		MemoryStateBackend backend = new MemoryStateBackend();
		DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);

		AbstractKeyedStateBackend<Integer> keyedStateBackend = backend.createKeyedStateBackend(dummyEnv,
				new JobID(),
				"test_op",
				IntSerializer.INSTANCE,
				numKeyGroups,
				new KeyGroupRange(0, 0),
				new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()));


		try {
			KvStateRegistry[] registries = new KvStateRegistry[numServers];
			KvStateID[] kvStateIds = new KvStateID[numServers];
			List<HeapValueState<Integer, VoidNamespace, Integer>> kvStates = new ArrayList<>();

			// Start the servers
			for (int i = 0; i < numServers; i++) {
				registries[i] = new KvStateRegistry();
				serverStats[i] = new AtomicKvStateRequestStats();
				servers[i] = new KvStateServer(InetAddress.getLocalHost(), 0, 1, 1, registries[i], serverStats[i]);
				servers[i].start();
				ValueStateDescriptor<Integer> descriptor =
						new ValueStateDescriptor<>("any", IntSerializer.INSTANCE, null);

				RegisteredBackendStateMetaInfo<VoidNamespace, Integer> registeredBackendStateMetaInfo = new RegisteredBackendStateMetaInfo<>(
						descriptor.getType(),
						descriptor.getName(),
						VoidNamespaceSerializer.INSTANCE,
						IntSerializer.INSTANCE);

				// Register state
				HeapValueState<Integer, VoidNamespace, Integer> kvState = new HeapValueState<>(
						keyedStateBackend,
						descriptor,
						new StateTable<Integer, VoidNamespace, Integer>(registeredBackendStateMetaInfo, new KeyGroupRange(0, 1)),
						IntSerializer.INSTANCE,
						VoidNamespaceSerializer.INSTANCE);

				kvStates.add(kvState);

				kvStateIds[i] = registries[i].registerKvState(
						jobId,
						new JobVertexID(),
						new KeyGroupRange(i, i),
						"choco",
						kvState);
			}

			int[] expectedRequests = new int[numServers];

			for (int key = 0; key < numKeys; key++) {
				int targetKeyGroupIndex = MathUtils.murmurHash(key) % numServers;
				expectedRequests[targetKeyGroupIndex]++;

				HeapValueState<Integer, VoidNamespace, Integer> kvState = kvStates.get(targetKeyGroupIndex);

				keyedStateBackend.setCurrentKey(key);
				kvState.setCurrentNamespace(VoidNamespace.INSTANCE);
				kvState.update(1337 + key);
			}

			// Location lookup service
			KvStateLocation location = new KvStateLocation(jobId, jobVertexId, numServers, "choco");
			for (int keyGroupIndex = 0; keyGroupIndex < numServers; keyGroupIndex++) {
				location.registerKvState(new KeyGroupRange(keyGroupIndex, keyGroupIndex), kvStateIds[keyGroupIndex], servers[keyGroupIndex].getAddress());
			}

			KvStateLocationLookupService lookupService = mock(KvStateLocationLookupService.class);
			when(lookupService.getKvStateLookupInfo(eq(jobId), eq("choco")))
					.thenReturn(Futures.successful(location));

			// The client
			networkClient = new KvStateClient(1, networkClientStats);

			client = new QueryableStateClient(lookupService, networkClient, testActorSystem.dispatcher());

			// Send all queries
			List<Future<byte[]>> futures = new ArrayList<>(numKeys);
			for (int key = 0; key < numKeys; key++) {
				byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
						key,
						IntSerializer.INSTANCE,
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE);

				futures.add(client.getKvState(jobId, "choco", key, serializedKeyAndNamespace));
			}

			// Verify results
			Future<Iterable<byte[]>> future = Futures.sequence(futures, testActorSystem.dispatcher());
			Iterable<byte[]> results = Await.result(future, timeout);

			int index = 0;
			for (byte[] buffer : results) {
				int deserializedValue = KvStateRequestSerializer.deserializeValue(buffer, IntSerializer.INSTANCE);
				assertEquals(1337 + index, deserializedValue);
				index++;
			}

			// Verify requests
			for (int i = 0; i < numServers; i++) {
				int numRetries = 10;
				for (int retry = 0; retry < numRetries; retry++) {
					try {
						assertEquals("Unexpected number of requests", expectedRequests[i], serverStats[i].getNumRequests());
						assertEquals("Unexpected success requests", expectedRequests[i], serverStats[i].getNumSuccessful());
						assertEquals("Unexpected failed requests", 0, serverStats[i].getNumFailed());
						break;
					} catch (Throwable t) {
						// Retry
						if (retry == numRetries-1) {
							throw t;
						} else {
							Thread.sleep(100);
						}
					}
				}
			}
		} finally {
			if (client != null) {
				client.shutDown();
			}

			if (networkClient != null) {
				networkClient.shutDown();
			}

			for (KvStateServer server : servers) {
				if (server != null) {
					server.shutDown();
				}
			}
		}
	}

	/**
	 * Tests that the QueryableState client correctly caches location lookups
	 * keyed by both job and name. This test is mainly due to a previous bug due
	 * to which cache entries were by name only. This is a problem, because the
	 * same client can be used to query multiple jobs.
	 */
	@Test
	public void testLookupMultipleJobIds() throws Exception {
		String name = "unique-per-job";

		// Exact contents don't matter here
		KvStateLocation location = new KvStateLocation(new JobID(), new JobVertexID(), 1, name);
		location.registerKvState(new KeyGroupRange(0, 0), new KvStateID(), new KvStateServerAddress(InetAddress.getLocalHost(), 892));

		JobID jobId1 = new JobID();
		JobID jobId2 = new JobID();

		KvStateLocationLookupService lookupService = mock(KvStateLocationLookupService.class);

		when(lookupService.getKvStateLookupInfo(any(JobID.class), anyString()))
				.thenReturn(Futures.successful(location));

		KvStateClient networkClient = mock(KvStateClient.class);
		when(networkClient.getKvState(any(KvStateServerAddress.class), any(KvStateID.class), any(byte[].class)))
				.thenReturn(Futures.successful(new byte[0]));

		QueryableStateClient client = new QueryableStateClient(
				lookupService,
				networkClient,
				testActorSystem.dispatcher());

		// Query ies with same name, but different job IDs should lead to a
		// single lookup per query and job ID.
		client.getKvState(jobId1, name, 0, new byte[0]);
		client.getKvState(jobId2, name, 0, new byte[0]);

		verify(lookupService, times(1)).getKvStateLookupInfo(eq(jobId1), eq(name));
		verify(lookupService, times(1)).getKvStateLookupInfo(eq(jobId2), eq(name));
	}
}
