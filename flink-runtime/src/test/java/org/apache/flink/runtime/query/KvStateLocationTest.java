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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;

public class KvStateLocationTest {

	/**
	 * Simple test registering/unregistereing state and looking it up again.
	 */
	@Test
	public void testRegisterAndLookup() throws Exception {
		JobID jobId = new JobID();
		JobVertexID jobVertexId = new JobVertexID();
		int numKeyGroups = 123;
		String registrationName = "asdasdasdasd";

		KvStateLocation location = new KvStateLocation(jobId, jobVertexId, numKeyGroups, registrationName);

		KvStateID[] kvStateIds = new KvStateID[numKeyGroups];
		KvStateServerAddress[] serverAddresses = new KvStateServerAddress[numKeyGroups];

		InetAddress host = InetAddress.getLocalHost();

		// Register
		for (int keyGroupIndex = 0; keyGroupIndex < numKeyGroups; keyGroupIndex++) {
			kvStateIds[keyGroupIndex] = new KvStateID();
			serverAddresses[keyGroupIndex] = new KvStateServerAddress(host, 1024 + keyGroupIndex);

			location.registerKvState(keyGroupIndex, kvStateIds[keyGroupIndex], serverAddresses[keyGroupIndex]);
			assertEquals(keyGroupIndex + 1, location.getNumRegisteredKeyGroups());
		}

		// Lookup
		for (int keyGroupIndex = 0; keyGroupIndex < numKeyGroups; keyGroupIndex++) {
			assertEquals(kvStateIds[keyGroupIndex], location.getKvStateID(keyGroupIndex));
			assertEquals(serverAddresses[keyGroupIndex], location.getKvStateServerAddress(keyGroupIndex));
		}

		// Overwrite
		for (int keyGroupIndex = 0; keyGroupIndex < numKeyGroups; keyGroupIndex++) {
			kvStateIds[keyGroupIndex] = new KvStateID();
			serverAddresses[keyGroupIndex] = new KvStateServerAddress(host, 1024 + keyGroupIndex);

			location.registerKvState(keyGroupIndex, kvStateIds[keyGroupIndex], serverAddresses[keyGroupIndex]);
			assertEquals(numKeyGroups, location.getNumRegisteredKeyGroups());
		}

		// Lookup
		for (int keyGroupIndex = 0; keyGroupIndex < numKeyGroups; keyGroupIndex++) {
			assertEquals(kvStateIds[keyGroupIndex], location.getKvStateID(keyGroupIndex));
			assertEquals(serverAddresses[keyGroupIndex], location.getKvStateServerAddress(keyGroupIndex));
		}

		// Unregister
		for (int keyGroupIndex = 0; keyGroupIndex < numKeyGroups; keyGroupIndex++) {
			location.unregisterKvState(keyGroupIndex);
			assertEquals(numKeyGroups - keyGroupIndex - 1, location.getNumRegisteredKeyGroups());
		}

		// Lookup
		for (int keyGroupIndex = 0; keyGroupIndex < numKeyGroups; keyGroupIndex++) {
			assertEquals(null, location.getKvStateID(keyGroupIndex));
			assertEquals(null, location.getKvStateServerAddress(keyGroupIndex));
		}

		assertEquals(0, location.getNumRegisteredKeyGroups());
	}
}
