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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.After;
import org.junit.Before;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Tests {@link ExecutionGraph} deployment when offloading job and task information into the BLOB
 * server.
 */
public class ExecutionGraphDeploymentWithBlobServerTest extends ExecutionGraphDeploymentTest {

	private Set<byte[]> seenHashes = Collections.newSetFromMap(new ConcurrentHashMap<byte[], Boolean>());

	@Before
	public void setupBlobServer() throws IOException {
		Configuration config = new Configuration();
		// always offload the serialized job and task information
		config.setInteger(JobManagerOptions.TDD_OFFLOAD_MINSIZE, 0);
		blobServer = Mockito.spy(new BlobServer(config, new VoidBlobStore()));

		seenHashes.clear();

		// verify that we do not upload the same content more than once
		doAnswer(
			invocation -> {
				PermanentBlobKey key = (PermanentBlobKey) invocation.callRealMethod();

				assertTrue(seenHashes.add(key.getHash()));

				return key;
			}
		).when(blobServer).putPermanent(any(JobID.class), Matchers.<byte[]>any());

		blobServer.start();
	}

	@After
	public void shutdownBlobServer() throws IOException {
		if (blobServer != null) {
			blobServer.close();
		}
	}

	@Override
	protected void checkJobOffloaded(ExecutionGraph eg) throws Exception {
		PermanentBlobKey jobInformationBlobKey = eg.getJobInformationBlobKey();
		assertNotNull(jobInformationBlobKey);

		// must not throw:
		blobServer.getFile(eg.getJobID(), jobInformationBlobKey);
	}

	@Override
	protected void checkTaskOffloaded(ExecutionGraph eg, JobVertexID jobVertexId) throws Exception {
		PermanentBlobKey taskInformationBlobKey = eg.getJobVertex(jobVertexId).getTaskInformationBlobKey();
		assertNotNull(taskInformationBlobKey);

		// must not throw:
		blobServer.getFile(eg.getJobID(), taskInformationBlobKey);
	}
}
