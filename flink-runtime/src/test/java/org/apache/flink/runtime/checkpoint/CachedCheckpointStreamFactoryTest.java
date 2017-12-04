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
package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.state.CachedCheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.mockito.Mockito.mock;

/**
 * Tests for the cached checkpoint stream factory.
 */
public class CachedCheckpointStreamFactoryTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testCachedCheckpointStateOutputStream() throws Exception {
		JobID jobID = new JobID();

		CheckpointCache checkpointCache = new CheckpointCache(
			jobID,
			tmp.newFolder().getAbsolutePath(),
			1000L,
			1000L,
			mock(CheckpointCacheManager.class),
			Executors.directExecutor()
		);

		CheckpointStreamFactory streamFactory = new FsCheckpointStreamFactory(
			new Path(tmp.newFolder().getAbsolutePath()),
			jobID,
			1024);

		CachedCheckpointStreamFactory factory = new CachedCheckpointStreamFactory(
			checkpointCache,
			streamFactory);
		StateHandleID handleID = new StateHandleID("handleId");
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = factory.createCheckpointStateOutputStream(1, 1, handleID);
		Assert.assertTrue(outputStream instanceof CachedCheckpointStreamFactory.CachedCheckpointStateOutputStream);

		String testStr = "hello";
		outputStream.write(testStr.getBytes());
		StreamStateHandle resultStateHandle = outputStream.closeAndGetHandle();
		Assert.assertTrue(resultStateHandle instanceof CachedStreamStateHandle);
		((CachedStreamStateHandle)resultStateHandle).setCheckpointCache(checkpointCache);

		checkpointCache.commitCache(1);

		byte[] buffer = new byte[1024];
		int n = resultStateHandle.openInputStream().read(buffer);
		Assert.assertEquals(testStr.length(), n);
		Assert.assertEquals(new String(buffer, 0, testStr.length()), testStr);
	}
}
