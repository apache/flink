/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Test for {@link FsSegmentCheckpointStreamFactory}.
 */
public class FsSegmentCheckpointStreamFactoryTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testCloseOldCheckpointFile() throws Exception {
		int writeBufferSize = 1024;
		FsSegmentCheckpointStreamFactory checkpointStreamFactory = new FsSegmentCheckpointStreamFactory(
			LocalFileSystem.getLocalFileSystem(),
			Path.fromLocalFile(temporaryFolder.newFolder()),
			Path.fromLocalFile(temporaryFolder.newFolder()),
			Path.fromLocalFile(temporaryFolder.newFolder()),
			writeBufferSize,
			1024,
			1);

		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(1, CheckpointedStateScope.SHARED);
		// write writeBufferSize + 10 ensure that we create a file for checkpoint 1.
		for (int i = 0; i < writeBufferSize + 10; ++i) {
			outputStream.write(ThreadLocalRandom.current().nextInt());
		}
		Map<Long, FSDataOutputStream> rawOutputSteams = checkpointStreamFactory.getFileOutputStreams();
		FSDataOutputStream rawOutputSteam = rawOutputSteams.get(1L);
		FSDataOutputStream spyOutputStream = spy(rawOutputSteam);
		// reset the outputstream, so that we can verify its close.
		rawOutputSteams.put(1L, spyOutputStream);

		checkpointStreamFactory.createCheckpointStateOutputStream(2, CheckpointedStateScope.SHARED);
		verify(spyOutputStream).close();
	}
}

