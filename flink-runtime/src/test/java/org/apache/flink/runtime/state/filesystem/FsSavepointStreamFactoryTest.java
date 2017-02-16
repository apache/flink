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

package org.apache.flink.runtime.state.filesystem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FsSavepointStreamFactoryTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	/**
	 * Tests that the factory creates all files in the given directory without
	 * creating any sub directories.
	 */
	@Test
	public void testSavepointStreamDirectoryLayout() throws Exception {
		File testRoot = folder.newFolder();
		JobID jobId = new JobID();

		FsSavepointStreamFactory savepointStreamFactory = new FsSavepointStreamFactory(
				new Path(testRoot.getAbsolutePath()),
				jobId,
				0);

		File[] listed = testRoot.listFiles();
		assertNotNull(listed);
		assertEquals(0, listed.length);

		FsCheckpointStateOutputStream stream = savepointStreamFactory
			.createCheckpointStateOutputStream(1273, 19231);

		stream.write(1);

		FileStateHandle handle = (FileStateHandle) stream.closeAndGetHandle();

		listed = testRoot.listFiles();
		assertNotNull(listed);
		assertEquals(1, listed.length);
		assertEquals(handle.getFilePath().getPath(), listed[0].getPath());
	}
}
