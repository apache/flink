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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for the {@link FsCheckpointStreamFactory}.
 */
public class FsCheckpointStreamFactoryTest {

	@Rule
	public final TemporaryFolder TMP = new TemporaryFolder();

	private Path exclusiveStateDir;
	private Path sharedStateDir;

	@Before
	public void createStateDirectories() throws IOException {
		exclusiveStateDir = Path.fromLocalFile(TMP.newFolder("exclusive"));
		sharedStateDir = Path.fromLocalFile(TMP.newFolder("shared"));
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testExclusiveStateHasRelativePathHandles() throws IOException {
		final FsCheckpointStreamFactory factory = createFactory(FileSystem.getLocalFileSystem());

		final FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
				factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
		stream.write(1657);
		final StreamStateHandle handle = stream.closeAndGetHandle();

		assertThat(handle, instanceOf(RelativeFileStateHandle.class));
		assertPathsEqual(exclusiveStateDir, ((RelativeFileStateHandle) handle).getFilePath().getParent());
	}

	@Test
	public void testSharedStateHasAbsolutePathHandles() throws IOException {
		final FsCheckpointStreamFactory factory = createFactory(FileSystem.getLocalFileSystem());

		final FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
			factory.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
		stream.write(0);
		final StreamStateHandle handle = stream.closeAndGetHandle();

		assertThat(handle, instanceOf(FileStateHandle.class));
		assertThat(handle, not(instanceOf(RelativeFileStateHandle.class)));
		assertPathsEqual(sharedStateDir, ((FileStateHandle) handle).getFilePath().getParent());
	}

	@Test
	public void testEntropyMakesExclusiveStateAbsolutePaths() throws IOException{
		final FsCheckpointStreamFactory factory = createFactory(new FsStateBackendEntropyTest.TestEntropyAwareFs());

		final FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
			factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
		stream.write(0);
		final StreamStateHandle handle = stream.closeAndGetHandle();

		assertThat(handle, instanceOf(FileStateHandle.class));
		assertThat(handle, not(instanceOf(RelativeFileStateHandle.class)));
		assertPathsEqual(exclusiveStateDir, ((FileStateHandle) handle).getFilePath().getParent());
	}

	// ------------------------------------------------------------------------
	//  test utils
	// ------------------------------------------------------------------------

	private static void assertPathsEqual(Path expected, Path actual) {
		final Path reNormalizedExpected = new Path(expected.toString());
		final Path reNormalizedActual = new Path(actual.toString());
		assertEquals(reNormalizedExpected, reNormalizedActual);
	}

	private FsCheckpointStreamFactory createFactory(FileSystem fs) {
		return new FsCheckpointStreamFactory(fs, exclusiveStateDir, sharedStateDir, 0, 4096);
	}
}
