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

import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link AbstractFileStateBackend}.
 */
public class AbstractFileStateBackendGlobalHooksTest {

	private final Random rnd = new Random();

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------
	//  Savepoint Dispose
	// ------------------------------------------------------------------------

	/**
	 * Tests that a savepoint directory can be disposed by 
	 */
	@Test
	public void testSavpointDisposeByFilePointer() throws Exception {
		final FsStateBackend backend = new FsStateBackend(tmp.newFolder().toURI());

		final File metadataFile = createCheckpointOrSavepoint(tmp.newFolder());
		assertTrue(metadataFile.exists());
		assertTrue(metadataFile.isFile());

		backend.disposeSavepoint(metadataFile.toURI().toString());

		assertFalse(metadataFile.exists());
		assertFalse(metadataFile.getParentFile().exists());
	}

	@Test
	public void testSavpointDisposeByDirectoryPointer() throws Exception {
		final FsStateBackend backend = new FsStateBackend(tmp.newFolder().toURI());

		final File metadataFile = createCheckpointOrSavepoint(tmp.newFolder());
		final File savepointDirectory = metadataFile.getParentFile();

		assertTrue(savepointDirectory.exists());
		assertTrue(savepointDirectory.isDirectory());

		backend.disposeSavepoint(savepointDirectory.toURI().toString());

		assertFalse(savepointDirectory.exists());
	}

	/**
	 * Tests that a directory that appears not to be a savepoint directory (missing metadata file)
	 * is not disposed by this hook.
	 */
	@Test
	public void testDoNotDisposeNonSavpointDir() throws Exception {
		final FsStateBackend backend = new FsStateBackend(tmp.newFolder().toURI());

		// write some random files
		final File savepointDir = tmp.newFolder();
		writeRandomFiles(savepointDir, rnd, 5, 100);
		
		try {
			backend.disposeSavepoint(savepointDir.toURI().toString());
			fail("this should fail with an exception");
		}
		catch (IOException e) {
			// expected
		}
	}
	

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private File createCheckpointOrSavepoint(File directory) throws Exception {
		final File metadataFile = new File(directory, AbstractFileStateBackend.METADATA_FILE_NAME);

		// write the metadata
		try (FileOutputStream out = new FileOutputStream(metadataFile);
			DataOutputStream dos = new DataOutputStream(out))
		{
			final SavepointV1 savepoint = new SavepointV1(rnd.nextInt(1000000), Collections.<TaskState>emptyList());
			Checkpoints.storeCheckpointMetadata(savepoint, dos);
		}

		// write some random files
		writeRandomFiles(directory, rnd, 5, 100);

		return metadataFile;
	}

	private static void writeRandomFiles(File directory, Random rnd, int num, int maxSize) throws IOException {
		//noinspection ResultOfMethodCallIgnored
		directory.mkdirs();

		// write some random files
		for (int i = num; i > 0; i--) {
			File file = new File(directory, UUID.randomUUID().toString());

			try (FileOutputStream out = new FileOutputStream(file)) {
				for (int k = rnd.nextInt(maxSize) + 1; k > 0; k--) {
					out.write(rnd.nextInt());
				}
			}
		}
	}
}
