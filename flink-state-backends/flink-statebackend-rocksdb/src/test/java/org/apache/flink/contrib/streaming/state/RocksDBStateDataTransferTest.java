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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RocksDbStateDataTransfer}.
 */
public class RocksDBStateDataTransferTest extends TestLogger {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Test that the exception arose in the thread pool will rethrow to the main thread.
	 */
	@Test
	public void testMultiThreadRestoreThreadPoolExceptionRethrow() {
		SpecifiedException expectedException = new SpecifiedException("throw exception while multi thread restore.");
		StreamStateHandle stateHandle = new StreamStateHandle() {
			@Override
			public FSDataInputStream openInputStream() throws IOException {
				throw expectedException;
			}

			@Override
			public void discardState() {

			}

			@Override
			public long getStateSize() {
				return 0;
			}
		};

		Map<StateHandleID, StreamStateHandle> stateHandles = new HashMap<>(1);
		stateHandles.put(new StateHandleID("state1"), stateHandle);

		IncrementalKeyedStateHandle incrementalKeyedStateHandle =
			new IncrementalKeyedStateHandle(
				UUID.randomUUID(),
				KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
				1,
				stateHandles,
				stateHandles,
				stateHandle);

		try {
			RocksDbStateDataTransfer.transferAllStateDataToDirectory(incrementalKeyedStateHandle, new Path(temporaryFolder.newFolder().toURI()), 5, new CloseableRegistry());
			fail();
		} catch (Exception e) {
			assertEquals(expectedException, e);
		}
	}

	/**
	 * Tests that download files with multi-thread correctly.
	 */
	@Test
	public void testMultiThreadRestoreCorrectly() throws Exception {
		Random random = new Random();
		int contentNum = 6;
		byte[][] contents = new byte[contentNum][];
		for (int i = 0; i < contentNum; ++i) {
			contents[i] = new byte[random.nextInt(100000) + 1];
			random.nextBytes(contents[i]);
		}

		List<StreamStateHandle> handles = new ArrayList<>(contentNum);
		for (int i = 0; i < contentNum; ++i) {
			handles.add(new ByteStreamStateHandle(String.format("state%d", i), contents[i]));
		}

		Map<StateHandleID, StreamStateHandle> sharedStates = new HashMap<>(contentNum);
		Map<StateHandleID, StreamStateHandle> privateStates = new HashMap<>(contentNum);
		for (int i = 0; i < contentNum; ++i) {
			sharedStates.put(new StateHandleID(String.format("sharedState%d", i)), handles.get(i));
			privateStates.put(new StateHandleID(String.format("privateState%d", i)), handles.get(i));
		}

		IncrementalKeyedStateHandle incrementalKeyedStateHandle =
			new IncrementalKeyedStateHandle(
				UUID.randomUUID(),
				KeyGroupRange.of(0, 1),
				1,
				sharedStates,
				privateStates,
				handles.get(0));

		Path dstPath = new Path(temporaryFolder.newFolder().toURI());
		RocksDbStateDataTransfer.transferAllStateDataToDirectory(incrementalKeyedStateHandle, dstPath, contentNum - 1, new CloseableRegistry());

		for (int i = 0; i < contentNum; ++i) {
			assertStateContentEqual(contents[i], new Path(dstPath, String.format("sharedState%d", i)));
		}
	}

	/**
	 * Test that the exception arose in the thread pool will rethrow to the main thread.
	 */
	@Test
	public void testMultiThreadUploadThreadPoolExceptionRethrow() throws IOException {
		SpecifiedException expectedException = new SpecifiedException("throw exception while multi thread upload states.");

		CheckpointStateOutputStream outputStream = createFailingCheckpointStateOutputStream(expectedException);
		CheckpointStreamFactory checkpointStreamFactory = (CheckpointedStateScope scope) -> outputStream;

		File file = temporaryFolder.newFile(String.valueOf(UUID.randomUUID()));
		generateRandomFileContent(file.getPath(), 20);

		Map<StateHandleID, Path> filePaths = new HashMap<>(1);
		filePaths.put(new StateHandleID("mockHandleID"), new Path(file.getPath()));
		try {
			RocksDbStateDataTransfer.uploadFilesToCheckpointFs(
				filePaths,
				5,
				checkpointStreamFactory,
				new CloseableRegistry());
			fail();
		} catch (Exception e) {
			assertEquals(expectedException, e);
		}
	}

	/**
	 * Test that upload files with multi-thread correctly.
	 */
	@Test
	public void testMultiThreadUploadCorrectly() throws Exception {

		File checkpointPrivateFolder = temporaryFolder.newFolder("private");
		Path checkpointPrivateDirectory = new Path(checkpointPrivateFolder.getPath());

		File checkpointSharedFolder = temporaryFolder.newFolder("shared");
		Path checkpointSharedDirectory = new Path(checkpointSharedFolder.getPath());

		FileSystem fileSystem = checkpointPrivateDirectory.getFileSystem();
		int fileStateSizeThreshold = 1024;
		FsCheckpointStreamFactory checkpointStreamFactory =
			new FsCheckpointStreamFactory(fileSystem, checkpointPrivateDirectory, checkpointSharedDirectory, fileStateSizeThreshold);

		String localFolder = "local";
		temporaryFolder.newFolder(localFolder);

		int sstFileCount = 6;
		Map<StateHandleID, Path> sstFilePaths = generateRandomSstFiles(localFolder, sstFileCount, fileStateSizeThreshold);

		Map<StateHandleID, StreamStateHandle> sstFiles = new HashMap<>(sstFileCount);

		sstFiles.putAll(RocksDbStateDataTransfer.uploadFilesToCheckpointFs(
			sstFilePaths,
			5,
			checkpointStreamFactory,
			new CloseableRegistry()));

		for (Map.Entry<StateHandleID, Path> entry : sstFilePaths.entrySet()) {
			assertStateContentEqual(entry.getValue(), sstFiles.get(entry.getKey()).openInputStream());
		}
	}

	private CheckpointStateOutputStream createFailingCheckpointStateOutputStream(
		IOException failureException) {
		return new CheckpointStateOutputStream() {
			@Nullable
			@Override
			public StreamStateHandle closeAndGetHandle() {
				return new ByteStreamStateHandle("testHandle", "testHandle".getBytes());
			}

			@Override
			public void close() {
			}

			@Override
			public long getPos() {
				return 0;
			}

			@Override
			public void flush() {
			}

			@Override
			public void sync() {
			}

			@Override
			public void write(int b) throws IOException {
				throw failureException;
			}
		};
	}

	private Map<StateHandleID, Path> generateRandomSstFiles(
		String localFolder,
		int sstFileCount,
		int fileStateSizeThreshold) throws IOException {
		ThreadLocalRandom random = ThreadLocalRandom.current();

		Map<StateHandleID, Path> sstFilePaths = new HashMap<>(sstFileCount);
		for (int i = 0; i < sstFileCount; ++i) {
			File file = temporaryFolder.newFile(String.format("%s/%d.sst", localFolder, i));
			generateRandomFileContent(file.getPath(), random.nextInt(1_000_000) + fileStateSizeThreshold);
			sstFilePaths.put(new StateHandleID(String.valueOf(i)), Path.fromLocalFile(file));
		}
		return sstFilePaths;
	}

	private void generateRandomFileContent(String filePath, int fileLength) throws IOException {
		FileOutputStream fileStream = new FileOutputStream(filePath);
		byte[] contents = new byte[fileLength];
		ThreadLocalRandom.current().nextBytes(contents);
		fileStream.write(contents);
		fileStream.close();
	}

	private void assertStateContentEqual(Path stateFilePath, FSDataInputStream inputStream) throws IOException {
		byte[] excepted = Files.readAllBytes(Paths.get(stateFilePath.toUri()));
		byte[] actual = new byte[excepted.length];
		IOUtils.readFully(inputStream, actual, 0, actual.length);
		assertEquals(-1, inputStream.read());
		assertArrayEquals(excepted, actual);
	}

	private void assertStateContentEqual(byte[] expected, Path path) throws IOException {
		byte[] actual = Files.readAllBytes(Paths.get(path.toUri()));
		assertArrayEquals(expected, actual);
	}

	private static class SpecifiedException extends IOException {
		SpecifiedException(String message) {
			super(message);
		}
	}
}
