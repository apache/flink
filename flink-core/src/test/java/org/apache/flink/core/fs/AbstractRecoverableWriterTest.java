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

package org.apache.flink.core.fs;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.fail;

/**
 * A base test-suite for the {@link RecoverableWriter}.
 * This should be subclassed to test each filesystem specific writer.
 */
public abstract class AbstractRecoverableWriterTest extends TestLogger {

	private static final Random RND = new Random();

	private static final String testData1 = "THIS IS A TEST 1.";
	private static final String testData2 = "THIS IS A TEST 2.";
	private static final String testData3 = "THIS IS A TEST 3.";

	private Path basePathForTest;

	private static FileSystem fileSystem;

	public abstract Path getBasePath() throws Exception;

	public abstract FileSystem initializeFileSystem() throws Exception;

	public Path getBasePathForTest() {
		return basePathForTest;
	}

	private FileSystem getFileSystem() throws Exception {
		if (fileSystem == null) {
			fileSystem = initializeFileSystem();
		}
		return fileSystem;
	}

	private RecoverableWriter getNewFileSystemWriter() throws Exception {
		return getFileSystem().createRecoverableWriter();
	}

	@Before
	public void prepare() throws Exception {
		basePathForTest = new Path(getBasePath(), randomName());
		getFileSystem().mkdirs(basePathForTest);
	}

	@After
	public void cleanup() throws Exception {
		getFileSystem().delete(basePathForTest, true);
	}

	@Test
	public void testCloseWithNoData() throws Exception {
		final RecoverableWriter writer = getNewFileSystemWriter();

		final Path testDir = getBasePathForTest();

		final Path path = new Path(testDir, "part-0");

		final RecoverableFsDataOutputStream stream = writer.open(path);
		for (Map.Entry<Path, String> fileContents : getFileContentByPath(testDir).entrySet()) {
			Assert.assertTrue(fileContents.getKey().getName().startsWith(".part-0.inprogress."));
			Assert.assertTrue(fileContents.getValue().isEmpty());
		}

		stream.closeForCommit().commit();

		for (Map.Entry<Path, String> fileContents : getFileContentByPath(testDir).entrySet()) {
			Assert.assertEquals("part-0", fileContents.getKey().getName());
			Assert.assertTrue(fileContents.getValue().isEmpty());
		}
	}

	@Test
	public void testCommitAfterNormalClose() throws Exception {
		final RecoverableWriter writer = getNewFileSystemWriter();

		final Path testDir = getBasePathForTest();

		final Path path = new Path(testDir, "part-0");

		RecoverableFsDataOutputStream stream = null;
		try {
			stream = writer.open(path);
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));
			stream.closeForCommit().commit();

			for (Map.Entry<Path, String> fileContents : getFileContentByPath(testDir).entrySet()) {
				Assert.assertEquals("part-0", fileContents.getKey().getName());
				Assert.assertEquals(testData1, fileContents.getValue());
			}
		} finally {
			IOUtils.closeQuietly(stream);
		}
	}

	@Test
	public void testCommitAfterPersist() throws Exception {
		final RecoverableWriter writer = getNewFileSystemWriter();

		final Path testDir = getBasePathForTest();

		final Path path = new Path(testDir, "part-0");

		RecoverableFsDataOutputStream stream = null;
		try {
			stream = writer.open(path);
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));
			stream.persist();

			stream.write(testData2.getBytes(StandardCharsets.UTF_8));
			stream.closeForCommit().commit();

			for (Map.Entry<Path, String> fileContents : getFileContentByPath(testDir).entrySet()) {
				Assert.assertEquals("part-0", fileContents.getKey().getName());
				Assert.assertEquals(testData1 + testData2, fileContents.getValue());
			}
		} finally {
			IOUtils.closeQuietly(stream);
		}
	}

	// TESTS FOR RECOVERY

	private static final String INIT_EMPTY_PERSIST = "EMPTY";
	private static final String INTERM_WITH_STATE_PERSIST = "INTERM-STATE";
	private static final String INTERM_WITH_NO_ADDITIONAL_STATE_PERSIST = "INTERM-IMEDIATE";
	private static final String FINAL_WITH_EXTRA_STATE = "FINAL";

	@Test
	public void testRecoverWithEmptyState() throws Exception {
		testResumeAfterMultiplePersist(
				INIT_EMPTY_PERSIST,
				"",
				testData3);
	}

	@Test
	public void testRecoverWithState() throws Exception {
		testResumeAfterMultiplePersist(
				INTERM_WITH_STATE_PERSIST,
				testData1,
				testData1 + testData3);
	}

	@Test
	public void testRecoverFromIntermWithoutAdditionalState() throws Exception {
		testResumeAfterMultiplePersist(
				INTERM_WITH_NO_ADDITIONAL_STATE_PERSIST,
				testData1,
				testData1 + testData3);
	}

	@Test
	public void testRecoverAfterMultiplePersistsState() throws Exception {
		testResumeAfterMultiplePersist(
				FINAL_WITH_EXTRA_STATE,
				testData1 + testData2,
				testData1 + testData2 + testData3);
	}

	private void testResumeAfterMultiplePersist(
			final String persistName,
			final String expectedPostRecoveryContents,
			final String expectedFinalContents) throws Exception {

		final Path testDir = getBasePathForTest();
		final Path path = new Path(testDir, "part-0");

		final RecoverableWriter initWriter = getNewFileSystemWriter();

		final Map<String, RecoverableWriter.ResumeRecoverable> recoverables = new HashMap<>(4);
		RecoverableFsDataOutputStream stream = null;
		try {
			stream = initWriter.open(path);
			recoverables.put(INIT_EMPTY_PERSIST, stream.persist());

			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			recoverables.put(INTERM_WITH_STATE_PERSIST, stream.persist());
			recoverables.put(INTERM_WITH_NO_ADDITIONAL_STATE_PERSIST, stream.persist());

			// and write some more data
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));

			recoverables.put(FINAL_WITH_EXTRA_STATE, stream.persist());
		} finally {
			IOUtils.closeQuietly(stream);
		}

		final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> serializer = initWriter.getResumeRecoverableSerializer();
		final byte[] serializedRecoverable = serializer.serialize(recoverables.get(persistName));

		// get a new serializer from a new writer to make sure that no pre-initialized state leaks in.
		final RecoverableWriter newWriter = getNewFileSystemWriter();
		final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> deserializer = newWriter.getResumeRecoverableSerializer();
		final RecoverableWriter.ResumeRecoverable recoveredRecoverable =
				deserializer.deserialize(serializer.getVersion(), serializedRecoverable);

		RecoverableFsDataOutputStream recoveredStream = null;
		try {
			recoveredStream = newWriter.recover(recoveredRecoverable);

			// we expect the data to be truncated
			Map<Path, String> files = getFileContentByPath(testDir);
			Assert.assertEquals(1L, files.size());

			for (Map.Entry<Path, String> fileContents : files.entrySet()) {
				Assert.assertTrue(fileContents.getKey().getName().startsWith(".part-0.inprogress."));
				Assert.assertEquals(expectedPostRecoveryContents, fileContents.getValue());
			}

			recoveredStream.write(testData3.getBytes(StandardCharsets.UTF_8));
			recoveredStream.closeForCommit().commit();

			files = getFileContentByPath(testDir);
			Assert.assertEquals(1L, files.size());

			for (Map.Entry<Path, String> fileContents : files.entrySet()) {
				Assert.assertEquals("part-0", fileContents.getKey().getName());
				Assert.assertEquals(expectedFinalContents, fileContents.getValue());
			}
		} finally {
			IOUtils.closeQuietly(recoveredStream);
		}
	}

	@Test
	public void testCommitAfterRecovery() throws Exception {
		final Path testDir = getBasePathForTest();
		final Path path = new Path(testDir, "part-0");

		final RecoverableWriter initWriter = getNewFileSystemWriter();

		final RecoverableWriter.CommitRecoverable recoverable;
		RecoverableFsDataOutputStream stream = null;
		try {
			stream = initWriter.open(path);
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			stream.persist();
			stream.persist();

			// and write some more data
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));

			recoverable = stream.closeForCommit().getRecoverable();
		} finally {
			IOUtils.closeQuietly(stream);
		}

		final byte[] serializedRecoverable = initWriter.getCommitRecoverableSerializer().serialize(recoverable);

		// get a new serializer from a new writer to make sure that no pre-initialized state leaks in.
		final RecoverableWriter newWriter = getNewFileSystemWriter();
		final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> deserializer = newWriter.getCommitRecoverableSerializer();
		final RecoverableWriter.CommitRecoverable recoveredRecoverable = deserializer.deserialize(deserializer.getVersion(), serializedRecoverable);

		final RecoverableFsDataOutputStream.Committer committer = newWriter.recoverForCommit(recoveredRecoverable);
		committer.commitAfterRecovery();

		Map<Path, String> files = getFileContentByPath(testDir);
		Assert.assertEquals(1L, files.size());

		for (Map.Entry<Path, String> fileContents : files.entrySet()) {
			Assert.assertEquals("part-0", fileContents.getKey().getName());
			Assert.assertEquals(testData1 + testData2, fileContents.getValue());
		}
	}

	// TESTS FOR EXCEPTIONS

	@Test(expected = IOException.class)
	public void testExceptionWritingAfterCloseForCommit() throws Exception {
		final Path testDir = getBasePathForTest();

		final RecoverableWriter writer = getNewFileSystemWriter();
		final Path path = new Path(testDir, "part-0");

		RecoverableFsDataOutputStream stream = null;
		try {
			stream = writer.open(path);
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			stream.closeForCommit().getRecoverable();
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));
			fail();
		} finally {
			IOUtils.closeQuietly(stream);
		}
	}

	@Test(expected = IOException.class)
	public void testResumeAfterCommit() throws Exception {
		final Path testDir = getBasePathForTest();

		final RecoverableWriter writer = getNewFileSystemWriter();
		final Path path = new Path(testDir, "part-0");

		RecoverableWriter.ResumeRecoverable recoverable;
		RecoverableFsDataOutputStream stream = null;
		try {
			stream = writer.open(path);
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			recoverable = stream.persist();
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));

			stream.closeForCommit().commit();
		} finally {
			IOUtils.closeQuietly(stream);
		}

		// this should throw an exception as the file is already committed
		writer.recover(recoverable);
		fail();
	}

	@Test
	public void testResumeWithWrongOffset() throws Exception {
		// this is a rather unrealistic scenario, but it is to trigger
		// truncation of the file and try to resume with missing data.

		final Path testDir = getBasePathForTest();

		final RecoverableWriter writer = getNewFileSystemWriter();
		final Path path = new Path(testDir, "part-0");

		final RecoverableWriter.ResumeRecoverable recoverable1;
		final RecoverableWriter.ResumeRecoverable recoverable2;
		RecoverableFsDataOutputStream stream = null;
		try {
			stream = writer.open(path);
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			recoverable1 = stream.persist();
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));

			recoverable2 = stream.persist();
			stream.write(testData3.getBytes(StandardCharsets.UTF_8));
		} finally {
			IOUtils.closeQuietly(stream);
		}

		try (RecoverableFsDataOutputStream ignored = writer.recover(recoverable1)) {
			// this should work fine
		} catch (Exception e) {
			fail();
		}

		// this should throw an exception
		try (RecoverableFsDataOutputStream ignored = writer.recover(recoverable2)) {
			fail();
		} catch (IOException e) {
			// we expect this
			return;
		}
		fail();
	}

	private Map<Path, String> getFileContentByPath(Path directory) throws Exception {
		Map<Path, String> contents = new HashMap<>();

		final FileStatus[] filesInBucket = getFileSystem().listStatus(directory);
		for (FileStatus file : filesInBucket) {
			final long fileLength = file.getLen();
			byte[] serContents = new byte[(int) fileLength];

			FSDataInputStream stream = getFileSystem().open(file.getPath());
			stream.read(serContents);

			contents.put(file.getPath(), new String(serContents, StandardCharsets.UTF_8));
		}
		return contents;
	}

	private static String randomName() {
		return StringUtils.getRandomString(RND, 16, 16, 'a', 'z');
	}
}
