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
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.fail;

/**
 * A base test-suite for the {@link ResumableWriter}.
 * This should be subclassed to test each filesystem specific writer.
 */
public abstract class AbstractResumableWriterTest extends TestLogger {

	private static final Random RND = new Random();

	private static final String testData1 = "THIS IS A TEST 1.";
	private static final String testData2 = "THIS IS A TEST 2.";
	private static final String testData3 = "THIS IS A TEST 3.";

	private Path basePathForTest;

	private static FileSystem fileSystem;

	public abstract Path getBasePath() throws Exception;

	public abstract FileSystem initializeFileSystem();

	public Path getBasePathForTest() {
		return basePathForTest;
	}

	private FileSystem getFileSystem() {
		if (fileSystem == null) {
			fileSystem = initializeFileSystem();
		}
		return fileSystem;
	}

	private ResumableWriter getNewFileSystemWriter() throws IOException {
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
		final ResumableWriter writer = getNewFileSystemWriter();

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
		final ResumableWriter writer = getNewFileSystemWriter();

		final Path testDir = getBasePathForTest();

		final Path path = new Path(testDir, "part-0");

		try (final RecoverableFsDataOutputStream stream = writer.open(path)) {
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));
			stream.closeForCommit().commit();

			for (Map.Entry<Path, String> fileContents : getFileContentByPath(testDir).entrySet()) {
				Assert.assertEquals("part-0", fileContents.getKey().getName());
				Assert.assertEquals(testData1, fileContents.getValue());
			}
		}
	}

	@Test(expected = IOException.class)
	public void testExceptionWritingAfterCloseForCommit() throws Exception {
		final Path testDir = getBasePathForTest();

		final ResumableWriter writer = getNewFileSystemWriter();
		final Path path = new Path(testDir, "part-0");

		try (final RecoverableFsDataOutputStream stream = writer.open(path)) {
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			stream.closeForCommit().getRecoverable();
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));
			fail();
		}
	}

	// TESTS FOR RECOVERY

	@Test
	public void testResumeAfterPersist() throws Exception {
		final Path testDir = getBasePathForTest();

		final ResumableWriter writer = getNewFileSystemWriter();
		final Path path = new Path(testDir, "part-0");

		ResumableWriter.ResumeRecoverable recoverable;
		try (final RecoverableFsDataOutputStream stream = writer.open(path)) {
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			// get the valid offset
			recoverable = stream.persist();

			// and write some more data
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));

			// todo if the check for the file contents were here,
			// in hadoop it would fail because close() has not been called.
		}

		Map<Path, String> files = getFileContentByPath(testDir);
		Assert.assertEquals(1L, files.size());

		for (Map.Entry<Path, String> fileContents : files.entrySet()) {
			Assert.assertTrue(fileContents.getKey().getName().startsWith(".part-0.inprogress."));
			Assert.assertEquals(testData1 + testData2, fileContents.getValue());
		}

		SimpleVersionedSerializer<ResumableWriter.ResumeRecoverable> serializer = writer.getResumeRecoverableSerializer();
		byte[] serializedRecoverable = serializer.serialize(recoverable);

		// get a new serializer from a new writer to make sure that no pre-initialized state leaks in.
		final ResumableWriter newWriter = getNewFileSystemWriter();
		final ResumableWriter.ResumeRecoverable recoveredRecoverable =
				newWriter.getResumeRecoverableSerializer()
						.deserialize(serializer.getVersion(), serializedRecoverable);

		try (final RecoverableFsDataOutputStream recoveredStream = newWriter.recover(recoveredRecoverable)) {

			// we expect the data to be truncated
			files = getFileContentByPath(testDir);
			Assert.assertEquals(1L, files.size());

			for (Map.Entry<Path, String> fileContents : files.entrySet()) {
				Assert.assertTrue(fileContents.getKey().getName().startsWith(".part-0.inprogress."));
				Assert.assertEquals(testData1, fileContents.getValue());
			}

			recoveredStream.write(testData3.getBytes(StandardCharsets.UTF_8));
			recoveredStream.closeForCommit().commit();

			files = getFileContentByPath(testDir);
			Assert.assertEquals(1L, files.size());

			for (Map.Entry<Path, String> fileContents : files.entrySet()) {
				Assert.assertEquals("part-0", fileContents.getKey().getName());
				Assert.assertEquals(testData1 + testData3, fileContents.getValue());
			}
		}
	}

	@Test
	public void testResumeCommitRecoverable() throws Exception {
		final Path testDir = getBasePathForTest();

		final ResumableWriter writer = getNewFileSystemWriter();
		final Path path = new Path(testDir, "part-0");

		final ResumableWriter.CommitRecoverable recoverable;
		try (final RecoverableFsDataOutputStream stream = writer.open(path)) {
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			recoverable = stream.closeForCommit().getRecoverable();
		}

		final SimpleVersionedSerializer<ResumableWriter.CommitRecoverable> serializer = writer.getCommitRecoverableSerializer();
		byte[] serializedCommitable = serializer.serialize(recoverable);

		// get a new serializer from a new writer to make sure that no pre-initialized state leaks in.
		final ResumableWriter newWriter = getNewFileSystemWriter();
		final ResumableWriter.CommitRecoverable recoveredCommitable =
				newWriter.getResumeRecoverableSerializer()
						.deserialize(serializer.getVersion(), serializedCommitable);

		newWriter.recoverForCommit(recoveredCommitable).commit();

		Map<Path, String> files = getFileContentByPath(testDir);
		Assert.assertEquals(1L, files.size());

		for (Map.Entry<Path, String> fileContents : files.entrySet()) {
			Assert.assertEquals("part-0", fileContents.getKey().getName());
			Assert.assertEquals(testData1, fileContents.getValue());
		}
	}

	@Test(expected = FileNotFoundException.class)
	public void testResumeAfterCommit() throws Exception {
		final Path testDir = getBasePathForTest();

		final ResumableWriter writer = getNewFileSystemWriter();
		final Path path = new Path(testDir, "part-0");

		ResumableWriter.ResumeRecoverable recoverable;
		try (final RecoverableFsDataOutputStream stream = writer.open(path)) {
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			recoverable = stream.persist();
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));

			// TODO: 7/10/18 again hadoop do
//			Map<Path, String> files = getFileContentByPath(testDir);
//			Assert.assertEquals(1L, files.size());
//
//			for (Map.Entry<Path, String> fileContents : files.entrySet()) {
//				Assert.assertTrue(fileContents.getKey().getName().startsWith(".part-0.inprogress."));
//				Assert.assertEquals(testData1 + testData2, fileContents.getValue());
//			}

			stream.closeForCommit().commit();
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

		final ResumableWriter writer = getNewFileSystemWriter();
		final Path path = new Path(testDir, "part-0");

		final ResumableWriter.ResumeRecoverable recoverable1;
		final ResumableWriter.ResumeRecoverable recoverable2;
		try (final RecoverableFsDataOutputStream stream = writer.open(path)) {
			stream.write(testData1.getBytes(StandardCharsets.UTF_8));

			recoverable1 = stream.persist();
			stream.write(testData2.getBytes(StandardCharsets.UTF_8));

			recoverable2 = stream.persist();
			stream.write(testData3.getBytes(StandardCharsets.UTF_8));

			// TODO: 7/10/18 again hadoop would fail without close().
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
			Assert.assertTrue(e.getMessage().startsWith("Missing data in tmp file:"));
		}
	}

	private Map<Path, String> getFileContentByPath(Path directory) throws IOException {
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
