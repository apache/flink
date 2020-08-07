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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.s3.common.utils.RefCountedBufferingFileStream;
import org.apache.flink.fs.s3.common.utils.RefCountedFSOutputStream;
import org.apache.flink.fs.s3.common.utils.RefCountedFileWithStream;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link S3RecoverableFsDataOutputStream}.
 */
public class S3RecoverableFsDataOutputStreamTest {

	private static final long USER_DEFINED_MIN_PART_SIZE = 10L;

	private TestMultipartUpload multipartUploadUnderTest;

	private TestFileProvider fileProvider;

	private S3RecoverableFsDataOutputStream streamUnderTest;

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Before
	public void beforeTest() throws IOException {
		fileProvider = new TestFileProvider(TEMP_FOLDER);

		multipartUploadUnderTest = new TestMultipartUpload(fileProvider);

		RefCountedBufferingFileStream fileStream = RefCountedBufferingFileStream.openNew(fileProvider);

		streamUnderTest = new S3RecoverableFsDataOutputStream(
				multipartUploadUnderTest,
				fileProvider,
				fileStream,
				USER_DEFINED_MIN_PART_SIZE,
				0L
		);
	}

	@Test
	public void simpleUsage() throws IOException {
		streamUnderTest.write(bytesOf("hello world"));

		RecoverableFsDataOutputStream.Committer committer = streamUnderTest.closeForCommit();
		committer.commit();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("hello world")));
	}

	@Test
	public void noWritesShouldResolveInAnEmptyFile() throws IOException {
		RecoverableFsDataOutputStream.Committer committer = streamUnderTest.closeForCommit();
		committer.commit();

		assertThat(multipartUploadUnderTest, hasContent(new byte[0]));
	}

	@Test
	public void closingWithoutCommittingDiscardsTheData() throws IOException {
		streamUnderTest.write(bytesOf("hello world"));

		streamUnderTest.close();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("")));
	}

	@Test
	public void twoWritesAreConcatenated() throws IOException {
		streamUnderTest.write(bytesOf("hello"));
		streamUnderTest.write(bytesOf(" "));
		streamUnderTest.write(bytesOf("world"));

		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("hello world")));
	}

	@Test
	public void writeLargeFile() throws IOException {
		List<byte[]> testDataBuffers = createRandomLargeTestDataBuffers();

		for (byte[] buffer : testDataBuffers) {
			streamUnderTest.write(buffer);
		}
		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(testDataBuffers));
	}

	@Test
	public void simpleRecovery() throws IOException {
		streamUnderTest.write(bytesOf("hello"));

		streamUnderTest.persist();

		streamUnderTest = reopenStreamUnderTestAfterRecovery();
		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("hello")));
	}

	@Test
	public void multiplePersistsDoesNotIntroduceJunk() throws IOException {
		streamUnderTest.write(bytesOf("hello"));

		streamUnderTest.persist();
		streamUnderTest.persist();
		streamUnderTest.persist();
		streamUnderTest.persist();

		streamUnderTest.write(bytesOf(" "));
		streamUnderTest.write(bytesOf("world"));

		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("hello world")));
	}

	@Test
	public void multipleWritesAndPersists() throws IOException {
		streamUnderTest.write(bytesOf("a"));

		streamUnderTest.persist();
		streamUnderTest.write(bytesOf("b"));

		streamUnderTest.persist();
		streamUnderTest.write(bytesOf("c"));

		streamUnderTest.persist();
		streamUnderTest.write(bytesOf("d"));

		streamUnderTest.persist();
		streamUnderTest.write(bytesOf("e"));

		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("abcde")));
	}

	@Test
	public void multipleWritesAndPersistsWithBigChunks() throws IOException {
		List<byte[]> testDataBuffers = createRandomLargeTestDataBuffers();

		for (byte[] buffer : testDataBuffers) {
			streamUnderTest.write(buffer);
			streamUnderTest.persist();
		}
		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(testDataBuffers));
	}

	@Test
	public void addDataAfterRecovery() throws IOException {
		streamUnderTest.write(bytesOf("hello"));

		streamUnderTest.persist();

		streamUnderTest = reopenStreamUnderTestAfterRecovery();
		streamUnderTest.write(bytesOf(" "));
		streamUnderTest.write(bytesOf("world"));
		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("hello world")));
	}

	@Test
	public void discardingUnpersistedNotYetUploadedData() throws IOException {
		streamUnderTest.write(bytesOf("hello"));

		streamUnderTest.persist();

		streamUnderTest.write(bytesOf("goodbye"));
		streamUnderTest = reopenStreamUnderTestAfterRecovery();

		streamUnderTest.write(bytesOf(" world"));
		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("hello world")));
	}

	@Test
	public void discardingUnpersistedUploadedData() throws IOException {
		streamUnderTest.write(bytesOf("hello"));

		streamUnderTest.persist();
		streamUnderTest.write(randomBuffer(RefCountedBufferingFileStream.BUFFER_SIZE + 1));
		streamUnderTest = reopenStreamUnderTestAfterRecovery();

		streamUnderTest.write(bytesOf(" world"));
		streamUnderTest.closeForCommit().commit();

		assertThat(multipartUploadUnderTest, hasContent(bytesOf("hello world")));
	}

	@Test
	public void commitEmptyStreamShouldBeSuccessful() throws IOException {
		streamUnderTest.closeForCommit().commit();
	}

	@Test(expected = IOException.class)
	public void closeForCommitOnClosedStreamShouldFail() throws IOException {
		streamUnderTest.closeForCommit().commit();
		streamUnderTest.closeForCommit().commit();
	}



	// ------------------------------------------------------------------------------------------------------------
	// Utils
	// ------------------------------------------------------------------------------------------------------------

	private S3RecoverableFsDataOutputStream reopenStreamUnderTestAfterRecovery() throws IOException {
		final long bytesBeforeCurrentPart = multipartUploadUnderTest.numBytes;
		final Optional<File> incompletePart = multipartUploadUnderTest.getIncompletePart();

		RefCountedBufferingFileStream fileStream = RefCountedBufferingFileStream.restore(fileProvider, incompletePart.get());
		multipartUploadUnderTest.discardUnpersistedData();

		return new S3RecoverableFsDataOutputStream(
				multipartUploadUnderTest,
				fileProvider,
				fileStream,
				USER_DEFINED_MIN_PART_SIZE,
				bytesBeforeCurrentPart
		);
	}

	private static List<byte[]> createRandomLargeTestDataBuffers() {
		final List<byte[]> testData = new ArrayList<>();
		final SplittableRandom random = new SplittableRandom();

		long totalSize = 0L;

		int expectedSize = (int) random.nextLong(USER_DEFINED_MIN_PART_SIZE * 5L, USER_DEFINED_MIN_PART_SIZE * 100L);
		while (totalSize < expectedSize) {

			int len = random.nextInt(0, (int) (2L * USER_DEFINED_MIN_PART_SIZE));
			byte[] buffer = randomBuffer(random, len);
			totalSize += buffer.length;
			testData.add(buffer);
		}
		return testData;
	}

	private static byte[] randomBuffer(int len) {
		final SplittableRandom random = new SplittableRandom();
		return randomBuffer(random, len);
	}

	private static byte[] randomBuffer(SplittableRandom random, int len) {
		byte[] buffer = new byte[len];
		for (int i = 0; i < buffer.length; i++) {
			buffer[i] = (byte) (random.nextInt() & 0xFF);
		}
		return buffer;
	}

	private static byte[] bytesOf(String str) {
		return str.getBytes(StandardCharsets.UTF_8);
	}

	private static byte[] readFileContents(RefCountedFSOutputStream file) throws IOException {
		final byte[] content = new byte[MathUtils.checkedDownCast(file.getPos())];
		File inputFile = file.getInputFile();
		long bytesRead = new FileInputStream(inputFile).read(content, 0,  MathUtils.checkedDownCast(inputFile.length()));
		Assert.assertEquals(file.getPos(), bytesRead);
		return content;
	}

	// ------------------------------------------------------------------------------------------------------------
	// Matchers
	// ------------------------------------------------------------------------------------------------------------

	private static TypeSafeMatcher<TestMultipartUpload> hasContent(final byte[] expectedContent) {
		return new TypeSafeMatcher<TestMultipartUpload>() {
			@Override
			protected boolean matchesSafely(TestMultipartUpload testMultipartUpload) {
				return Arrays.equals(
						testMultipartUpload.getPublishedContents(),
						expectedContent
				);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a TestMultipartUpload with contents='")
						.appendValue(expectedContent)
						.appendText("'");
			}
		};
	}

	private static TypeSafeMatcher<TestMultipartUpload> hasContent(final Collection<byte[]> expectedContents) throws IOException {

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		for (byte[] c : expectedContents) {
			stream.write(c);
		}

		byte[] expectedContent = stream.toByteArray();

		return new TypeSafeMatcher<TestMultipartUpload>() {
			@Override
			protected boolean matchesSafely(TestMultipartUpload testMultipartUpload) {
				return Arrays.equals(
						testMultipartUpload.getPublishedContents(),
						expectedContent
				);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a TestMultipartUpload with contents='")
						.appendValue(expectedContent)
						.appendText("'");
			}
		};
	}

	// ------------------------------------------------------------------------------------------------------------
	// Test Classes
	// ------------------------------------------------------------------------------------------------------------

	private static class TestMultipartUpload implements RecoverableMultiPartUpload {

		private final TestFileProvider fileProvider;

		private List<byte[]> uploadedContent = new ArrayList<>();

		private int lastPersistedIndex;

		private int numParts;

		private long numBytes;

		private byte[] published;

		private final ByteArrayOutputStream publishedContents = new ByteArrayOutputStream();

		private Optional<byte[]> uncompleted = Optional.empty();

		TestMultipartUpload(TestFileProvider fileProvider) {
			this.published = new byte[0];
			this.lastPersistedIndex = 0;
			this.fileProvider = fileProvider;
		}

		public void discardUnpersistedData() {
			uploadedContent = uploadedContent.subList(0, lastPersistedIndex);
		}

		@Override
		public Optional<File> getIncompletePart() {
			if (!uncompleted.isPresent()) {
				return Optional.empty();
			}
			byte[] uncompletedBytes = uncompleted.get();
			try {
				File uncompletedTempFile = fileProvider.apply(null).getFile();
				Files.write(uncompletedTempFile.toPath(), uncompletedBytes);
				return Optional.of(uncompletedTempFile);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public RecoverableFsDataOutputStream.Committer snapshotAndGetCommitter() throws IOException {
			lastPersistedIndex = uploadedContent.size();

			return new RecoverableFsDataOutputStream.Committer() {
				@Override
				public void commit() throws IOException {
					published = getPublishedContents();
					uploadedContent.clear();
					lastPersistedIndex = 0;
				}

				@Override
				public void commitAfterRecovery() throws IOException {
					if (published.length == 0) {
						commit();
					}
				}

				@Override
				public RecoverableWriter.CommitRecoverable getRecoverable() {
					return null;
				}
			};
		}

		@Override
		public RecoverableWriter.ResumeRecoverable snapshotAndGetRecoverable(RefCountedFSOutputStream incompletePartFile) throws IOException {
			lastPersistedIndex = uploadedContent.size();

			if (incompletePartFile.getPos() >= 0L) {
				byte[] bytes = readFileContents(incompletePartFile);
				uncompleted = Optional.of(bytes);
			}

			return null;
		}

		@Override
		public void uploadPart(RefCountedFSOutputStream file) throws IOException {
			numParts++;
			numBytes += file.getPos();

			uploadedContent.add(readFileContents(file));
		}

		public byte[] getPublishedContents() {
			for (int i = 0; i < lastPersistedIndex; i++) {
				try {
					publishedContents.write(uploadedContent.get(i));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			return publishedContents.toByteArray();
		}

		@Override
		public String toString() {
			return "TestMultipartUpload{" +
					"contents="
					+ Arrays.toString(published)
					+ '}';
		}
	}

	private static class TestFileProvider implements FunctionWithException<File, RefCountedFileWithStream, IOException> {

		private final TemporaryFolder folder;

		TestFileProvider(TemporaryFolder folder) {
			this.folder = Preconditions.checkNotNull(folder);
		}

		@Override
		public RefCountedFileWithStream apply(@Nullable File file) throws IOException {
			while (true) {
				try {
					if (file == null) {
						final File newFile = new File(folder.getRoot(), ".tmp_" + UUID.randomUUID());
						final OutputStream out = Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);
						return RefCountedFileWithStream.newFile(newFile, out);
					} else {
						final OutputStream out = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND);
						return RefCountedFileWithStream.restoredFile(file, out, file.length());
					}
				} catch (FileAlreadyExistsException e) {
					// fall through the loop and retry
				}
			}
		}
	}
}
