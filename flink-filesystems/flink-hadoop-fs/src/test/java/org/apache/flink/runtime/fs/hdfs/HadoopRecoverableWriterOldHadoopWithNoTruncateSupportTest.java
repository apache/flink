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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link HadoopRecoverableWriter} with Hadoop versions pre Hadoop 2.7.
 * Contains tests that show that the user can use the writer with pre-2.7 versions as
 * long as he/she does not use the {@code truncate()} functionality of the underlying FS.
 */
public class HadoopRecoverableWriterOldHadoopWithNoTruncateSupportTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;

	/** The cached file system instance. */
	private static FileSystem fileSystem;

	private static Path basePath;

	@BeforeClass
	public static void testHadoopVersion() {
		Assume.assumeTrue(HadoopUtils.isMaxHadoopVersion(2, 7));
	}

	@BeforeClass
	public static void verifyOS() {
		Assume.assumeTrue("HDFS cluster cannot be started on Windows without extensions.", !OperatingSystem.isWindows());
	}

	@BeforeClass
	public static void createHDFS() throws Exception {
		final File baseDir = TEMP_FOLDER.newFolder();

		final Configuration hdConf = new Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

		final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		hdfsCluster = builder.build();

		final org.apache.hadoop.fs.FileSystem hdfs = hdfsCluster.getFileSystem();

		fileSystem = new HadoopFileSystem(hdfs);
		basePath = new Path(hdfs.getUri() + "/tests");
	}

	@AfterClass
	public static void destroyHDFS() throws Exception {
		if (hdfsCluster != null) {
			hdfsCluster.getFileSystem().delete(new org.apache.hadoop.fs.Path(basePath.toUri()), true);
			hdfsCluster.shutdown();
		}
	}

	@Test
	public void testWriteAndCommitWorks() throws IOException {
		final Path testPath = new Path(basePath, "test-0");
		final String expectedContent = "test_line";

		final RecoverableWriter writerUnderTest = fileSystem.createRecoverableWriter();
		final RecoverableFsDataOutputStream streamUnderTest =
				getOpenStreamToFileWithContent(writerUnderTest, testPath, expectedContent);
		streamUnderTest.closeForCommit().commit();

		verifyFileContent(testPath, expectedContent);
	}

	@Test
	public void testRecoveryAfterClosingForCommitWorks() throws IOException {
		final Path testPath = new Path(basePath, "test-1");
		final String expectedContent = "test_line";

		final RecoverableWriter writerUnderTest = fileSystem.createRecoverableWriter();
		final RecoverableFsDataOutputStream streamUnderTest =
				getOpenStreamToFileWithContent(writerUnderTest, testPath, expectedContent);

		final RecoverableWriter.CommitRecoverable committable =
				streamUnderTest.closeForCommit().getRecoverable();

		writerUnderTest.recoverForCommit(committable).commitAfterRecovery();

		verifyFileContent(testPath, expectedContent);
	}

	@Test
	public void testExceptionThrownWhenRecoveringWithInProgressFile() throws IOException {
		final RecoverableWriter writerUnderTest = fileSystem.createRecoverableWriter();
		final RecoverableFsDataOutputStream stream = writerUnderTest.open(new Path(basePath, "test-2"));
		final RecoverableWriter.ResumeRecoverable recoverable = stream.persist();
		assertNotNull(recoverable);

		try {
			writerUnderTest.recover(recoverable);
		} catch (IOException e) {
			// this is the expected exception and we check also if the root cause is the hadoop < 2.7 version
			assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}

	@Test
	public void testRecoverableWriterWithViewfsScheme() {
		final org.apache.hadoop.fs.FileSystem mockViewfs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
		when(mockViewfs.getScheme()).thenReturn("viewfs");
		// Creating the writer should not throw UnsupportedOperationException.
		RecoverableWriter recoverableWriter = new HadoopRecoverableWriter(mockViewfs);
	}

	private RecoverableFsDataOutputStream getOpenStreamToFileWithContent(
			final RecoverableWriter writerUnderTest,
			final Path path,
			final String expectedContent) throws IOException {

		final byte[] content = expectedContent.getBytes(UTF_8);

		final RecoverableFsDataOutputStream streamUnderTest = writerUnderTest.open(path);
		streamUnderTest.write(content);
		return streamUnderTest;
	}

	private static void verifyFileContent(final Path testPath, final String expectedContent) throws IOException {
		try (FSDataInputStream in = fileSystem.open(testPath);
				InputStreamReader ir = new InputStreamReader(in, UTF_8);
				BufferedReader reader = new BufferedReader(ir)) {

			final String line = reader.readLine();
			assertEquals(expectedContent, line);
		}
	}
}
