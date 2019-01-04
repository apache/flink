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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.s3.S3TestCredentials;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.apache.flink.core.fs.FileSystemTestUtils.checkPathEventualExistence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the S3 file system support via Hadoop's {@link org.apache.hadoop.fs.s3a.S3AFileSystem}.
 *
 * <p><strong>BEWARE</strong>: tests must take special care of S3's
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel">consistency guarantees</a>
 * and what the {@link org.apache.hadoop.fs.s3a.S3AFileSystem} offers.
 */
public class HadoopS3FileSystemITCase extends TestLogger {

	/**
	 * Will be updated by {@link #checkCredentialsAndSetup()} if the test is not skipped.
	 */
	private static FileSystem fileSystem;

	private static Path basePath;

	@BeforeClass
	public static void checkCredentialsAndSetup() throws IOException {
		// check whether credentials exist
		S3TestCredentials.assumeCredentialsAvailable();

		// initialize configuration with valid credentials
		final Configuration conf = new Configuration();
		conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
		conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());
		FileSystem.initialize(conf);

		basePath = new Path(S3TestCredentials.getTestBucketUri() + "tests-" + UUID.randomUUID());
		fileSystem = basePath.getFileSystem();

		// check for uniqueness of the test directory
		// directory must not yet exist
		assertFalse(fileSystem.exists(basePath));
	}

	@AfterClass
	public static void cleanUp() throws IOException, InterruptedException {
		try {
			if (fileSystem != null) {
				final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs

				// clean up
				fileSystem.delete(basePath, true);

				// now directory must be gone
				checkPathEventualExistence(fileSystem, basePath, false, deadline);
			}
		}
		finally {
			FileSystem.initialize(new Configuration());
		}
	}

	@Test
	public void testSimpleFileWriteAndRead() throws Exception {
		final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs
		final String testLine = "Hello Upload!";

		final Path path = new Path(basePath, "test.txt");
		final FileSystem fs = path.getFileSystem();

		try {
			try (FSDataOutputStream out = fs.create(path, WriteMode.OVERWRITE);
					OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
				writer.write(testLine);
			}

			// just in case, wait for the path to exist
			checkPathEventualExistence(fs, path, true, deadline);

			try (FSDataInputStream in = fs.open(path);
					InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8);
					BufferedReader reader = new BufferedReader(ir)) {
				String line = reader.readLine();
				assertEquals(testLine, line);
			}
		}
		finally {
			fs.delete(path, false);
		}

		// now file must be gone (this is eventually-consistent!)
		checkPathEventualExistence(fs, path, false, deadline);
	}

	@Test
	public void testDirectoryListing() throws Exception {
		final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs

		final Path directory = new Path(basePath, "testdir/");
		final FileSystem fs = directory.getFileSystem();

		// directory must not yet exist
		assertFalse(fs.exists(directory));

		try {
			// create directory
			assertTrue(fs.mkdirs(directory));

			checkPathEventualExistence(fs, directory, true, deadline);

			// directory empty
			assertEquals(0, fs.listStatus(directory).length);

			// create some files
			final int numFiles = 3;
			for (int i = 0; i < numFiles; i++) {
				Path file = new Path(directory, "/file-" + i);
				try (FSDataOutputStream out = fs.create(file, WriteMode.OVERWRITE);
						OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
					writer.write("hello-" + i + "\n");
				}
				// just in case, wait for the file to exist (should then also be reflected in the
				// directory's file list below)
				checkPathEventualExistence(fs, file, true, deadline);
			}

			FileStatus[] files = fs.listStatus(directory);
			assertNotNull(files);
			assertEquals(3, files.length);

			for (FileStatus status : files) {
				assertFalse(status.isDir());
			}

			// now that there are files, the directory must exist
			assertTrue(fs.exists(directory));
		}
		finally {
			// clean up
			fs.delete(directory, true);
		}

		// now directory must be gone (this is eventually-consistent, though!)
		checkPathEventualExistence(fs, directory, false, deadline);
	}
}
