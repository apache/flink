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

package org.apache.flink.fs.s3presto;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static com.facebook.presto.hive.PrestoS3FileSystem.S3_USE_INSTANCE_CREDENTIALS;
import static org.apache.flink.core.fs.FileSystemTestUtils.checkPathEventualExistence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for the S3 file system support via Presto's {@link com.facebook.presto.hive.PrestoS3FileSystem}.
 *
 * <p><strong>BEWARE</strong>: tests must take special care of S3's
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel">consistency guarantees</a>
 * and what the {@link com.facebook.presto.hive.PrestoS3FileSystem} offers.
 */
public class PrestoS3FileSystemITCase extends TestLogger {

	private static final String BUCKET = System.getenv("ARTIFACTS_AWS_BUCKET");

	private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

	private static final String ACCESS_KEY = System.getenv("ARTIFACTS_AWS_ACCESS_KEY");
	private static final String SECRET_KEY = System.getenv("ARTIFACTS_AWS_SECRET_KEY");

	@BeforeClass
	public static void checkIfCredentialsArePresent() {
		Assume.assumeTrue("AWS S3 bucket not configured, skipping test...", BUCKET != null);
		Assume.assumeTrue("AWS S3 access key not configured, skipping test...", ACCESS_KEY != null);
		Assume.assumeTrue("AWS S3 secret key not configured, skipping test...", SECRET_KEY != null);
	}

	@Test
	public void testConfigKeysForwarding() throws Exception {
		final Path path = new Path("s3://" + BUCKET + '/' + TEST_DATA_DIR);

		// access without credentials should fail
		{
			Configuration conf = new Configuration();
			// fail fast and do not fall back to trying EC2 credentials
			conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
			FileSystem.initialize(conf);

			try {
				path.getFileSystem().exists(path);
				fail("should fail with an exception");
			} catch (IOException ignored) {}
		}

		// standard Presto-style credential keys
		{
			Configuration conf = new Configuration();
			conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
			conf.setString("presto.s3.access-key", ACCESS_KEY);
			conf.setString("presto.s3.secret-key", SECRET_KEY);

			FileSystem.initialize(conf);
			path.getFileSystem().exists(path);
		}

		// shortened Presto-style credential keys
		{
			Configuration conf = new Configuration();
			conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
			conf.setString("s3.access-key", ACCESS_KEY);
			conf.setString("s3.secret-key", SECRET_KEY);

			FileSystem.initialize(conf);
			path.getFileSystem().exists(path);
		}

		// shortened Hadoop-style credential keys
		{
			Configuration conf = new Configuration();
			conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
			conf.setString("s3.access.key", ACCESS_KEY);
			conf.setString("s3.secret.key", SECRET_KEY);

			FileSystem.initialize(conf);
			path.getFileSystem().exists(path);
		}

		// shortened Hadoop-style credential keys with presto prefix
		{
			Configuration conf = new Configuration();
			conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
			conf.setString("presto.s3.access.key", ACCESS_KEY);
			conf.setString("presto.s3.secret.key", SECRET_KEY);

			FileSystem.initialize(conf);
			path.getFileSystem().exists(path);
		}

		// re-set configuration
		FileSystem.initialize(new Configuration());
	}

	@Test
	public void testSimpleFileWriteAndRead() throws Exception {
		final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs
		final Configuration conf = new Configuration();
		conf.setString("s3.access-key", ACCESS_KEY);
		conf.setString("s3.secret-key", SECRET_KEY);

		final String testLine = "Hello Upload!";

		FileSystem.initialize(conf);

		final Path path = new Path("s3://" + BUCKET + '/' + TEST_DATA_DIR + "/test.txt");
		final FileSystem fs = path.getFileSystem();

		try {
			try (FSDataOutputStream out = fs.create(path, WriteMode.OVERWRITE);
					OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
				writer.write(testLine);
			}

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
		final Configuration conf = new Configuration();
		conf.setString("s3.access-key", ACCESS_KEY);
		conf.setString("s3.secret-key", SECRET_KEY);

		FileSystem.initialize(conf);

		final Path directory = new Path("s3://" + BUCKET + '/' + TEST_DATA_DIR + "/testdir/");
		final FileSystem fs = directory.getFileSystem();

		// directory must not yet exist
		assertFalse(fs.exists(directory));

		try {
			// create directory
			assertTrue(fs.mkdirs(directory));

			// seems the presto file system does not assume existence of empty directories in S3
//			assertTrue(fs.exists(directory));

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

		// now directory must be gone (this is eventually-consistent!)
		checkPathEventualExistence(fs, directory, false, deadline);
	}
}
