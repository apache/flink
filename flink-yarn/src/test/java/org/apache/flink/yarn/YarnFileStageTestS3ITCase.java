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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertFalse;

/**
 * Tests for verifying file staging during submission to YARN works with the S3A file system.
 *
 * <p>Note that the setup is similar to <tt>org.apache.flink.fs.s3hadoop.HadoopS3FileSystemITCase</tt>.
 */
public class YarnFileStageTestS3ITCase extends TestLogger {

	private static final String BUCKET = System.getenv("ARTIFACTS_AWS_BUCKET");

	private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

	private static final String ACCESS_KEY = System.getenv("ARTIFACTS_AWS_ACCESS_KEY");
	private static final String SECRET_KEY = System.getenv("ARTIFACTS_AWS_SECRET_KEY");

	/**
	 * Will be updated by {@link #checkCredentialsAndSetup()} if the test is not skipped.
	 */
	private static boolean skipTest = true;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@BeforeClass
	public static void checkCredentialsAndSetup() throws IOException {
		// check whether credentials exist
		Assume.assumeTrue("AWS S3 bucket not configured, skipping test...", BUCKET != null);
		Assume.assumeTrue("AWS S3 access key not configured, skipping test...", ACCESS_KEY != null);
		Assume.assumeTrue("AWS S3 secret key not configured, skipping test...", SECRET_KEY != null);

		// initialize configuration with valid credentials
		final Configuration conf = new Configuration();
		conf.setString("s3.access.key", ACCESS_KEY);
		conf.setString("s3.secret.key", SECRET_KEY);
		FileSystem.initialize(conf);

		// check for uniqueness of the test directory
		final Path directory = new Path("s3://" + BUCKET + '/' + TEST_DATA_DIR);
		final FileSystem fs = directory.getFileSystem();

		// directory must not yet exist
		assertFalse(fs.exists(directory));

		// reset configuration
		FileSystem.initialize(new Configuration());

		skipTest = false;
	}

	@AfterClass
	public static void cleanUp() throws IOException {
		if (!skipTest) {
			// initialize configuration with valid credentials
			final Configuration conf = new Configuration();
			conf.setString("s3.access.key", ACCESS_KEY);
			conf.setString("s3.secret.key", SECRET_KEY);
			FileSystem.initialize(conf);

			final Path directory = new Path("s3://" + BUCKET + '/' + TEST_DATA_DIR);
			final FileSystem fs = directory.getFileSystem();

			// clean up
			fs.delete(directory, true);

			// now directory must be gone
			assertFalse(fs.exists(directory));

			// reset configuration
			FileSystem.initialize(new Configuration());
		}
	}

	/**
	 * Verifies that nested directories are properly copied with a <tt>s3a://</tt> file
	 * systems during resource uploads for YARN.
	 */
	@Test
	public void testRecursiveUploadForYarn() throws Exception {
		final Configuration conf = new Configuration();
		conf.setString("s3.access.key", ACCESS_KEY);
		conf.setString("s3.secret.key", SECRET_KEY);

		FileSystem.initialize(conf);

		final Path directory = new Path("s3://" + BUCKET + '/' + TEST_DATA_DIR + "/testYarn/");
		final HadoopFileSystem fs = (HadoopFileSystem) directory.getFileSystem();

		YarnFileStageTest.testCopyFromLocalRecursive(fs.getHadoopFileSystem(),
			new org.apache.hadoop.fs.Path(directory.toUri()), tempFolder, true);

		// now directory must be gone
		assertFalse(fs.exists(directory));
	}
}
