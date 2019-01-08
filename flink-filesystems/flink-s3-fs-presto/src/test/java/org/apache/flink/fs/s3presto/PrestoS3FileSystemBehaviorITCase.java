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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.UUID;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for the s3a-based S3 file system.
 */
public class PrestoS3FileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

	private static final String BUCKET = System.getenv("ARTIFACTS_AWS_BUCKET");

	private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

	private static final String ACCESS_KEY = System.getenv("ARTIFACTS_AWS_ACCESS_KEY");
	private static final String SECRET_KEY = System.getenv("ARTIFACTS_AWS_SECRET_KEY");

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
	}

	@AfterClass
	public static void clearFsConfig() throws IOException {
		FileSystem.initialize(new Configuration());
	}

	@Override
	public FileSystem getFileSystem() throws Exception {
		return getBasePath().getFileSystem();
	}

	@Override
	public Path getBasePath() throws Exception {
		return new Path("s3://" + BUCKET + '/' + TEST_DATA_DIR);
	}

	@Override
	public FileSystemKind getFileSystemKind() {
		return FileSystemKind.OBJECT_STORE;
	}
}
