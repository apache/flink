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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link BlobUtils}.
 */
public class BlobUtilsTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory} using
	 * {@link BlobServerOptions#STORAGE_DIRECTORY} per default.
	 */
	@Test
	public void testDefaultBlobStorageDirectory() throws IOException {
		Configuration config = new Configuration();
		String blobStorageDir = temporaryFolder.newFolder().getAbsolutePath();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, blobStorageDir);
		config.setString(CoreOptions.TMP_DIRS, temporaryFolder.newFolder().getAbsolutePath());

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(), startsWith(blobStorageDir));
	}

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory}'s fallback to
	 * {@link CoreOptions#TMP_DIRS} with a single temp directory.
	 */
	@Test
	public void testTaskManagerFallbackBlobStorageDirectory1() throws IOException {
		Configuration config = new Configuration();
		String blobStorageDir = temporaryFolder.getRoot().getAbsolutePath();
		config.setString(CoreOptions.TMP_DIRS, blobStorageDir);

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(), startsWith(blobStorageDir));
	}

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory}'s fallback to
	 * {@link CoreOptions#TMP_DIRS} having multiple temp directories.
	 */
	@Test
	public void testTaskManagerFallbackBlobStorageDirectory2a() throws IOException {
		Configuration config = new Configuration();
		String blobStorageDirs = temporaryFolder.newFolder().getAbsolutePath() + "," +
			temporaryFolder.newFolder().getAbsolutePath();
		config.setString(CoreOptions.TMP_DIRS, blobStorageDirs);

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(), startsWith(temporaryFolder.getRoot().getAbsolutePath()));
	}

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory}'s fallback to
	 * {@link CoreOptions#TMP_DIRS} having multiple temp directories.
	 */
	@Test
	public void testTaskManagerFallbackBlobStorageDirectory2b() throws IOException {
		Configuration config = new Configuration();
		String blobStorageDirs = temporaryFolder.newFolder().getAbsolutePath() + File.pathSeparator +
			temporaryFolder.newFolder().getAbsolutePath();
		config.setString(CoreOptions.TMP_DIRS, blobStorageDirs);

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(), startsWith(temporaryFolder.getRoot().getAbsolutePath()));
	}

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory}'s fallback to the default value of
	 * {@link CoreOptions#TMP_DIRS}.
	 */
	@Test
	public void testTaskManagerFallbackFallbackBlobStorageDirectory1() throws IOException {
		Configuration config = new Configuration();

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(),
			startsWith(CoreOptions.TMP_DIRS.defaultValue()));
	}
}
